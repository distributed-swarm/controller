# app.py — controller (GPU-aware agent registry + lightweight 24h metrics + worker-aware wiring v1)

from fastapi import FastAPI, HTTPException, Body, Query, Response, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Tuple
from collections import deque, defaultdict
import time, uuid, asyncio, math

app = FastAPI()

# ---------------- In-memory stores ----------------
queue: List[Dict[str, Any]] = []             # pending tasks; normalized: {"id","op","payload",...,"requires"?}
results: Dict[str, Dict[str, Any]] = {}      # task_id -> result record
task_meta: Dict[str, Dict[str, Any]] = {}    # task_id -> metadata (op, seed_ts, assigned_to, leased_at, job_id, requires)
jobs: Dict[str, Dict[str, Any]] = {}         # job_id -> job record (for submit_job)

# 24h metrics: we track per-second completed counts for rolling rates
TIMELINE_MAX_SECS = 24 * 60 * 60
timeline: deque = deque(maxlen=TIMELINE_MAX_SECS)  # (timestamp_sec, completed_total, queue_len)
completed_total = 0
failed_total = 0
leased_total = 0

# op-level aggregates
op_counts: Dict[str, int] = defaultdict(int)
op_dur_sum_ms: Dict[str, float] = defaultdict(float)
op_dur_n: Dict[str, int] = defaultdict(int)

# agent registry: {agent_id: {"labels": {...}, "capabilities": {...}, "last_seen": ts, "workers": {...}}}
agents: Dict[str, Dict[str, Any]] = {}

# per-agent leases currently in flight (for worker sizing / utilization views)
agent_inflight: Dict[str, int] = defaultdict(int)

# ---------------- Helpers ----------------

def _now() -> float:
    return time.time()

def _now_s() -> int:
    return int(time.time())

def _new_id() -> str:
    return uuid.uuid4().hex

def _update_timeline():
    """Append a point each time we finish a task."""
    if not timeline:
        timeline.append((_now_s(), completed_total, len(queue)))
    else:
        last_ts, _, _ = timeline[-1]
        now_ts = _now_s()
        if now_ts != last_ts:
            timeline.append((now_ts, completed_total, len(queue)))

def _rate_per_sec(window_s: int) -> float:
    """Return completed tasks/sec over the last window_s seconds."""
    if not timeline:
        return 0.0
    now_ts = _now_s()
    cutoff = now_ts - window_s
    # find first point >= cutoff
    start_idx = None
    for i, (ts, _, _) in enumerate(timeline):
        if ts >= cutoff:
            start_idx = i
            break
    if start_idx is None:
        # all points older than cutoff
        return 0.0
    start_ts, start_completed, _ = timeline[start_idx]
    end_ts, end_completed, _ = timeline[-1]
    dt = max(end_ts - start_ts, 1)
    return (end_completed - start_completed) / dt

def _parse_capabilities(req: Request) -> Optional[List[str]]:
    """
    Agents can advertise which ops they support via:
      Header: X-Tasks: sha256,trace,map_tokenize
      Query:  ?ops=sha256,trace,map_tokenize
    """
    caps = req.headers.get("X-Tasks") or req.query_params.get("ops") or ""
    caps = [c.strip() for c in caps.split(",") if c.strip()]
    return caps or None

def _parse_requires(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = obj.get("requires")
    return r if isinstance(r, dict) else None

def _tuple_ver(v: str) -> Tuple[int, int]:
    try:
        parts = str(v).split(".")
        return int(parts[0]), int(parts[1] if len(parts) > 1 else 0)
    except Exception:
        return (0, 0)

def _meets_requires(agent_rec: Optional[Dict[str, Any]], requires: Optional[Dict[str, Any]]) -> bool:
    """Return True if agent satisfies 'requires'. Permissive by default if no requires."""
    if not requires:
        return True
    if not agent_rec:
        # If task explicitly requires GPU and we don't know the agent, reject.
        if requires.get("gpu") is True:
            return False
        # Otherwise allow unknowns.
        return True

    caps = (agent_rec or {}).get("capabilities", {})
    gpu_caps = caps.get("gpu") or {}
    labels = (agent_rec or {}).get("labels") or {}

    # GPU presence
    need_gpu = requires.get("gpu")
    if need_gpu is True and not gpu_caps.get("present"):
        return False

    # Min VRAM
    min_vram = requires.get("min_vram_mb")
    if isinstance(min_vram, (int, float)) and isinstance(gpu_caps.get("vram_mb"), (int, float)):
        if gpu_caps["vram_mb"] < min_vram:
            return False

    # Min CUDA
    min_cuda = requires.get("min_cuda")
    if isinstance(min_cuda, str) and isinstance(gpu_caps.get("cuda_version"), str):
        if _tuple_ver(gpu_caps["cuda_version"]) < _tuple_ver(min_cuda):
            return False

    # Precision hints (not enforced strictly, but we can deny if impossible)
    for prec in ("fp16", "bf16"):
        req_val = requires.get(prec)
        if req_val is True:
            if gpu_caps.get("present") and gpu_caps.get(prec) is False:
                return False

    # Label match: requires.labels = {k:v} means agent.labels[k] == v
    req_labels = requires.get("labels")
    if isinstance(req_labels, dict):
        for k, v in req_labels.items():
            if labels.get(k) != v:
                return False

    return True

def _merge_worker_info(rec: Dict[str, Any], payload: Dict[str, Any]) -> None:
    """
    Step 1 wiring for worker sizing:
    - Accept either top-level 'max_workers' or nested 'workers': {'max': N}
    - Store under rec['workers'] = {'max': int} for UI.
    """
    workers = rec.get("workers") or {}
    # top-level shorthand
    if "max_workers" in payload:
        try:
            workers["max"] = int(payload["max_workers"])
        except Exception:
            pass
    # nested structure
    if isinstance(payload.get("workers"), dict):
        w = payload["workers"]
        if "max" in w:
            try:
                workers["max"] = int(w["max"])
            except Exception:
                pass
    if workers:
        rec["workers"] = workers

# --------------- Models (for ergonomics) ----------
class SeedLegacy(BaseModel):
    # {"type":"sha256","count":5}
    type: str = Field(default="sha256")
    count: int = Field(ge=0, le=100000, default=0)
    requires: Optional[Dict[str, Any]] = None

class SeedItems(BaseModel):
    # {"items":[...], "task":"sha256", "requires": {...}}
    items: List[Any] = Field(default_factory=list)
    task: str = Field(default="sha256")
    requires: Optional[Dict[str, Any]] = None

class JobIn(BaseModel):
    # {"type":"map_tokenize","payload":{...}, "requires": {...}}  (compat with trainer)
    type: str
    payload: Dict[str, Any]
    requires: Optional[Dict[str, Any]] = None

# --------------- Agent registry endpoints ----------------
@app.post("/agents/register")
def agents_register(payload: Dict[str, Any] = Body(...)):
    """
    Register or upsert an agent with labels + capabilities + optional worker sizing.
    payload: {
      "agent": "...",
      "labels": {...},
      "capabilities": {...},
      "timestamp": int,
      "max_workers": int,          # optional shorthand
      "workers": {"max": int}      # optional structured form
    }
    """
    agent_id = str(payload.get("agent") or "").strip()
    if not agent_id:
        raise HTTPException(status_code=422, detail="missing 'agent'")
    rec = agents.get(agent_id, {})
    rec["labels"] = payload.get("labels") or rec.get("labels") or {}
    rec["capabilities"] = payload.get("capabilities") or rec.get("capabilities") or {}
    rec["last_seen"] = _now()
    _merge_worker_info(rec, payload)
    agents[agent_id] = rec
    return {"registered": True, "agent": agent_id}

@app.post("/agents/heartbeat")
def agents_heartbeat(payload: Dict[str, Any] = Body(...)):
    """
    Lightweight upsert for periodic updates.
    Allows refreshing labels/capabilities and worker sizing.
    """
    agent_id = str(payload.get("agent") or "").strip()
    if not agent_id:
        raise HTTPException(status_code=422, detail="missing 'agent'")
    rec = agents.get(agent_id, {"labels": {}, "capabilities": {}})
    if isinstance(payload.get("labels"), dict):
        rec["labels"].update(payload["labels"])
    if isinstance(payload.get("capabilities"), dict):
        # replace to avoid stale nested keys
        rec["capabilities"] = payload["capabilities"]
    _merge_worker_info(rec, payload)
    rec["last_seen"] = _now()
    agents[agent_id] = rec
    return {"heartbeat": True, "agent": agent_id}

@app.get("/agents")
def list_agents():
    """Debug view of registered agents, including per-agent inflight leases."""
    out: Dict[str, Any] = {}
    for aid, rec in agents.items():
        # shallow copy so we don't mutate the in-memory record
        view = dict(rec)
        view["inflight"] = int(agent_inflight.get(aid, 0))
        out[aid] = view
    return out

# --------------- Task leasing (simple, back-compat) ------------
@app.get("/task")
async def get_task(
    request: Request,
    agent: str = Query(..., description="Agent identifier"),
    wait_ms: int = Query(2000, ge=0, le=120000),
    empty: int = Query(0, description="If 1, return {} with 200 when no task instead of 204"),
):
    """
    Long-poll up to wait_ms for an eligible task.
    If the agent advertises capabilities (X-Tasks or ?ops=...), lease only compatible ops.
    If the task has 'requires', also check agent registry to ensure constraints are met.
    Preference: unassigned, or already assigned to this agent (retries).

    Worker wiring (step 1):
    - We increment agent_inflight[agent] whenever we lease a task.
    - Decrement happens in /result when that task is reported back.
    - This does NOT enforce limits yet; it just feeds UI + future policies.
    """
    global leased_total, agent_inflight

    ops_advertised = _parse_capabilities(request)  # None = accept anything (back-compat)
    deadline = _now() + (wait_ms / 1000.0)

    while _now() < deadline:
        # scan for first eligible task
        for i, t in enumerate(queue):
            # capability/op check
            if ops_advertised is not None and t.get("op") not in ops_advertised:
                continue
            
            # requires check
            requires = _parse_requires(t)
            agent_rec = agents.get(agent)
            if not _meets_requires(agent_rec, requires):
                continue

            # lease this task
            leased_total += 1
            queue.pop(i)
            tid = t["id"]
            meta = task_meta.get(tid) or {}
            meta["assigned_to"] = agent
            meta["leased_at"] = _now()
            task_meta[tid] = meta
            agent_inflight[agent] = agent_inflight.get(agent, 0) + 1
            _update_timeline()
            return t

        # nothing found: either sleep briefly or return empty
        await asyncio.sleep(0.05)

        if empty == 1:
            # return {} instead of 204 for clients that want explicit "no task"
            return {}

    # timed out
    if empty == 1:
        return {}
    return Response(status_code=204)

@app.post("/result")
def store_result(payload: Dict[str, Any] = Body(...)):
    """
    Store result from an agent.
    Expected payload (agent side):
      {"id":..., "agent":..., "ok": bool, "duration_ms": int, "output":..., "error":..., "op":..., "trace":...}
    """
    global completed_total, failed_total, agent_inflight

    tid = str(payload.get("id") or "").strip()
    if not tid:
        raise HTTPException(status_code=422, detail="missing 'id'")

    # Update main result record
    rid = tid
    ok_flag = bool(payload.get("ok"))
    dur_ms = int(payload.get("duration_ms") or 0)
    op = payload.get("op") or ""
    agent_id = payload.get("agent")

    meta = task_meta.get(tid) or {}
    rec = {
        "id": rid,
        "task_id": tid,
        "agent": agent_id,
        "ok": ok_flag,
        "duration_ms": dur_ms,
        "output": payload.get("output"),
        "error": payload.get("error"),
        "op": op,
        "trace": payload.get("trace") or "",
        "ts": _now_s(),
        "leased_at": meta.get("leased_at"),
        "seed_ts": meta.get("seed_ts"),
        "requires": meta.get("requires"),
    }
    results[rid] = rec

    # metrics
    completed_total += 1
    if not ok_flag:
        failed_total += 1
    if op:
        op_counts[op] += 1
        if dur_ms > 0:
            op_dur_sum_ms[op] += dur_ms
            op_dur_n[op] += 1

    job_id = meta.get("job_id")
    if job_id and job_id in jobs:
        jobs[job_id]["status"] = "done"
        jobs[job_id]["result_id"] = rid

    # decrement inflight for that agent, but don't let it go negative even if agent_id is weird
    if isinstance(agent_id, str) and agent_id:
        current = agent_inflight.get(agent_id, 0)
        if current <= 1:
            agent_inflight[agent_id] = 0
        else:
            agent_inflight[agent_id] = current - 1

    return {"stored": True, "id": rid}


# --------------- Task normalization helper -------------------
def _normalize_single_task(
    op: str,
    payload: Any,
    given_id: Optional[str] = None,
    requires: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Normalize incoming tasks into the internal queue format.

    Ensures:
      - every task has an id
      - we store op/payload/requires on the queue item
      - task_meta is updated so metrics & jobs have consistent data
    """
    op = str(op or "").strip() or "sha256"
    tid = given_id or _new_id()

    t: Dict[str, Any] = {
        "id": tid,
        "op": op,
        "payload": payload,
    }
    if requires:
        t["requires"] = requires

    meta = task_meta.get(tid) or {}
    meta.update(
        {
            "id": tid,
            "op": op,
            "seed_ts": _now(),
            "requires": requires,
        }
    )
    task_meta[tid] = meta

    return t


# --------------- Seeding (compat & modern) ----------
@app.post("/seed")
def seed(payload: Dict[str, Any] = Body(...)):
    """
    Accepts any of the following and normalizes:
      1) Legacy count: {"type":"sha256","count":5, "requires": {...}}
      2) Items list:   {"items":[...], "task":"sha256", "requires": {...}}
      3) Single task:  {"op":"map_tokenize","payload":{...}, "requires": {...}}   (preferred)
      4) Alt keys:     {"type":"map_tokenize","params":{...}, "requires": {...}}  (compat)
    'requires' supports: gpu:bool, min_vram_mb:int, min_cuda:str, fp16:bool, bf16:bool, labels:{k:v}
    """
    queued = 0
    requires = _parse_requires(payload) or None

    # Case 3: single task with explicit op/payload
    if "op" in payload and "payload" in payload:
        t = _normalize_single_task(payload["op"], payload["payload"], requires=requires)
        queue.append(t); queued += 1
        return {"queued": queued, "op": t["op"], "requires": requires}

    # Case 4: alt keys -> normalize to op/payload
    if "type" in payload and "params" in payload:
        t = _normalize_single_task(payload["type"], payload["params"], requires=requires)
        queue.append(t); queued += 1
        return {"queued": queued, "op": t["op"], "requires": requires}

    # Case 2: items list with per-item payload
    if "items" in payload:
        model = SeedItems(**payload)
        for item in model.items:
            t = _normalize_single_task(model.task, item, requires=model.requires or requires)
            queue.append(t); queued += 1
        return {"queued": queued, "op": model.task, "requires": model.requires or requires}

    # Case 1: legacy count seeding
    model = SeedLegacy(**payload)
    for _ in range(model.count):
        data = uuid.uuid4().hex
        t = _normalize_single_task(model.type, data, requires=model.requires or requires)
        queue.append(t); queued += 1
    return {"queued": queued, "op": model.type, "requires": model.requires or requires}

@app.post("/submit_job")
def submit_job(job: JobIn):
    """
    Enqueue a single job as one normalized task.
    job_id == task_id for easy tracing.
    Supports 'requires'.
    """
    job_id = _new_id()
    task = _normalize_single_task(job.type, job.payload, given_id=job_id, requires=job.requires)
    queue.append(task)
    jobs[job_id] = {
        "id": job_id,
        "type": job.type,
        "status": "queued",
        "requires": job.requires,
        "created_at": _now(),
        "result_id": None,
    }
    return {"job_id": job_id, "status": "queued"}

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job_not_found")
    res = None
    if job.get("result_id"):
        res = results.get(job["result_id"])
    return {"job": job, "result": res}

@app.get("/results")
def list_results(
    agent: Optional[str] = None,
    op: Optional[str] = None,
    trace: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    """
    Lightweight query over results for debugging.
    """
    items = list(results.values())
    if agent:
        items = [r for r in items if r.get("agent") == agent]
    if op:
        items = [r for r in items if r.get("op") == op]
    if trace:
        items = [r for r in items if r.get("trace") == trace]

    items.sort(key=lambda r: r.get("ts", 0))
    if limit and limit > 0:
        items = items[-limit:]
    return {r["id"]: r for r in items}

# --------------- Metrics endpoints -----------------
@app.get("/stats")
def stats():
    """Snapshot plus rolling rates and per-agent inflight leases."""
    rate_1s = _rate_per_sec(1)
    rate_60s = _rate_per_sec(60)
    rate_5m = _rate_per_sec(300)
    rate_15m = _rate_per_sec(900)
    latest_queue = timeline[-1][2] if timeline else len(queue)

    return {
        "time": _now_s(),
        "agents_online": len(agents),
        "queue_len": latest_queue,
        "leased_total": leased_total,
        "completed_total": completed_total,
        "failed_total": failed_total,
        "rate_1s": rate_1s,
        "rate_60s": rate_60s,
        "rate_5m": rate_5m,
        "rate_15m": rate_15m,
        "op_counts": dict(op_counts),
        "op_avg_ms": {
            op: (op_dur_sum_ms[op] / max(op_dur_n[op], 1)) for op in op_dur_sum_ms.keys()
        },
        # new: exposure for UI / capacity views
        "agent_inflight": {k: int(v) for k, v in agent_inflight.items()},
    }

@app.get("/timeline")
def get_timeline():
    """Return the raw timeline for plotting."""
    return list(timeline)

@app.get("/healthz")
def healthz():
    return {"status": "ok", "queue_len": len(queue), "results": len(results)}

# --------------- Admin/debug --------------------
@app.post("/purge")
def purge(
    q: bool = Query(True, description="Clear the queue"),
    r: bool = Query(False, description="Clear results"),
    all: bool = Query(False, description="Clear queue+results+jobs+metrics+timeline"),
):
    global completed_total, failed_total, leased_total, agent_inflight
    if q:
        queue.clear()
    if r:
        results.clear()
    if all:
        results.clear()
        task_meta.clear()
        jobs.clear()
        # keep 'agents' so the registry survives between runs (in-memory anyway)
        completed_total = 0
        failed_total = 0
        leased_total = 0
        timeline.clear()
        timeline.append((_now_s(), completed_total, len(queue)))
        op_counts.clear()
        op_dur_sum_ms.clear()
        op_dur_n.clear()
        agent_inflight = defaultdict(int)
    return {"purged": True, "all": bool(all), "queue_len": len(queue)}
