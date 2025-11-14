# app.py — controller (GPU-aware agent registry + lightweight 24h metrics)

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
agents: Dict[str, Dict[str, Any]] = {}       # agent_id -> {"labels":{}, "capabilities":{}, "last_seen": ts}

# ----------------- Lightweight metrics (24h) -----------------
SAMPLE_PERIOD_SEC = 1
RETENTION_SEC = 24 * 60 * 60            # 24 hours
MAX_POINTS = RETENTION_SEC // SAMPLE_PERIOD_SEC

completed_total = 0
failed_total = 0
leased_total = 0

# Per-second timeline of cumulative completed count and queue length.
# Each item: (timestamp_sec, completed_total, queue_len)
timeline = deque(maxlen=MAX_POINTS)

# Per-op counters and duration aggregates (ms)
op_counts: Dict[str, int] = defaultdict(int)
op_dur_sum_ms: Dict[str, int] = defaultdict(int)
op_dur_n: Dict[str, int] = defaultdict(int)

def _now() -> float:
    return time.time()

def _now_s() -> int:
    return int(_now())

def _new_id() -> str:
    return f"tsk-{uuid.uuid4().hex[:8]}"

def _rate_per_sec(window_seconds: int) -> float:
    """Compute tasks/sec over the last window_seconds using timeline."""
    if len(timeline) < 2:
        return 0.0
    t1, c1, _ = timeline[-1]
    # Walk back until we reach the window or run out
    target = t1 - window_seconds
    # find first index with ts >= target
    idx = None
    for i in range(len(timeline) - 1, -1, -1):
        if timeline[i][0] <= target:
            idx = i
            break
    if idx is None:
        # window longer than our buffer; use earliest
        t0, c0, _ = timeline[0]
    else:
        t0, c0, _ = timeline[idx]
    dt = max(1e-6, t1 - t0)
    return (c1 - c0) / dt

def _ema_update(prev: float, value: float, alpha: float = 0.2) -> float:
    return value if math.isnan(prev) else (alpha * value + (1 - alpha) * prev)

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
    # {"type":"map_tokenize","payload":{...}, "requires": {...}}
    type: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    resources: Optional[Dict[str, Any]] = None
    policy: Optional[Dict[str, Any]] = None
    tenant: Optional[str] = "default"
    requires: Optional[Dict[str, Any]] = None

# --------------- Helpers ---------------------------
def _parse_capabilities(req: Request) -> Optional[List[str]]:
    """
    Read capabilities from header or query string.
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
    gpu = caps.get("gpu", {}) if isinstance(caps, dict) else {}

    # requires.gpu: bool
    want_gpu = requires.get("gpu")
    if want_gpu is True and not gpu.get("present"):
        return False

    # requires.min_vram_mb
    mv = requires.get("min_vram_mb") or requires.get("vram_mb")
    if mv is not None:
        try:
            actual = int(gpu.get("vram_mb") or 0)
            if actual < int(mv):
                return False
        except Exception:
            return False

    # requires.min_cuda
    min_cuda = requires.get("min_cuda") or requires.get("cuda")
    if min_cuda:
        agent_cuda = str(gpu.get("cuda_version") or "0.0")
        if _tuple_ver(agent_cuda) < _tuple_ver(str(min_cuda)):
            return False

    # requires.fp16 / bf16
    for k in ("fp16", "bf16"):
        if requires.get(k) is True and not bool(gpu.get(k)):
            return False

    # requires.labels: all must match exactly as strings
    want_labels = requires.get("labels")
    if isinstance(want_labels, dict):
        labels = (agent_rec or {}).get("labels", {})
        for k, v in want_labels.items():
            if str(labels.get(k)) != str(v):
                return False

    return True

def _eligible_for_agent(task: Dict[str, Any], agent_id: str, ops_advertised: Optional[List[str]]) -> bool:
    # op filter (header-advertised)
    if ops_advertised is not None and task.get("op") not in ops_advertised:
        return False
    # requires filter (GPU + labels + etc.)
    req = task.get("requires") or task_meta.get(task.get("id", ""), {}).get("requires")
    agent_rec = agents.get(agent_id)
    return _meets_requires(agent_rec, req)

# ----------------- Metrics sampler -----------------
@app.on_event("startup")
async def _metrics_sampler_start():
    # seed timeline immediately so stats don't look empty at boot
    timeline.append((_now_s(), completed_total, len(queue)))

    async def _sample_loop():
        while True:
            # append one point each second: timestamp, completed_total, current queue length
            timeline.append((_now_s(), completed_total, len(queue)))
            await asyncio.sleep(SAMPLE_PERIOD_SEC)

    asyncio.create_task(_sample_loop())

# --------------- Health ----------------------------
@app.get("/healthz")
def healthz():
    return {
        "status": "ok",
        "time": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "agents_online": len(agents),
        "queue_len": len(queue),
    }

# --------------- Agent registry --------------------
@app.post("/agents/register")
def agents_register(payload: Dict[str, Any] = Body(...)):
    """
    Register or upsert an agent with labels + capabilities.
    payload: {"agent": "...", "labels": {...}, "capabilities": {...}, "timestamp": int}
    """
    agent_id = str(payload.get("agent") or "").strip()
    if not agent_id:
        raise HTTPException(status_code=422, detail="missing 'agent'")
    rec = agents.get(agent_id, {})
    rec["labels"] = payload.get("labels") or rec.get("labels") or {}
    rec["capabilities"] = payload.get("capabilities") or rec.get("capabilities") or {}
    rec["last_seen"] = _now()
    agents[agent_id] = rec
    return {"registered": True, "agent": agent_id}

@app.post("/agents/heartbeat")
def agents_heartbeat(payload: Dict[str, Any] = Body(...)):
    """
    Lightweight upsert for periodic updates.
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
    rec["last_seen"] = _now()
    agents[agent_id] = rec
    return {"heartbeat": True, "agent": agent_id}

@app.get("/agents")
def list_agents():
    """Debug view of registered agents."""
    return {aid: {**rec, "capabilities": rec.get("capabilities", {})} for aid, rec in agents.items()}

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
    """
    global leased_total

    ops_advertised = _parse_capabilities(request)  # None = accept anything (back-compat)
    deadline = _now() + (wait_ms / 1000.0)

    while _now() < deadline:
        # scan for first eligible task
        for i, t in enumerate(queue):
            # capability/op check
            if ops_advertised is not None and t.get("op") not in ops_advertised:
                continue
            # requires check
            if not _eligible_for_agent(t, agent, ops_advertised):
                continue
            # assignment preference
            if t.get("assigned_to") in (None, agent):
                task = queue.pop(i)
                task["assigned_to"] = agent
                task["leased_at"] = _now()

                tid = task.get("id")
                if tid:
                    meta = task_meta.get(tid, {})
                    meta.update({"assigned_to": agent, "leased_at": task["leased_at"]})
                    task_meta[tid] = meta

                leased_total += 1
                return JSONResponse(task, status_code=200)
        await asyncio.sleep(0.02)

    if empty == 1:
        return JSONResponse({}, status_code=200)
    return Response(status_code=204)

# --------------- Result ingest ---------------------
@app.post("/result")
def post_result(r: Dict[str, Any] = Body(...)):
    """
    Accepts agent results (any superset). Normalizes and stores by task id.
    """
    global completed_total, failed_total

    if "id" not in r or "agent" not in r:
        raise HTTPException(status_code=422, detail="Result must include 'id' and 'agent'")

    rid = str(r.get("id"))
    meta = task_meta.get(rid, {})
    op = meta.get("op", str(r.get("op", "")))  # tolerate agents echoing op

    trace = ""
    if isinstance(op, str) and op.startswith("trace-"):
        trace = op.split(".", 1)[0]

    ok_flag = bool(r.get("ok", True))
    dur_ms = int(r.get("duration_ms") or 0)

    rec = {
        "id": rid,
        "agent": r.get("agent"),
        "ok": ok_flag,
        "output": r.get("output"),
        "duration_ms": dur_ms,
        "error": r.get("error") or "",
        "op": op,
        "trace": trace,
        "_raw": r,
        "ts": _now(),
        "assigned_to": meta.get("assigned_to"),
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

    return {"stored": True, "id": rid}

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
    task_meta[job_id].update({"job_id": job_id})

    jobs[job_id] = {
        "id": job_id,
        "type": job.type,
        "payload": job.payload,
        "resources": job.resources,
        "policy": job.policy,
        "tenant": job.tenant,
        "requires": job.requires,
        "status": "queued",
        "queued_ts": _now(),
    }
    return {"job_id": job_id, "status": "queued", "requires": job.requires}

@app.get("/job/{job_id}")
def job_status(job_id: str):
    j = jobs.get(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="job not found")
    return j

# --------------- Results dump ----------------------
@app.get("/results")
def all_results(
    limit: int = Query(0, ge=0, description="If >0, return only the latest N results"),
    agent: Optional[str] = Query(None),
    op: Optional[str] = Query(None),
    trace: Optional[str] = Query(None),
):
    """
    Returns stored results as a dict keyed by task id (back-compat).
    Optional filters + limit.
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
    """Snapshot plus rolling rates."""
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
        "rate_tasks_per_sec": {
            "1s": round(rate_1s, 3),
            "60s": round(rate_60s, 3),
            "5m": round(rate_5m, 3),
            "15m": round(rate_15m, 3),
        }
    }

@app.get("/stats/sparkline")
def stats_sparkline(seconds: int = Query(60, ge=1, le=min(3600, RETENTION_SEC))):
    """
    Return per-second throughput for last N seconds as a list of {t, tps}.
    Useful for UI charts. Light enough to poll every second.
    """
    if len(timeline) < 2:
        return {"points": []}

    # Build tps from deltas of completed_total between consecutive seconds
    end_ts = timeline[-1][0]
    start_ts = end_ts - seconds + 1
    # Convert timeline into dict for O(1) lookups per ts
    by_ts = {ts: comp for ts, comp, _ in timeline}
    points = []
    prev_comp = None
    for ts in range(start_ts, end_ts + 1):
        comp = by_ts.get(ts, prev_comp if prev_comp is not None else 0)
        if prev_comp is None:
            tps = 0.0
        else:
            tps = max(0.0, comp - prev_comp)
        points.append({"t": ts, "tps": tps})
        prev_comp = comp
    return {"points": points}

@app.get("/stats/ops")
def stats_ops():
    """Per-op counts and average duration in ms."""
    data = {}
    for op, cnt in op_counts.items():
        n = max(1, op_dur_n.get(op, 0))
        avg_ms = (op_dur_sum_ms.get(op, 0) / n) if op_dur_n.get(op, 0) else 0.0
        data[op] = {"count": cnt, "avg_duration_ms": round(avg_ms, 2)}
    return {"ops": data}

# --------------- Maintenance -----------------------
@app.post("/purge")
def purge(all: int = 0):
    """
    POST /purge       -> clear queue only
    POST /purge?all=1 -> clear queue + results + meta + jobs (keeps agents)
    Metrics timeline is retained (24h ring); counters are NOT reset unless all=1.
    """
    global completed_total, failed_total, leased_total
    queue.clear()
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
    return {"purged": True, "all": bool(all), "queue_len": len(queue)}
