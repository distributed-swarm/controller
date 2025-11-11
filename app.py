# app.py — controller (GPU-aware agent registry + requires filter, backward-compatible)

from fastapi import FastAPI, HTTPException, Body, Query, Response, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Tuple
import time, uuid, asyncio

app = FastAPI()

# ---------------- In-memory stores ----------------
queue: List[Dict[str, Any]] = []             # pending tasks; normalized: {"id","op","payload",...,"requires"?}
results: Dict[str, Dict[str, Any]] = {}      # task_id -> result record
task_meta: Dict[str, Dict[str, Any]] = {}    # task_id -> metadata (op, seed_ts, assigned_to, leased_at, job_id, requires)
jobs: Dict[str, Dict[str, Any]] = {}         # job_id -> job record (for submit_job)
agents: Dict[str, Dict[str, Any]] = {}       # agent_id -> {"labels":{}, "capabilities":{}, "last_seen": ts}

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
def _now() -> float:
    return time.time()

def _new_id() -> str:
    return f"tsk-{uuid.uuid4().hex[:8]}"

def _normalize_single_task(op: str, payload: Any, given_id: Optional[str] = None,
                           requires: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Return a normalized task dict the agent understands."""
    tid = given_id or _new_id()
    task = {"id": tid, "op": str(op), "payload": payload}
    if requires:
        task["requires"] = requires
    # minimal meta for traceability
    meta = task_meta.get(tid, {})
    meta.update({"op": str(op), "seed_ts": _now(), "requires": requires or meta.get("requires")})
    task_meta[tid] = meta
    return task

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
    if "id" not in r or "agent" not in r:
        raise HTTPException(status_code=422, detail="Result must include 'id' and 'agent'")

    rid = str(r.get("id"))
    meta = task_meta.get(rid, {})
    op = meta.get("op", str(r.get("op", "")))  # tolerate agents echoing op

    trace = ""
    if isinstance(op, str) and op.startswith("trace-"):
        trace = op.split(".", 1)[0]

    rec = {
        "id": rid,
        "agent": r.get("agent"),
        "ok": bool(r.get("ok", True)),
        "output": r.get("output"),
        "duration_ms": int(r.get("duration_ms") or 0),
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

# --------------- Maintenance -----------------------
@app.post("/purge")
def purge(all: int = 0):
    """
    POST /purge       -> clear queue only
    POST /purge?all=1 -> clear queue + results + meta + jobs + agents (no, keep agents)
    """
    queue.clear()
    if all:
        results.clear()
        task_meta.clear()
        jobs.clear()
        # keep 'agents' so the registry survives between runs (in-memory anyway)
    return {"purged": True, "all": bool(all), "queue_len": len(queue)}
