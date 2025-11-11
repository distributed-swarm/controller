# app.py — controller (normalized tasks, optional capability filter, backward-compatible)

from fastapi import FastAPI, HTTPException, Body, Query, Response, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Tuple
import time, uuid, asyncio

app = FastAPI()

# ---------------- In-memory stores ----------------
queue: List[Dict[str, Any]] = []             # pending tasks; normalized: {"id","op","payload",...}
results: Dict[str, Dict[str, Any]] = {}      # task_id -> result record
task_meta: Dict[str, Dict[str, Any]] = {}    # task_id -> metadata (op, seed_ts, assigned_to, leased_at, job_id)
jobs: Dict[str, Dict[str, Any]] = {}         # job_id -> job record (for submit_job)

# --------------- Models (for ergonomics) ----------
class SeedLegacy(BaseModel):
    # {"type":"sha256","count":5}
    type: str = Field(default="sha256")
    count: int = Field(ge=0, le=100000, default=0)

class SeedItems(BaseModel):
    # {"items":[...], "task":"sha256"}
    items: List[Any] = Field(default_factory=list)
    task: str = Field(default="sha256")

class JobIn(BaseModel):
    # {"type":"map_tokenize","payload":{...}, ...}
    type: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    resources: Optional[Dict[str, Any]] = None
    policy: Optional[Dict[str, Any]] = None
    tenant: Optional[str] = "default"

# --------------- Helpers ---------------------------
def _now() -> float:
    return time.time()

def _new_id() -> str:
    return f"tsk-{uuid.uuid4().hex[:8]}"

def _normalize_single_task(op: str, payload: Any, given_id: Optional[str] = None) -> Dict[str, Any]:
    """Return a normalized task dict the agent understands."""
    tid = given_id or _new_id()
    task = {"id": tid, "op": str(op), "payload": payload}
    # minimal meta for traceability
    task_meta[tid] = {"op": str(op), "seed_ts": _now(), **task_meta.get(tid, {})}
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

# --------------- Health ----------------------------
@app.get("/healthz")
def healthz():
    return {"status": "ok", "time": time.strftime("%Y-%m-%dT%H:%M:%S%z")}

# --------------- Task leasing ----------------------
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
    Preference: unassigned, or already assigned to this agent (retries).
    """
    caps = _parse_capabilities(request)  # None = accept anything (back-compat)
    deadline = _now() + (wait_ms / 1000.0)

    while _now() < deadline:
        # scan for first eligible task
        for i, t in enumerate(queue):
            # capability check (if provided)
            if caps is not None and t.get("op") not in caps:
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
      1) Legacy count: {"type":"sha256","count":5}
      2) Items list:   {"items":[...], "task":"sha256"}
      3) Single task:  {"op":"map_tokenize","payload":{...}}   (preferred)
      4) Alt keys:     {"type":"map_tokenize","params":{...}}  (compat)
    """
    queued = 0

    # Case 3: single task with explicit op/payload
    if "op" in payload and "payload" in payload:
        t = _normalize_single_task(payload["op"], payload["payload"])
        queue.append(t); queued += 1
        return {"queued": queued, "op": t["op"]}

    # Case 4: alt keys -> normalize to op/payload
    if "type" in payload and "params" in payload:
        t = _normalize_single_task(payload["type"], payload["params"])
        queue.append(t); queued += 1
        return {"queued": queued, "op": t["op"]}

    # Case 2: items list with per-item payload
    if "items" in payload:
        model = SeedItems(**payload)
        for item in model.items:
            t = _normalize_single_task(model.task, item)
            queue.append(t); queued += 1
        return {"queued": queued, "op": model.task}

    # Case 1: legacy count seeding
    model = SeedLegacy(**payload)
    for _ in range(model.count):
        data = uuid.uuid4().hex
        t = _normalize_single_task(model.type, data)
        queue.append(t); queued += 1
    return {"queued": queued, "op": model.type}

@app.post("/submit_job")
def submit_job(job: JobIn):
    """
    Enqueue a single job as one normalized task.
    job_id == task_id for easy tracing.
    """
    job_id = _new_id()
    task = _normalize_single_task(job.type, job.payload, given_id=job_id)
    queue.append(task)
    task_meta[job_id].update({"job_id": job_id})

    jobs[job_id] = {
        "id": job_id,
        "type": job.type,
        "payload": job.payload,
        "resources": job.resources,
        "policy": job.policy,
        "tenant": job.tenant,
        "status": "queued",
        "queued_ts": _now(),
    }
    return {"job_id": job_id, "status": "queued"}

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
    POST /purge      -> clear queue only
    POST /purge?all=1 -> clear queue + results + meta + jobs
    """
    queue.clear()
    if all:
        results.clear()
        task_meta.clear()
        jobs.clear()
    return {"purged": True, "all": bool(all), "queue_len": len(queue)}
