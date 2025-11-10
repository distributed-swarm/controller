# app.py — controller: health, task queue, results (traceable, backwards-compatible)

from fastapi import FastAPI, HTTPException, Body, Query, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import time, uuid, asyncio

app = FastAPI()

# In-memory stores
queue: List[Dict[str, Any]] = []             # pending tasks
results: Dict[str, Dict[str, Any]] = {}      # stored results by id
task_meta: Dict[str, Dict[str, Any]] = {}    # id -> {"op": str, "seed_ts": float}

# ---------- Models ----------
class Result(BaseModel):
    id: str
    agent: str
    ok: bool
    output: Optional[str] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None

class SeedLegacy(BaseModel):
    # legacy format: {"type":"sha256","count":5}
    type: str = Field(default="sha256")
    count: int = Field(ge=0, le=100000, default=0)

class SeedItems(BaseModel):
    # list format: {"items":[...], "task":"sha256"}
    items: List[Any] = Field(default_factory=list)
    task: str = Field(default="sha256")

# ---------- Health ----------
@app.get("/healthz")
def healthz():
    return {"status": "ok", "time": time.strftime("%Y-%m-%dT%H:%M:%S%z")}

# ---------- Task leasing ----------
@app.get("/task")
async def get_task(
    agent: str = Query(..., description="Agent identifier"),
    wait_ms: int = Query(2000, ge=0, le=120000),
    empty: int = Query(0, description="If 1, return {} with 200 when no task instead of 204"),
):
    """
    Long-poll for a task up to wait_ms, prefer tasks unassigned or already assigned to this agent.
    Returns 204 if no work after the wait window (unless empty=1 -> 200 with {}).
    """
    deadline = time.time() + (wait_ms / 1000.0)
    while time.time() < deadline:
        for i, t in enumerate(queue):
            if t.get("assigned_to") in (None, agent):
                task = queue.pop(i)
                task["assigned_to"] = agent
                task["leased_at"] = time.time()
                return JSONResponse(task, status_code=200)
        await asyncio.sleep(0.02)

    if empty == 1:
        return JSONResponse({}, status_code=200)
    return Response(status_code=204)  # 204 must have no body

# ---------- Result ingest ----------
@app.post("/result")
def post_result(r: Dict[str, Any] = Body(...)):
    """
    Accepts either the strict Result model or a superset.
    Normalizes into a Result-like dict and stores by id.
    """
    # Basic validation
    if "id" not in r or "agent" not in r:
        raise HTTPException(status_code=422, detail="Result must include 'id' and 'agent'")

    rid = str(r.get("id"))
    meta = task_meta.get(rid, {})
    op = meta.get("op", "")

    # derive a human-friendly trace from op if present, e.g., "trace-123456.sha256" -> "trace-123456"
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
        "_raw": r,                   # stash original payload
        "ts": time.time(),           # stored timestamp
        "assigned_to": meta.get("assigned_to"),
        "leased_at": meta.get("leased_at"),
        "seed_ts": meta.get("seed_ts"),
    }
    results[rid] = rec
    return {"stored": True, "id": rid}

# ---------- Seed helpers ----------
@app.post("/seed")
def seed(payload: Dict[str, Any] = Body(...)):
    """
    Accepts both:
      - Legacy: {"type": "sha256", "count": 5}
      - Items:  {"items": [...], "task": "sha256"}
    """
    queued = 0

    # Items-format first
    if "items" in payload:
        model = SeedItems(**payload)
        op = model.task
        for item in model.items:
            tid = f"tsk-{uuid.uuid4().hex[:8]}"
            queue.append({"id": tid, "op": op, "payload": item})
            task_meta[tid] = {"op": op, "seed_ts": time.time()}
            queued += 1
        return {"queued": queued, "op": op}

    # Fallback legacy format
    model = SeedLegacy(**payload)
    op = model.type
    for _ in range(model.count):
        tid = f"tsk-{uuid.uuid4().hex[:8]}"
        data = uuid.uuid4().hex  # synthetic payload for sha256 work
        queue.append({"id": tid, "op": op, "payload": data})
        task_meta[tid] = {"op": op, "seed_ts": time.time()}
        queued += 1
    return {"queued": queued, "op": op}

# ---------- Results dump ----------
@app.get("/results")
def all_results(
    limit: int = Query(0, ge=0, description="If >0, return only the latest N results"),
    agent: Optional[str] = Query(None),
    op: Optional[str] = Query(None),
    trace: Optional[str] = Query(None),
):
    """
    Returns stored results as a dict keyed by task id (backwards compatible).
    Optional filters + limit to avoid flooding terminals.
    """
    items = list(results.values())

    if agent:
        items = [r for r in items if r.get("agent") == agent]
    if op:
        items = [r for r in items if r.get("op") == op]
    if trace:
        items = [r for r in items if r.get("trace") == trace]

    items.sort(key=lambda r: r.get("ts", 0))  # oldest -> newest
    if limit and limit > 0:
        items = items[-limit:]

    # keep the original "dict of dicts" shape
    return {r["id"]: r for r in items}
