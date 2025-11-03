# app.py — minimal controller: health, task queue, results (stable, lenient I/O)

from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import hashlib, time, uuid, asyncio

app = FastAPI()
queue: List[Dict[str, Any]] = []         # in-memory task queue (tiny, on purpose)
results: Dict[str, Dict[str, Any]] = {}  # in-memory results store

# ---------- Models ----------
class Result(BaseModel):
    id: str
    agent: str
    ok: bool
    output: Optional[str] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None

class SeedLegacy(BaseModel):
    # legacy format you used: {"type":"sha256","count":5}
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
    Returns 204 if no work after the wait window (unless empty=1, then 200 with {}).
    """
    deadline = time.time() + (wait_ms / 1000.0)
    while time.time() < deadline:
        for i, t in enumerate(queue):
            if t.get("assigned_to") in (None, agent):
                task = queue.pop(i)
                task["assigned_to"] = agent
                return JSONResponse(task, status_code=200)
        await asyncio.sleep(0.02)

    if empty == 1:
        return JSONResponse({}, status_code=200)
    return JSONResponse(status_code=204, content=None)

# ---------- Result ingest ----------
@app.post("/result")
def post_result(r: Dict[str, Any] = Body(...)):
    """
    Accepts either the strict Result model or a superset.
    We normalize into the Result-like dict and store by id.
    """
    # Basic validation
    if "id" not in r or "agent" not in r:
        raise HTTPException(status_code=422, detail="Result must include 'id' and 'agent'")

    rec = {
        "id": r.get("id"),
        "agent": r.get("agent"),
        "ok": bool(r.get("ok", True)),
        "output": r.get("output"),
        "duration_ms": r.get("duration_ms"),
        "error": r.get("error"),
        # stash raw just in case
        "_raw": r,
        "ts": time.time(),
    }
    results[rec["id"]] = rec
    return {"stored": True, "id": rec["id"]}

# ---------- Seed helpers ----------
@app.post("/seed")
def seed(payload: Dict[str, Any] = Body(...)):
    """
    Accepts both:
      - Legacy: {"type": "sha256", "count": 5}
      - Items:  {"items": [...], "task": "sha256"}
    """
    queued = 0
    op = None

    # Try items-format first
    if "items" in payload:
        model = SeedItems(**payload)
        op = model.task
        for item in model.items:
            tid = f"tsk-{uuid.uuid4().hex[:8]}"
            queue.append({"id": tid, "op": op, "payload": item})
            queued += 1
        return {"queued": queued, "op": op}

    # Fallback legacy format
    model = SeedLegacy(**payload)
    op = model.type
    for _ in range(model.count):
        tid = f"tsk-{uuid.uuid4().hex[:8]}"
        # minimal synthetic payload for sha256 work; agents can ignore if they generate their own
        payload = uuid.uuid4().hex.encode()
        queue.append({"id": tid, "op": op, "payload": payload.hex()})
        queued += 1
    return {"queued": queued, "op": op}

# ---------- Results dump ----------
@app.get("/results")
def all_results():
    return results
