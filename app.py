# app.py — minimal controller: health, task queue, results
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict
import hashlib, time, uuid
import asyncio, time
from starlette.responses import Response
from starlette.responses import Response

app = FastAPI()
queue: List[Dict] = []         # in-memory task queue (tiny, on purpose)
results: Dict[str, Dict] = {}  # in-memory results store

class Result(BaseModel):
    id: str
    agent: str
    ok: bool
    output: Optional[str] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None

@app.get("/healthz")
def healthz():
    return {"status": "ok", "time": time.strftime("%Y-%m-%dT%H:%M:%S%z")}

@app.get("/task")
async def get_task(agent: str, wait_ms: int = 2000):
    """
    Long-poll for a task up to wait_ms, prefer tasks unassigned or already assigned to this agent.
    Returns 204 if no work after the wait window.
    """
    deadline = time.time() + (wait_ms / 1000.0)
    while time.time() < deadline:
        for i, t in enumerate(queue):
            if t.get("assigned_to") in (None, agent):
                task = queue.pop(i)
                task["assigned_to"] = agent
                return task
        # nothing yet; yield briefly to avoid hot-looping
        await asyncio.sleep(0.02)
    return Response(status_code=204)

@app.post("/result")
def post_result(r: Result):
    results[r.id] = r.dict()
    return {"stored": True}

# Testing helpers
@app.post("/seed")
def seed(data: dict):
    items = data.get("items", [])
    op = data.get("task", "sha256")
    for item in items:
        tid = f"tsk-{uuid.uuid4().hex[:8]}"
        queue.append({"id": tid, "op": op, "payload": item})
    return {"queued": len(items), "op": op}

@app.get("/results")
def all_results():
    return results
