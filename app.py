# app.py — minimal controller: health, task queue, results
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict
import hashlib, time, uuid
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
def get_task(agent: str):
    # pop first task
    for i, t in enumerate(queue):
        if t.get("assigned_to") in (None, agent):
            task = queue.pop(i)
            task["assigned_to"] = agent
            return task
    return Response(status_code=204)

@app.post("/result")
def post_result(r: Result):
    results[r.id] = r.dict()
    return {"stored": True}

# Testing helpers
@app.post("/seed")
def seed(n: int = 5, op: str = "sha256", payload: str = "hello-world"):
    for _ in range(n):
        tid = f"tsk-{uuid.uuid4().hex[:8]}"
        queue.append({"id": tid, "op": op, "payload": payload})
    return {"queued": n, "op": op}

@app.get("/results")
def all_results():
    return results
