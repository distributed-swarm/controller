# controller app.py – stable reset
# FastAPI controller for swarm: agents, tasks, jobs, stats.

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
import time
import uuid

app = FastAPI(title="Distributed Swarm Controller", version="1.0")

# -------------------- models --------------------

class JobIn(BaseModel):
    type: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    requires: Dict[str, Any] = Field(default_factory=dict)


class TaskOut(BaseModel):
    id: str
    op: str
    payload: Any


class ResultIn(BaseModel):
    id: str
    agent: str
    ok: bool
    output: Optional[Any] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None
    op: Optional[str] = None


class AgentCaps(BaseModel):
    gpu: Dict[str, Any] = Field(default_factory=dict)
    cpu: Dict[str, Any] = Field(default_factory=dict)
    memory: Dict[str, Any] = Field(default_factory=dict)
    workers: Dict[str, Any] = Field(default_factory=dict)


class AgentEnvelope(BaseModel):
    agent: str
    labels: Dict[str, str] = Field(default_factory=dict)
    capabilities: AgentCaps = Field(default_factory=AgentCaps)
    timestamp: int


# -------------------- in-memory state --------------------

agents: Dict[str, Dict[str, Any]] = {}
jobs: Dict[str, Dict[str, Any]] = {}
task_queue: List[str] = []     # list of job_ids waiting to be turned into tasks
results: List[Dict[str, Any]] = []


# -------------------- helpers --------------------

def now_ts() -> int:
    return int(time.time())


def make_job_id() -> str:
    return f"job-{uuid.uuid4().hex[:12]}"


def agent_status(last_seen: float, stale_after: int = 45, offline_after: int = 300) -> str:
    delta = time.time() - last_seen
    if delta < stale_after:
        return "online"
    if delta < offline_after:
        return "stale"
    return "offline"


# -------------------- healthz --------------------

@app.get("/healthz", response_class=PlainTextResponse)
def healthz() -> str:
    return "ok"


# -------------------- agents --------------------

@app.post("/agents/register")
def agents_register(env: AgentEnvelope):
    agents[env.agent] = {
        "name": env.agent,
        "labels": env.labels,
        "capabilities": env.capabilities.dict(),
        "last_seen": env.timestamp,
    }
    return {"ok": True}


@app.post("/agents/heartbeat")
def agents_heartbeat(env: AgentEnvelope):
    rec = agents.setdefault(
        env.agent,
        {
            "name": env.agent,
            "labels": env.labels,
            "capabilities": env.capabilities.dict(),
            "last_seen": env.timestamp,
        },
    )
    rec["labels"] = env.labels or rec.get("labels", {})
    rec["capabilities"] = env.capabilities.dict() or rec.get("capabilities", {})
    rec["last_seen"] = env.timestamp
    return {"ok": True}


@app.get("/agents")
def list_agents():
    out = {}
    for name, data in agents.items():
        out[name] = {
            "labels": data.get("labels", {}),
            "capabilities": data.get("capabilities", {}),
            "last_seen": data.get("last_seen", 0),
            "status": agent_status(data.get("last_seen", 0.0)),
        }
    return out


# -------------------- jobs + tasks --------------------

@app.post("/submit_job", response_model=str)
def submit_job(job: JobIn):
    """
    Enqueue a single job as one normalized task.
    Request body:
    {
      "type": "map_tokenize",
      "payload": { ... },
      "requires": { ... }   # optional; currently ignored
    }
    """
    job_id = make_job_id()
    jobs[job_id] = {
        "id": job_id,
        "type": job.type,
        "payload": job.payload,
        "requires": job.requires,
        "status": "queued",
        "created_ts": now_ts(),
        "started_ts": None,
        "done_ts": None,
        "agent": None,
        "result": None,
        "error": None,
    }
    task_queue.append(job_id)
    return job_id


@app.get("/task", responses={204: {"description": "No task available"}})
def get_task(agent: str = Query(..., description="Agent name"),
             wait_ms: int = Query(0, description="Long poll (unused for now)")):
    """
    Called by agents to fetch the next task.
    Returns 204 when there is no work.
    """
    # fast path: no tasks
    if not task_queue:
        return Response(status_code=204)

    # simple FIFO queue
    job_id = task_queue.pop(0)
    job = jobs.get(job_id)
    if not job:
        # should not happen; skip
        return Response(status_code=204)

    job["status"] = "in_flight"
    job["started_ts"] = now_ts()
    job["agent"] = agent

    # shape matches what agent.run_task() expects:
    #   op or type; payload or params
    task = {
        "id": job_id,
        "op": job["type"],
        "payload": job["payload"],
    }
    return JSONResponse(task)


@app.post("/result")
def store_result(res: ResultIn):
    job = jobs.get(res.id)
    if not job:
        # ignore unknown job ids to keep agents simple
        return {"ok": False, "error": "unknown job id"}

    job["status"] = "completed" if res.ok else "failed"
    job["done_ts"] = now_ts()
    job["result"] = res.output
    job["error"] = res.error
    job["duration_ms"] = res.duration_ms
    job["agent"] = res.agent or job.get("agent")

    results.append(
        {
            "id": res.id,
            "agent": res.agent,
            "ok": res.ok,
            "output": res.output,
            "error": res.error,
            "duration_ms": res.duration_ms,
            "op": res.op,
            "ts": now_ts(),
        }
    )
    return {"ok": True}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.get("/results")
def list_results(limit: int = 50):
    return results[-limit:]


# -------------------- stats & timeline --------------------

@app.get("/stats")
def stats():
    total = len(jobs)
    completed = sum(1 for j in jobs.values() if j["status"] == "completed")
    failed = sum(1 for j in jobs.values() if j["status"] == "failed")
    in_flight = sum(1 for j in jobs.values() if j["status"] == "in_flight")
    queued = sum(1 for j in jobs.values() if j["status"] == "queued")

    return {
        "jobs_total": total,
        "jobs_completed": completed,
        "jobs_failed": failed,
        "jobs_in_flight": in_flight,
        "jobs_queued": queued,
        "agents": {
            name: {
                "status": agent_status(a.get("last_seen", 0.0)),
                "last_seen": a.get("last_seen", 0),
            }
            for name, a in agents.items()
        },
    }


@app.get("/timeline")
def timeline(limit: int = 100):
    items = []
    for job in jobs.values():
        items.append(
            {
                "id": job["id"],
                "type": job["type"],
                "status": job["status"],
                "created_ts": job["created_ts"],
                "started_ts": job["started_ts"],
                "done_ts": job["done_ts"],
                "agent": job["agent"],
            }
        )
    # sort by created time
    items.sort(key=lambda x: x["created_ts"])
    return items[-limit:]


@app.post("/seed")
def seed():
    """
    Optional: quick dev endpoint to enqueue a demo job.
    """
    job_id = make_job_id()
    payload = {
        "text": "hello swarm seed",
        "chunk_size": 512,
        "overlap": 64,
    }
    jobs[job_id] = {
        "id": job_id,
        "type": "map_tokenize",
        "payload": payload,
        "requires": {},
        "status": "queued",
        "created_ts": now_ts(),
        "started_ts": None,
        "done_ts": None,
        "agent": None,
        "result": None,
        "error": None,
    }
    task_queue.append(job_id)
    return {"ok": True, "job_id": job_id}


@app.post("/purge")
def purge():
    agents.clear()
    jobs.clear()
    task_queue.clear()
    results.clear()
    return {"ok": True}
