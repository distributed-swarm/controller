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
    """Incoming job from UI or API client."""
    type: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    requires: Dict[str, Any] = Field(default_factory=dict)


class TaskOut(BaseModel):
    """Task sent to agents."""
    id: str
    op: str
    payload: Any


class ResultIn(BaseModel):
    """Result posted back from agents."""
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
task_queue: List[str] = []          # queue of job_ids waiting to be run
results: List[Dict[str, Any]] = []  # flat list of result records


# -------------------- helpers --------------------


def now_ts() -> int:
    return int(time.time())


def make_job_id() -> str:
    return f"job-{uuid.uuid4().hex[:12]}"


def agent_status(last_seen: float,
                 stale_after: int = 45,
                 offline_after: int = 300) -> str:
    delta = time.time() - last_seen
    if delta < stale_after:
        return "online"
    if delta < offline_after:
        return "stale"
    return "offline"


# -------------------- health --------------------


@app.get("/healthz", response_class=PlainTextResponse)
def healthz() -> str:
    return "ok"


# -------------------- agent registration / heartbeat --------------------


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
    if env.labels:
        rec["labels"] = env.labels
    if env.capabilities:
        rec["capabilities"] = env.capabilities.dict()
    rec["last_seen"] = env.timestamp
    return {"ok": True}


@app.get("/agents")
def list_agents():
    out = {}
    for name, data in agents.items():
        last_seen = data.get("last_seen", 0)
        out[name] = {
            "labels": data.get("labels", {}),
            "capabilities": data.get("capabilities", {}),
            "last_seen": last_seen,
            "status": agent_status(last_seen),
        }
    return out


# -------------------- jobs & task dispatch --------------------


@app.post("/submit_job", response_model=str)
def submit_job(job: JobIn):
    """
    Enqueue a single job. Job id doubles as task id.
    Expected body:
      {
        "type": "map_tokenize",
        "payload": { ... },
        "requires": { ... }   # optional
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
        "duration_ms": None,
    }
    task_queue.append(job_id)
    return job_id


@app.get(
    "/task",
    responses={204: {"description": "No task available"}},
)
def get_task(
    agent: str = Query(..., description="Agent name"),
    wait_ms: int = Query(0, description="Long-poll wait (unused for now)"),
):
    """
    Called by agents to fetch next work item.
    Returns:
      200 + JSON task {id, op, payload} or
      204 when there is no work.
    """
    if not task_queue:
        return Response(status_code=204)

    job_id = task_queue.pop(0)
    job = jobs.get(job_id)
    if not job:
        return Response(status_code=204)

    job["status"] = "in_flight"
    job["started_ts"] = now_ts()
    job["agent"] = agent

    task = {
        "id": job_id,
        "op": job["type"],
        "payload": job["payload"],
    }
    return JSONResponse(task)


@app.post("/result")
def store_result(res: ResultIn):
    """
    Agent posts job result here.
    """
    job = jobs.get(res.id)
    if not job:
        # don't kill the agent for old/unknown ids
        return {"ok": False, "error": "unknown job id"}

    job["status"] = "completed" if res.ok else "failed"
    job["done_ts"] = now_ts()
    job["result"] = res.output
    job["error"] = res.error
    job["duration_ms"] = res.duration_ms
    if res.agent:
        job["agent"] = res.agent

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

    # basic throughput: completed in last 60s
    cutoff = now_ts() - 60
    throughput_60s = sum(
        1
        for j in jobs.values()
        if j["status"] == "completed" and (j["done_ts"] or 0) >= cutoff
    )

    return {
        "jobs_total": total,
        "jobs_completed": completed,
        "jobs_failed": failed,
        "jobs_in_flight": in_flight,
        "jobs_queued": queued,
        "throughput_60s": throughput_60s,
        "agents": {
            name: {
                "status": agent_status(a.get("last_seen", 0)),
                "last_seen": a.get("last_seen", 0),
                "labels": a.get("labels", {}),
                "capabilities": a.get("capabilities", {}),
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
    items.sort(key=lambda x: x["created_ts"])
    return items[-limit:]


# -------------------- seed & purge helpers --------------------


@app.post("/seed")
def seed():
    """
    Dev helper: enqueue a simple demo job.
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
        "duration_ms": None,
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
