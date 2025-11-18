import time
import uuid
import threading
from collections import deque, defaultdict
from typing import Any, Deque, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse, JSONResponse

app = FastAPI(title="Distributed Swarm Controller")

# -----------------------------------------------------------------------------
# In-memory state
# -----------------------------------------------------------------------------

STATE_LOCK = threading.Lock()

# agent_name -> {labels, capabilities, last_seen}
AGENTS: Dict[str, Dict[str, Any]] = {}

# job_id -> job dict
JOBS: Dict[str, Dict[str, Any]] = {}

# FIFO queue of job_ids waiting to be leased
TASK_QUEUE: Deque[str] = deque()

# counters
LEASED_TOTAL = 0
COMPLETED_TOTAL = 0
FAILED_TOTAL = 0

# timestamps of completed jobs (epoch seconds) for rate & sparkline
COMPLETION_TIMES: Deque[float] = deque()

# per-op stats: op -> {"count": int, "total_duration_ms": float}
OP_STATS: Dict[str, Dict[str, float]] = defaultdict(
    lambda: {"count": 0, "total_duration_ms": 0.0}
)

# how long we care about completion timestamps (seconds)
MAX_COMPLETION_WINDOW_S = 15 * 60  # 15 minutes


def _now() -> float:
    return time.time()


def _prune_completion_times(now: Optional[float] = None) -> None:
    """Keep COMPLETION_TIMES within MAX_COMPLETION_WINDOW_S."""
    if now is None:
        now = _now()
    cutoff = now - MAX_COMPLETION_WINDOW_S
    while COMPLETION_TIMES and COMPLETION_TIMES[0] < cutoff:
        COMPLETION_TIMES.popleft()


def _calc_rate(window_s: float, now: Optional[float] = None) -> float:
    """Tasks/sec over the last window_s seconds."""
    if window_s <= 0:
        return 0.0
    if now is None:
        now = _now()
    cutoff = now - window_s
    _prune_completion_times(now)
    count = sum(1 for t in COMPLETION_TIMES if t >= cutoff)
    return count / window_s if count > 0 else 0.0


def _enqueue_job(op: str, payload: Dict[str, Any]) -> str:
    global JOBS, TASK_QUEUE
    job_id = str(uuid.uuid4())
    now = _now()
    job = {
        "id": job_id,
        "op": op,
        "payload": payload,
        "created_ts": now,
        "leased_ts": None,
        "leased_by": None,
        "lease_timeout_s": 300,
        "status": "queued",  # queued | leased | completed | failed
        "result": None,
        "error": None,
        "completed_ts": None,
        "duration_ms": None,
    }
    JOBS[job_id] = job
    TASK_QUEUE.append(job_id)
    return job_id


def _lease_next_job(agent: str) -> Optional[Dict[str, Any]]:
    """Pop the next job from the queue and mark it leased."""
    global LEASED_TOTAL

    now = _now()
    while TASK_QUEUE:
        job_id = TASK_QUEUE.popleft()
        job = JOBS.get(job_id)
        if not job:
            continue

        # If job somehow already finished, skip
        if job["status"] not in ("queued", "leased"):
            continue

        job["status"] = "leased"
        job["leased_by"] = agent
        job["leased_ts"] = now
        LEASED_TOTAL += 1

        return {
            "id": job["id"],
            "type": job["op"],
            "payload": job["payload"],
            "timeout_ms": job["lease_timeout_s"] * 1000,
        }

    return None


# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------

@app.get("/healthz", response_class=PlainTextResponse)
def healthz() -> PlainTextResponse:
    """Simple health check for Docker / UI."""
    with STATE_LOCK:
        queue_len = len(TASK_QUEUE)
        agents_online = len(AGENTS)
    return PlainTextResponse(
        f"ok queue={queue_len} agents={agents_online}",
        media_type="text/plain",
    )


# -----------------------------------------------------------------------------
# Agents
# -----------------------------------------------------------------------------

@app.post("/agents/register")
async def register_agent(request: Request) -> Dict[str, Any]:
    """
    Register an agent.

    Accepts either:
      {"agent": "name", "labels": {...}, "capabilities": {...}}
    or:
      {"name": "name", ...}
    """
    payload = await request.json()
    name = payload.get("agent") or payload.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Missing 'agent' or 'name'")

    labels = payload.get("labels") or {}
    capabilities = payload.get("capabilities") or {}
    now = _now()

    with STATE_LOCK:
        AGENTS[name] = {
            "labels": labels,
            "capabilities": capabilities,
            "last_seen": now,
        }

    return {"status": "ok", "agent": name}


@app.post("/agents/heartbeat")
async def agent_heartbeat(request: Request) -> Dict[str, Any]:
    """
    Agent heartbeat. Same shape as register.

    Controllers in the wild already use this heavily, so keep it forgiving.
    """
    payload = await request.json()
    name = payload.get("agent") or payload.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Missing 'agent' or 'name'")

    labels = payload.get("labels") or {}
    capabilities = payload.get("capabilities") or {}
    now = _now()

    with STATE_LOCK:
        entry = AGENTS.get(name, {})
        entry.setdefault("labels", {}).update(labels)
        entry.setdefault("capabilities", {}).update(capabilities)
        entry["last_seen"] = now
        AGENTS[name] = entry

    return {"status": "ok", "agent": name, "time": now}


@app.get("/agents")
def list_agents() -> Dict[str, Any]:
    """Return agents in the shape the UI & your PS script expect."""
    with STATE_LOCK:
        # shallow copy so we don't leak internal refs
        return {
            name: {
                "labels": dict(info.get("labels") or {}),
                "capabilities": dict(info.get("capabilities") or {}),
                "last_seen": info.get("last_seen"),
            }
            for name, info in AGENTS.items()
        }


# -----------------------------------------------------------------------------
# Task leasing
# -----------------------------------------------------------------------------

@app.get(
    "/task",
    summary="Agent polls for next task",
)
def get_task(agent: str, wait_ms: int = 0):
    """
    GET /task?agent=agent-1&wait_ms=1000

    Returns:
      200 + JSON task if available
      204 if none available before wait_ms
    """
    deadline = _now() + (wait_ms / 1000.0)

    while True:
        with STATE_LOCK:
            task = _lease_next_job(agent)

        if task is not None:
            return JSONResponse(task)

        if _now() >= deadline:
            # nothing to do
            return JSONResponse(status_code=204, content=None)

        # brief sleep to avoid hot-spinning the CPU
        time.sleep(0.05)


@app.post("/result")
async def post_result(request: Request) -> Dict[str, Any]:
    """
    Agent posts result:

    {
      "id": "<job_id>",
      "agent": "agent-1",
      "ok": true/false,
      "result": {...} | null,
      "error": "..." | null,
      "op": "map_tokenize",
      "duration_ms": float | null
    }
    """
    global COMPLETED_TOTAL, FAILED_TOTAL

    payload = await request.json()
    job_id = payload.get("id")
    if not job_id:
        raise HTTPException(status_code=400, detail="Missing 'id'")

    now = _now()

    with STATE_LOCK:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Unknown job id")

        ok = bool(payload.get("ok", True))
        job["status"] = "completed" if ok else "failed"
        job["result"] = payload.get("result")
        job["error"] = payload.get("error")
        job["completed_ts"] = now

        # duration
        duration_ms = payload.get("duration_ms")
        if duration_ms is None and job.get("leased_ts") is not None:
            duration_ms = (now - job["leased_ts"]) * 1000.0
        job["duration_ms"] = duration_ms

        # update counters
        if ok:
            COMPLETED_TOTAL += 1
        else:
            FAILED_TOTAL += 1

        # completion timeline (throughput & sparkline)
        COMPLETION_TIMES.append(now)
        _prune_completion_times(now)

        # op stats
        op = job.get("op") or payload.get("op") or "unknown"
        stat = OP_STATS[op]
        stat["count"] += 1
        if duration_ms is not None:
            stat["total_duration_ms"] += float(duration_ms)

    return {"status": "ok", "id": job_id}


# -----------------------------------------------------------------------------
# Jobs & results inspection
# -----------------------------------------------------------------------------

@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> Dict[str, Any]:
    with STATE_LOCK:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Unknown job id")
        return job


@app.get("/results")
def list_results(limit: int = 100) -> List[Dict[str, Any]]:
    """Recent finished jobs."""
    with STATE_LOCK:
        finished = [
            j
            for j in JOBS.values()
            if j.get("status") in ("completed", "failed")
        ]
        finished.sort(key=lambda j: j.get("completed_ts") or 0.0, reverse=True)
        return finished[:limit]


@app.get("/timeline")
def timeline(limit: int = 200) -> List[Dict[str, Any]]:
    """Simple job timeline ordered by creation time."""
    with STATE_LOCK:
        items = list(JOBS.values())
        items.sort(key=lambda j: j.get("created_ts") or 0.0, reverse=True)
        return items[:limit]


# -----------------------------------------------------------------------------
# Stats & metrics (used by ds-ui)
# -----------------------------------------------------------------------------

@app.get("/stats")
def stats() -> Dict[str, Any]:
    """
    Aggregate stats.

    Shape matches what your PowerShell output showed:
      time, agents_online, queue_len, leased_total,
      completed_total, failed_total, rate_1s, rate_60s, rate_5m, rate_15m,
      op_counts, op_avg_ms
    """
    now = _now()
    with STATE_LOCK:
        agents_online = len(AGENTS)
        queue_len = len(TASK_QUEUE)
        leased_total = LEASED_TOTAL
        completed_total = COMPLETED_TOTAL
        failed_total = FAILED_TOTAL

        _prune_completion_times(now)
        rate_1s = _calc_rate(1, now)
        rate_60s = _calc_rate(60, now)
        rate_5m = _calc_rate(5 * 60, now)
        rate_15m = _calc_rate(15 * 60, now)

        op_counts = {op: int(stat["count"]) for op, stat in OP_STATS.items()}
        op_avg_ms = {
            op: (stat["total_duration_ms"] / stat["count"])
            if stat["count"] > 0
            else 0.0
            for op, stat in OP_STATS.items()
        }

    return {
        "time": int(now),
        "agents_online": agents_online,
        "queue_len": queue_len,
        "leased_total": leased_total,
        "completed_total": completed_total,
        "failed_total": failed_total,
        "rate_1s": rate_1s,
        "rate_60s": rate_60s,
        "rate_5m": rate_5m,
        "rate_15m": rate_15m,
        "op_counts": op_counts,
        "op_avg_ms": op_avg_ms,
    }


@app.get("/stats/sparkline")
def stats_sparkline(seconds: int = 60) -> Dict[str, Any]:
    """
    Throughput sparkline for the UI.

    Returns:
      {
        "window_s": 60,
        "points": [
          {"t": <unix>, "tps": <float>}, ...
        ]
      }
    """
    if seconds <= 0:
        seconds = 60

    now = _now()
    start = now - seconds

    with STATE_LOCK:
        _prune_completion_times(now)
        # copy timestamps to avoid holding lock while doing O(n * window) work
        timestamps = list(COMPLETION_TIMES)

    # bucket into 1s intervals
    points: List[Dict[str, float]] = []
    for i in range(seconds):
        bucket_start = start + i
        bucket_end = bucket_start + 1.0
        count = sum(
            1 for t in timestamps if bucket_start <= t < bucket_end
        )
        points.append({"t": bucket_start, "tps": float(count)})

    return {"window_s": seconds, "points": points}


@app.get("/stats/ops")
def stats_ops() -> Dict[str, Any]:
    """
    Per-op metrics for the table in the dashboard.

    {
      "ops": {
        "map_tokenize": {
          "count": 123,
          "avg_duration_ms": 45.67
        },
        ...
      }
    }
    """
    with STATE_LOCK:
        ops = {
            op: {
                "count": int(stat["count"]),
                "avg_duration_ms": (
                    stat["total_duration_ms"] / stat["count"]
                    if stat["count"] > 0
                    else 0.0
                ),
            }
            for op, stat in OP_STATS.items()
        }
    return {"ops": ops}


# -----------------------------------------------------------------------------
# Seeding & admin
# -----------------------------------------------------------------------------

@app.post("/seed")
@app.post("/api/seed")  # extra compatibility; nginx usually strips /api
async def seed_job(request: Request) -> Dict[str, Any]:
    """
    Enqueue a job.

    Accepts either:
      JSON: { "type": "map_tokenize", "payload": { ... } }
    or query params: /seed?type=map_tokenize&text=smoke+test
    """
    if request.headers.get("content-type", "").startswith(
        "application/json"
    ):
        body = await request.json()
        op = body.get("type")
        payload = body.get("payload") or {}
    else:
        # fall back to query params
        qp = dict(request.query_params)
        op = qp.get("type")
        payload = {k: v for k, v in qp.items() if k != "type"}

    if not op:
        raise HTTPException(status_code=400, detail="Missing 'type'")

    with STATE_LOCK:
        job_id = _enqueue_job(op, payload)

    # Keep response shape similar to what your PS table showed
    return {"queued": 0, "op": op, "requires": None, "job_id": job_id}


@app.post("/purge")
def purge() -> Dict[str, Any]:
    """Brutal reset. Nukes jobs & stats, keeps agents."""
    global JOBS, TASK_QUEUE, LEASED_TOTAL, COMPLETED_TOTAL, FAILED_TOTAL
    global COMPLETION_TIMES, OP_STATS

    with STATE_LOCK:
        JOBS = {}
        TASK_QUEUE = deque()
        LEASED_TOTAL = 0
        COMPLETED_TOTAL = 0
        FAILED_TOTAL = 0
        COMPLETION_TIMES = deque()
        OP_STATS = defaultdict(
            lambda: {"count": 0, "total_duration_ms": 0.0}
        )

    return {"status": "ok", "purged": True}


# -----------------------------------------------------------------------------
# Local dev entrypoint (container uses uvicorn app:app)
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=True)
