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

AGENTS: Dict[str, Dict[str, Any]] = {}
JOBS: Dict[str, Dict[str, Any]] = {}
TASK_QUEUE: Deque[str] = deque()

LEASED_TOTAL = 0
COMPLETED_TOTAL = 0
FAILED_TOTAL = 0

COMPLETION_TIMES: List[float] = []
OP_COUNTS: Dict[str, int] = defaultdict(int)
OP_TOTAL_MS: Dict[str, float] = defaultdict(float)

# -----------------------------------------------------------------------------
# Agent health / autonomic thresholds
# -----------------------------------------------------------------------------

# How long an agent can go without a heartbeat before we call it "dead"
DEAD_AGENT_THRESHOLD_S = 60.0

# Latency / error thresholds for degraded / suspect classification
DEGRADED_LATENCY_MS = 500.0       # above this, we start to worry
DEGRADED_ERROR_RATE = 0.10        # >10% failures → degraded (if enough samples)
SUSPECT_ERROR_RATE = 0.50         # >50% failures with enough volume → suspect
SUSPECT_MIN_TASKS = 5             # only call it suspect if it's actually done work

# Metrics we accept from agents in heartbeat/register payloads
AGENT_METRIC_KEYS = [
    "cpu_util",        # float 0..1
    "ram_mb",          # float or int
    "tasks_completed", # cumulative
    "tasks_failed",    # cumulative
    "avg_task_ms",     # moving average duration
    "latency_ms",      # observed round-trip latency
]


def _now() -> float:
    return time.time()


def _prune_completion_times(now: Optional[float] = None, window_s: float = 900.0) -> None:
    """
    Keep only completions in the last `window_s` seconds (default 15m).
    """
    if now is None:
        now = _now()

    cutoff = now - window_s
    i = 0
    while i < len(COMPLETION_TIMES) and COMPLETION_TIMES[i] < cutoff:
        i += 1
    if i > 0:
        del COMPLETION_TIMES[:i]


def _compute_agent_state(info: Dict[str, Any], now: Optional[float] = None) -> (str, str):
    """
    Compute a coarse health state for an agent based on last_seen + metrics.

    Returns:
      (state, reason)
      state ∈ {"healthy", "degraded", "suspect", "dead", "unknown"}
    """
    if now is None:
        now = _now()

    last_seen = info.get("last_seen")
    if last_seen is None:
        return "unknown", "no_heartbeat_yet"

    # Dead: missed heartbeat for too long
    if now - float(last_seen) > DEAD_AGENT_THRESHOLD_S:
        return "dead", "missed_heartbeat"

    metrics = info.get("metrics") or {}
    latency_ms = metrics.get("latency_ms")
    tasks_completed = metrics.get("tasks_completed") or 0
    tasks_failed = metrics.get("tasks_failed") or 0
    total_tasks = (tasks_completed or 0) + (tasks_failed or 0)

    # Default: assume healthy unless metrics say otherwise
    state = "healthy"
    reason = "normal"

    error_rate = 0.0
    if total_tasks > 0:
        error_rate = float(tasks_failed) / float(total_tasks)

    # High error rate with enough samples → suspect
    if total_tasks >= SUSPECT_MIN_TASKS and error_rate >= SUSPECT_ERROR_RATE:
        return "suspect", f"high_error_rate_{error_rate:.2f}"

    # Elevated error rate → degraded
    if total_tasks >= SUSPECT_MIN_TASKS and error_rate >= DEGRADED_ERROR_RATE:
        state = "degraded"
        reason = f"elevated_error_rate_{error_rate:.2f}"

    # High latency alone can also degrade
    if latency_ms is not None and latency_ms >= DEGRADED_LATENCY_MS:
        if state == "healthy":
            state = "degraded"
            reason = f"high_latency_{latency_ms:.1f}ms"
        else:
            # already degraded due to errors; just annotate
            reason += f"_and_high_latency_{latency_ms:.1f}ms"

    return state, reason


def _refresh_agent_states(now: Optional[float] = None) -> None:
    """
    Recompute health state for all agents.
    NOTE: Must be called with STATE_LOCK held.
    """
    if now is None:
        now = _now()

    for info in AGENTS.values():
        state, reason = _compute_agent_state(info, now)
        info["state"] = state
        info["health_reason"] = reason


# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------

@app.get("/healthz", response_class=PlainTextResponse)
def healthz() -> str:
    """
    Simple health check used by Docker/Portainer and UI.
    """
    with STATE_LOCK:
        now = _now()
        _refresh_agent_states(now)
        queue_len = len(TASK_QUEUE)
        agents_online = len(AGENTS)
        # count non-dead agents as "active"
        active_agents = sum(
            1 for info in AGENTS.values()
            if info.get("state") != "dead"
        )

    return PlainTextResponse(
        f"ok queue={queue_len} agents={agents_online} active_agents={active_agents}",
        media_type="text/plain",
    )


# -----------------------------------------------------------------------------
# Agents
# -----------------------------------------------------------------------------

@app.post("/agents/register")
@app.post("/api/agents/register")
async def register_agent(request: Request) -> Dict[str, Any]:
    """
    Register an agent.

    Accepts either:
      {"agent": "name", "labels": {...}, "capabilities": {...}}
    or:
      {"name": "name", ...}

    Optionally may include basic metrics fields (cpu_util, ram_mb, etc.).
    """
    payload = await request.json()
    name = payload.get("agent") or payload.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Missing 'agent' or 'name'")

    labels = payload.get("labels") or {}
    capabilities = payload.get("capabilities") or {}
    now = _now()

    # Extract optional metrics from payload
    metrics: Dict[str, Any] = {}
    for key in AGENT_METRIC_KEYS:
        if key in payload:
            metrics[key] = payload.get(key)

    with STATE_LOCK:
        info: Dict[str, Any] = {
            "labels": labels,
            "capabilities": capabilities,
            "last_seen": now,
            "metrics": metrics,
        }
        state, reason = _compute_agent_state(info, now)
        info["state"] = state
        info["health_reason"] = reason
        AGENTS[name] = info

    return {"status": "ok", "agent": name, "time": now}


@app.post("/agents/heartbeat")
@app.post("/api/agents/heartbeat")
async def agent_heartbeat(request: Request) -> Dict[str, Any]:
    """
    Agent heartbeat. Same shape as register, but forgiving.

    Existing agents that only send {agent/name, labels, capabilities}
    will continue to work. Newer agents can also send metrics:
      cpu_util, ram_mb, tasks_completed, tasks_failed, avg_task_ms, latency_ms
    """
    payload = await request.json()
    name = payload.get("agent") or payload.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Missing 'agent' or 'name'")

    labels = payload.get("labels") or {}
    capabilities = payload.get("capabilities") or {}
    now = _now()

    # Extract optional metrics from payload
    metrics_update: Dict[str, Any] = {}
    for key in AGENT_METRIC_KEYS:
        if key in payload:
            metrics_update[key] = payload.get(key)

    with STATE_LOCK:
        entry = AGENTS.get(name, {})
        # merge labels/capabilities
        entry.setdefault("labels", {}).update(labels)
        entry.setdefault("capabilities", {}).update(capabilities)
        entry["last_seen"] = now
        # merge metrics
        existing_metrics = entry.get("metrics") or {}
        existing_metrics.update(metrics_update)
        entry["metrics"] = existing_metrics

        # recompute health state
        state, reason = _compute_agent_state(entry, now)
        entry["state"] = state
        entry["health_reason"] = reason

        AGENTS[name] = entry

    return {"status": "ok", "agent": name, "time": now}


@app.get("/agents")
@app.get("/api/agents")
def list_agents() -> Dict[str, Any]:
    """
    Return agents in the shape the UI & your PS script expect, plus:
      - state
      - health_reason
      - metrics (if any)
    """
    with STATE_LOCK:
        now = _now()
        _refresh_agent_states(now)

        # shallow copy so we don't leak internal refs
        return {
            name: {
                "labels": dict(info.get("labels") or {}),
                "capabilities": dict(info.get("capabilities") or {}),
                "last_seen": info.get("last_seen"),
                "state": info.get("state", "unknown"),
                "health_reason": info.get("health_reason"),
                "metrics": dict(info.get("metrics") or {}),
            }
            for name, info in AGENTS.items()
        }


# -----------------------------------------------------------------------------
# Task leasing
# -----------------------------------------------------------------------------

@app.get("/task")
@app.get(
    "/api/task",
    summary="Agent polls for next task",
)
def get_task(agent: str, wait_ms: int = 0):
    """
    GET /task?agent=agent-1&wait_ms=1000

    Returns:
      200 + JSON task if available
      204 if none available before wait_ms

    NOTE: For now we do not block leasing based on health state.
    Awareness first, enforcement later.
    """
    deadline = _now() + (wait_ms / 1000.0)

    # Simple long-poll loop
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


def _enqueue_job(op: str, payload: Dict[str, Any]) -> str:
    """
    Enqueue a job and return its ID.
    """
    global JOBS, TASK_QUEUE

    job_id = str(uuid.uuid4())
    job = {
        "id": job_id,
        "op": op,
        "payload": payload,
        "created_ts": _now(),
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
            "id": job_id,
            "op": job["op"],
            "payload": job["payload"],
        }

    # no jobs
    return None


@app.post("/result")
@app.post("/api/result")
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

        # op-level stats
        op = job.get("op") or payload.get("op") or "unknown"
        OP_COUNTS[op] += 1
        if duration_ms is not None:
            OP_TOTAL_MS[op] += float(duration_ms)

    return {"status": "ok", "id": job_id}


# -----------------------------------------------------------------------------
# Jobs / results inspection
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
    """Timeline of jobs by created_ts."""
    with STATE_LOCK:
        jobs = list(JOBS.values())
        jobs.sort(key=lambda j: j.get("created_ts") or 0.0)
        return jobs[-limit:]


# -----------------------------------------------------------------------------
# Stats / metrics for UI
# -----------------------------------------------------------------------------

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


@app.get("/stats")
def stats() -> Dict[str, Any]:
    """
    Summary stats for the UI.

    Shape matches what your PowerShell output showed:
      time, agents_online, queue_len, leased_total,
      completed_total, failed_total, rate_1s, rate_60s, rate_5m, rate_15m,
      op_counts, op_avg_ms

    Plus:
      agent_states: counts by health state
    """
    now = _now()
    with STATE_LOCK:
        # refresh agent health states before reporting
        _refresh_agent_states(now)

        agents_online = len(AGENTS)
        queue_len = len(TASK_QUEUE)
        leased_total = LEASED_TOTAL
        completed_total = COMPLETED_TOTAL
        failed_total = FAILED_TOTAL

        _prune_completion_times(now)

        rate_1s = _calc_rate(1.0, now)
        rate_60s = _calc_rate(60.0, now)
        rate_5m = _calc_rate(300.0, now)
        rate_15m = _calc_rate(900.0, now)

        op_counts = dict(OP_COUNTS)
        op_avg_ms = {
            op: (OP_TOTAL_MS[op] / count) if count > 0 else 0.0
            for op, count in OP_COUNTS.items()
        }

        # summarize agent states
        agent_states: Dict[str, int] = defaultdict(int)
        for info in AGENTS.values():
            state = info.get("state") or "unknown"
            agent_states[state] += 1

    return {
        "time": now,
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
        "agent_states": dict(agent_states),
    }


@app.get("/stats/sparkline")
def stats_sparkline() -> Dict[str, Any]:
    """
    Tiny sparkline-friendly throughput series over the last 15 minutes.
    """
    now = _now()
    window_s = 900.0
    bucket_s = 10.0

    with STATE_LOCK:
        _prune_completion_times(now, window_s)
        times = list(COMPLETION_TIMES)

    if not times:
        return {"buckets": [], "values": []}

    start = now - window_s
    num_buckets = int(window_s / bucket_s)
    buckets = [start + i * bucket_s for i in range(num_buckets)]
    counts = [0] * num_buckets

    for t in times:
        idx = int((t - start) / bucket_s)
        if 0 <= idx < num_buckets:
            counts[idx] += 1

    return {
        "buckets": buckets,
        "values": counts,
    }


@app.get("/stats/ops")
def stats_ops() -> Dict[str, Any]:
    """
    Per-op stats for the UI: counts and average ms.
    """
    with STATE_LOCK:
        op_counts = dict(OP_COUNTS)
        op_avg_ms = {
            op: (OP_TOTAL_MS[op] / count) if count > 0 else 0.0
            for op, count in OP_COUNTS.items()
        }

    return {
        "op_counts": op_counts,
        "op_avg_ms": op_avg_ms,
    }


# -----------------------------------------------------------------------------
# Seeding / purge
# -----------------------------------------------------------------------------

@app.post("/seed")
@app.post("/api/seed")  # extra compatibility; nginx usually strips /api
async def seed(request: Request) -> Dict[str, Any]:
    """
    Seed some demo jobs, mostly for your brutal smoke tests.
    """
    payload = await request.json()
    count = int(payload.get("count", 10))
    op = payload.get("op", "map_tokenize")
    template = payload.get("payload_template") or {
        "text": "The quick brown fox jumps over the lazy dog."
    }

    job_ids = []
    with STATE_LOCK:
        for _ in range(count):
            # naive template expansion
            body = dict(template)
            body["ts"] = _now()
            job_ids.append(_enqueue_job(op, body))

    return {"status": "ok", "count": count, "op": op, "job_ids": job_ids}


@app.post("/purge")
def purge() -> Dict[str, Any]:
    """
    Blow away all jobs and reset counters. Useful when you want a clean run.
    """
    global JOBS, TASK_QUEUE, LEASED_TOTAL, COMPLETED_TOTAL, FAILED_TOTAL
    global COMPLETION_TIMES, OP_COUNTS, OP_TOTAL_MS

    with STATE_LOCK:
        JOBS = {}
        TASK_QUEUE = deque()
        LEASED_TOTAL = 0
        COMPLETED_TOTAL = 0
        FAILED_TOTAL = 0
        COMPLETION_TIMES = []
        OP_COUNTS = defaultdict(int)
        OP_TOTAL_MS = defaultdict(float)

    return {"status": "ok", "message": "purged"}
