import asyncio
import time
import uuid
import threading
from collections import deque, defaultdict
from typing import Any, Deque, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse, JSONResponse
from starlette.responses import Response

from pipelines.engine import run_text_pipeline
from pipelines.spec import IntakeRequest
from connectors.ingress_http import parse_intake_body

app = FastAPI(title="Distributed Swarm Controller")

# -----------------------------------------------------------------------------
# In-memory state
# -----------------------------------------------------------------------------

STATE_LOCK = threading.Lock()

AGENTS: Dict[str, Dict[str, Any]] = {}
JOBS: Dict[str, Dict[str, Any]] = {}
TASK_QUEUE: Deque[str] = deque()

# Pipeline runs (digestion layer) tracked in-memory for now
PIPELINES: Dict[str, Dict[str, Any]] = {}

LEASED_TOTAL = 0
COMPLETED_TOTAL = 0
FAILED_TOTAL = 0

COMPLETION_TIMES: List[float] = []
OP_COUNTS: Dict[str, int] = defaultdict(int)
OP_TOTAL_MS: Dict[str, float] = defaultdict(float)

# Health events ring buffer (state transitions, auto-quarantine, etc.)
HEALTH_EVENTS: List[Dict[str, Any]] = []
HEALTH_EVENTS_MAX = 500

# -----------------------------------------------------------------------------
# Agent health / autonomic thresholds (cluster-aware policy)
# -----------------------------------------------------------------------------

HEALTH_POLICY: Dict[str, Any] = {
    # Heartbeat / liveness rules
    "heartbeat": {
        "suspect_age_sec": 40.0,
        "dead_age_sec": 120.0,
    },
    # Behavior changes depending on cluster capacity
    "modes": {
        # Plenty of healthy workers → be aggressive about cutting bad nodes
        "aggressive": {
            "error_rate_suspect": 0.05,          # 5% errors → suspect
            "error_rate_quarantine": 0.15,       # 15% errors → quarantine
            "max_timeouts_quarantine": 5,        # timeouts → quarantine
        },
        # Few workers → tolerate more garbage to keep cluster usable
        "conservative": {
            "error_rate_suspect": 0.10,          # 10% errors → suspect
            "error_rate_quarantine": 0.25,       # 25% errors → quarantine
            "max_timeouts_quarantine": 8,
        },
    },
    # Need at least this many tasks before we trust error-rate based decisions
    "min_tasks_for_rate": 30,
    # Auto-ban rules (hard fail)
    "ban": {
        "max_timeouts": 50,                      # many timeouts → ban
        "max_error_rate": 0.40,                  # 40%+ error rate with enough volume → ban
        "min_tasks": 200,
    },
    # Auto-recovery rules (out of suspect/quarantined back to healthy)
    "recovery": {
        "cooldown_sec": 60.0,                    # no failures for 60s
        "max_error_rate": 0.02,                  # ≤2% error
    },
    # Latency thresholds
    "latency_ms": {
        "degraded": 500.0,                       # ≥500ms avg latency → degraded/suspect
    },
}

# Metrics we accept from agents in heartbeat/register payloads
AGENT_METRIC_KEYS = [
    "cpu_util",        # float 0..1
    "ram_mb",          # float or int
    "tasks_completed", # cumulative from agent
    "tasks_failed",    # cumulative from agent
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


def _emit_health_event(
    name: str,
    old_state: Optional[str],
    new_state: str,
    old_reason: Optional[str],
    new_reason: Optional[str],
    ts: Optional[float] = None,
) -> None:
    """
    Record a health state transition for an agent.
    Stored in a simple ring buffer HEALTH_EVENTS.
    """
    global HEALTH_EVENTS

    if ts is None:
        ts = _now()

    # Ignore non-transitions
    if old_state == new_state:
        return

    event = {
        "ts": ts,
        "agent": name,
        "old_state": old_state,
        "new_state": new_state,
        "old_reason": old_reason,
        "new_reason": new_reason,
    }
    HEALTH_EVENTS.append(event)

    # Ring buffer trim
    if len(HEALTH_EVENTS) > HEALTH_EVENTS_MAX:
        overflow = len(HEALTH_EVENTS) - HEALTH_EVENTS_MAX
        del HEALTH_EVENTS[:overflow]


def _cluster_policy_mode() -> str:
    """
    Decide whether we're in 'aggressive' or 'conservative' mode
    based on overall cluster capacity and health.

    Uses worker_profile.workers.max_total_workers if present; otherwise assumes 1 per agent.
    """
    total_workers = 0
    healthy_workers = 0
    healthy_agents = 0

    for info in AGENTS.values():
        wp = info.get("worker_profile") or {}
        workers = wp.get("workers") or {}
        try:
            max_workers = int(workers.get("max_total_workers", 1) or 1)
        except (TypeError, ValueError):
            max_workers = 1

        total_workers += max_workers

        state = info.get("state") or "unknown"
        if state == "healthy":
            healthy_workers += max_workers
            healthy_agents += 1

    if total_workers == 0:
        return "conservative"

    healthy_ratio = healthy_workers / float(total_workers)
    if healthy_ratio >= 0.7 and healthy_agents >= 5:
        return "aggressive"
    return "conservative"


def _cluster_health_summary() -> Dict[str, Any]:
    """
    Aggregate view of cluster health for routing decisions.
    Assumes STATE_LOCK is held when called.
    """
    total_workers = 0
    healthy_workers = 0
    degraded_workers = 0
    healthy_agents = 0
    degraded_agents = 0

    for info in AGENTS.values():
        wp = info.get("worker_profile") or {}
        workers = wp.get("workers") or {}
        try:
            max_workers = int(workers.get("max_total_workers", 1) or 1)
        except (TypeError, ValueError):
            max_workers = 1

        total_workers += max_workers
        state = info.get("state") or "unknown"

        if state == "healthy":
            healthy_workers += max_workers
            healthy_agents += 1
        elif state == "degraded":
            degraded_workers += max_workers
            degraded_agents += 1

    return {
        "total_workers": total_workers,
        "healthy_workers": healthy_workers,
        "degraded_workers": degraded_workers,
        "healthy_agents": healthy_agents,
        "degraded_agents": degraded_agents,
    }


def _compute_agent_state(info: Dict[str, Any], now: Optional[float] = None) -> (str, str):
    """
    Compute a health state for an agent based on:
      - manual_state overrides (quarantined / banned)
      - last_seen / heartbeat age
      - controller-observed metrics (ctrl_*)
      - cluster-wide policy mode (aggressive / conservative)

    Returns:
      (state, reason)
      state ∈ {"healthy", "degraded", "quarantined", "banned", "dead", "unknown"}
    """
    if now is None:
        now = _now()

    policy = HEALTH_POLICY

    # Manual override wins, always.
    manual_state = info.get("manual_state")
    manual_reason = info.get("manual_reason") or "manual_override"
    if manual_state == "quarantined":
        return "quarantined", manual_reason
    if manual_state == "banned":
        return "banned", manual_reason

    last_seen = info.get("last_seen")
    if last_seen is None:
        return "unknown", "no_heartbeat_yet"

    try:
        last_seen_f = float(last_seen)
    except (TypeError, ValueError):
        last_seen_f = now

    hb_conf = policy["heartbeat"]
    dead_age = float(hb_conf["dead_age_sec"])
    suspect_age = float(hb_conf["suspect_age_sec"])

    # Dead: missed heartbeat for too long
    age = now - last_seen_f
    if age > dead_age:
        return "dead", "missed_heartbeat"

    metrics = info.get("metrics") or {}

    # Prefer controller-maintained metrics over agent-reported ones
    ctrl_completed = int(metrics.get("ctrl_tasks_completed", 0) or 0)
    ctrl_failed = int(metrics.get("ctrl_tasks_failed", 0) or 0)
    ctrl_timeouts = int(metrics.get("ctrl_lease_timeouts", 0) or 0)
    ctrl_avg_latency_ms = metrics.get("ctrl_avg_latency_ms")

    agent_completed = int(metrics.get("tasks_completed", 0) or 0)
    agent_failed = int(metrics.get("tasks_failed", 0) or 0)
    agent_latency_ms = metrics.get("latency_ms")

    # Effective counts
    if ctrl_completed or ctrl_failed:
        tasks_completed = ctrl_completed
        tasks_failed = ctrl_failed
    else:
        tasks_completed = agent_completed
        tasks_failed = agent_failed

    total_tasks = tasks_completed + tasks_failed

    # Choose latency source
    latency_ms = ctrl_avg_latency_ms if ctrl_avg_latency_ms is not None else agent_latency_ms

    # Baseline state
    old_state = info.get("state") or "unknown"
    state = "healthy"
    reason = "normal"

    error_rate = 0.0
    if total_tasks > 0:
        error_rate = float(tasks_failed) / float(total_tasks)

    # Heartbeat-based "suspect": too quiet, but not dead
    if age > suspect_age and old_state == "healthy":
        state = "degraded"
        reason = f"stale_heartbeat_{age:.1f}s"

    # Cluster-aware mode
    mode = _cluster_policy_mode()
    mode_cfg = policy["modes"][mode]
    min_tasks = int(policy["min_tasks_for_rate"])

    # --- Auto-ban rules ------------------------------------------------------
    if total_tasks >= min_tasks:
        ban_cfg = policy["ban"]
        ban_min_tasks = int(ban_cfg["min_tasks"])
        ban_max_err = float(ban_cfg["max_error_rate"])
        ban_max_timeouts = int(ban_cfg["max_timeouts"])

        if total_tasks >= ban_min_tasks and error_rate >= ban_max_err:
            return "banned", f"auto_ban_error_rate_{error_rate:.2f}"

        if ctrl_timeouts >= ban_max_timeouts:
            return "banned", f"auto_ban_timeouts_{ctrl_timeouts}"

    # --- Quarantine / suspect / degraded rules ------------------------------
    if total_tasks >= min_tasks:
        err_quar = float(mode_cfg["error_rate_quarantine"])
        err_sus = float(mode_cfg["error_rate_suspect"])
        max_timeouts_quar = int(mode_cfg["max_timeouts_quarantine"])

        # Auto-quarantine: bad error rate or many timeouts
        if error_rate >= err_quar or ctrl_timeouts >= max_timeouts_quar:
            state = "quarantined"
            reason = f"auto_quarantine_err={error_rate:.2f}_timeouts={ctrl_timeouts}"
        # Suspect/degraded: elevated error rate
        elif error_rate >= err_sus:
            state = "degraded"
            reason = f"elevated_error_rate_{error_rate:.2f}"

    # --- Latency-based degraded ---------------------------------------------
    lat_conf = policy["latency_ms"]
    degraded_latency = float(lat_conf["degraded"])
    if latency_ms is not None and float(latency_ms) >= degraded_latency:
        if state == "healthy":
            state = "degraded"
            reason = f"high_latency_{float(latency_ms):.1f}ms"
        else:
            reason = f"{reason}_high_latency_{float(latency_ms):.1f}ms"

    # --- Auto-recovery from degraded/quarantined ----------------------------
    # Only for non-manual states (manual handled at top).
    rec_cfg = policy["recovery"]
    rec_cooldown = float(rec_cfg["cooldown_sec"])
    rec_max_err = float(rec_cfg["max_error_rate"])

    last_fail_ts = metrics.get("ctrl_last_failure_ts")
    last_fail_age = None
    if last_fail_ts is not None:
        try:
            last_fail_age = now - float(last_fail_ts)
        except (TypeError, ValueError):
            last_fail_age = None

    if old_state in ("degraded", "quarantined"):
        # If we've cooled down and error rate is low, let the agent redeem itself.
        if error_rate <= rec_max_err and (last_fail_age is None or last_fail_age >= rec_cooldown):
            state = "healthy"
            reason = "recovered"

    return state, reason


def _set_agent_health(name: str, entry: Dict[str, Any], state: str, reason: str, now: Optional[float] = None) -> None:
    """
    Helper to set agent state + reason and emit health events on transitions.
    NOTE: Must be called with STATE_LOCK held.
    """
    if now is None:
        now = _now()

    old_state = entry.get("state")
    old_reason = entry.get("health_reason")

    entry["state"] = state
    entry["health_reason"] = reason
    AGENTS[name] = entry

    _emit_health_event(name, old_state, state, old_reason, reason, ts=now)


def _refresh_agent_states(now: Optional[float] = None) -> None:
    """
    Recompute health state for all agents using the current cluster policy mode.
    NOTE: Must be called with STATE_LOCK held.
    """
    if now is None:
        now = _now()

    for name, info in AGENTS.items():
        state, reason = _compute_agent_state(info, now)
        _set_agent_health(name, info, state, reason, now)


# -----------------------------------------------------------------------------
# Lease reaper (brainstem Option A)
# -----------------------------------------------------------------------------

async def _lease_reaper_loop(interval_s: float = 1.0) -> None:
    """
    Periodically scan for leased jobs whose lease_timeout_s has expired and
    return them to the TASK_QUEUE as queued.

    Also increments per-agent ctrl_lease_timeouts, which feeds health state.
    """
    while True:
        now = _now()

        with STATE_LOCK:
            for job_id, job in list(JOBS.items()):
                if job.get("status") != "leased":
                    continue

                leased_ts = job.get("leased_ts")
                timeout_s = job.get("lease_timeout_s") or 0
                if leased_ts is None or timeout_s <= 0:
                    continue

                try:
                    leased_ts_f = float(leased_ts)
                    timeout_f = float(timeout_s)
                except (TypeError, ValueError):
                    continue

                if now - leased_ts_f > timeout_f:
                    # Record a lease timeout against the agent, if known
                    agent_name = job.get("leased_by")
                    if agent_name and agent_name in AGENTS:
                        entry = AGENTS[agent_name]
                        metrics = entry.get("metrics") or {}
                        timeouts = int(metrics.get("ctrl_lease_timeouts", 0) or 0)
                        metrics["ctrl_lease_timeouts"] = timeouts + 1
                        entry["metrics"] = metrics

                        state, reason = _compute_agent_state(entry, now)
                        _set_agent_health(agent_name, entry, state, reason, now)

                    # Lease expired → return to queue
                    job["status"] = "queued"
                    job["leased_ts"] = None
                    job["leased_by"] = None
                    TASK_QUEUE.append(job_id)

        await asyncio.sleep(interval_s)


@app.on_event("startup")
async def controller_startup() -> None:
    """
    Startup hook: launches the lease reaper in the background.
    """
    asyncio.create_task(_lease_reaper_loop())


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
        active_agents = sum(
            1 for info in AGENTS.values()
            if info.get("state") != "dead"
        )

    return PlainTextResponse(
        f"ok queue={queue_len} agents={agents_online} active_agents={active_agents}",
        media_type="text/plain",
    )


# -----------------------------------------------------------------------------
# Job submission (new)
# -----------------------------------------------------------------------------

@app.post("/job")
@app.post("/api/job")
async def submit_job(request: Request) -> Dict[str, Any]:
    """
    Submit one or more jobs.

    Single:
      {"op": "...", "payload": {...}}

    Batch:
      [{"op": "...", "payload": {...}}, ...]
    """
    body = await request.json()

    def _one(item: Dict[str, Any]) -> str:
        op = item.get("op")
        payload = item.get("payload")
        if not op or payload is None:
            raise HTTPException(status_code=400, detail="Each job requires {op, payload}")
        with STATE_LOCK:
            return _enqueue_job(op, payload)

    if isinstance(body, list):
        ids = [_one(it) for it in body]
        return {"status": "ok", "job_ids": ids}

    job_id = _one(body)
    return {"status": "ok", "job_ids": [job_id]}


# -----------------------------------------------------------------------------
# Agents
# -----------------------------------------------------------------------------

@app.post("/agents/register")
@app.post("/api/agents/register")
async def register_agent(request: Request) -> Dict[str, Any]:
    payload = await request.json()
    name = payload.get("agent") or payload.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Missing 'agent' or 'name'")

    labels = payload.get("labels") or {}
    capabilities = payload.get("capabilities") or {}

    # Support worker_profile either at top-level or nested under labels
    raw_worker_profile = payload.get("worker_profile")
    if raw_worker_profile is None:
        raw_worker_profile = labels.get("worker_profile")
    worker_profile = raw_worker_profile or {}

    now = _now()

    # Extract optional metrics from payload.
    metrics: Dict[str, Any] = {}
    nested_metrics = payload.get("metrics") or {}
    for key in AGENT_METRIC_KEYS:
        if key in payload:
            metrics[key] = payload.get(key)
        elif key in nested_metrics:
            metrics[key] = nested_metrics.get(key)

    with STATE_LOCK:
        info: Dict[str, Any] = {
            "labels": labels,
            "capabilities": capabilities,
            "worker_profile": worker_profile,
            "last_seen": now,
            "metrics": metrics,
        }
        state, reason = _compute_agent_state(info, now)
        _set_agent_health(name, info, state, reason, now)

    return {"status": "ok", "agent": name, "time": now}


@app.post("/agents/heartbeat")
@app.post("/api/agents/heartbeat")
async def agent_heartbeat(request: Request) -> Dict[str, Any]:
    payload = await request.json()
    name = payload.get("agent") or payload.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Missing 'agent' or 'name'")

    labels = payload.get("labels") or {}
    capabilities = payload.get("capabilities") or {}

    raw_worker_profile = payload.get("worker_profile")
    if raw_worker_profile is None:
        raw_worker_profile = labels.get("worker_profile")
    worker_profile = raw_worker_profile or {}

    now = _now()

    metrics_update: Dict[str, Any] = {}
    nested_metrics = payload.get("metrics") or {}
    for key in AGENT_METRIC_KEYS:
        if key in payload:
            metrics_update[key] = payload.get(key)
        elif key in nested_metrics:
            metrics_update[key] = nested_metrics.get(key)

    with STATE_LOCK:
        entry = AGENTS.get(name, {})
        entry.setdefault("labels", {}).update(labels)
        entry.setdefault("capabilities", {}).update(capabilities)
        entry["last_seen"] = now

        existing_metrics = entry.get("metrics") or {}
        existing_metrics.update(metrics_update)
        entry["metrics"] = existing_metrics

        if worker_profile:
            entry["worker_profile"] = worker_profile
        else:
            entry.setdefault("worker_profile", {})

        state, reason = _compute_agent_state(entry, now)
        _set_agent_health(name, entry, state, reason, now)

    return {"status": "ok", "agent": name, "time": now}


@app.get("/agents")
@app.get("/api/agents")
def list_agents() -> Dict[str, Any]:
    with STATE_LOCK:
        now = _now()
        _refresh_agent_states(now)

        return {
            name: {
                "labels": dict(info.get("labels") or {}),
                "capabilities": dict(info.get("capabilities") or {}),
                "last_seen": info.get("last_seen"),
                "state": info.get("state", "unknown"),
                "health_reason": info.get("health_reason"),
                "metrics": dict(info.get("metrics") or {}),
                "worker_profile": dict(info.get("worker_profile") or {}),
            }
            for name, info in AGENTS.items()
        }


@app.post("/agents/{name}/quarantine")
@app.post("/api/agents/{name}/quarantine")
async def quarantine_agent(name: str, request: Request) -> Dict[str, Any]:
    reason_default = "manual_quarantine"

    try:
        body = await request.json()
    except Exception:
        body = {}

    manual_reason = body.get("reason", reason_default)
    now = _now()

    with STATE_LOCK:
        entry = AGENTS.get(name)
        if not entry:
            raise HTTPException(status_code=404, detail="Unknown agent")

        entry["manual_state"] = "quarantined"
        entry["manual_reason"] = manual_reason

        state, reason = _compute_agent_state(entry, now)
        _set_agent_health(name, entry, state, reason, now)

    return {"status": "ok", "agent": name, "state": state, "reason": reason}


@app.post("/agents/{name}/restore")
@app.post("/api/agents/{name}/restore")
async def restore_agent(name: str) -> Dict[str, Any]:
    now = _now()

    with STATE_LOCK:
        entry = AGENTS.get(name)
        if not entry:
            raise HTTPException(status_code=404, detail="Unknown agent")

        entry.pop("manual_state", None)
        entry.pop("manual_reason", None)

        state, reason = _compute_agent_state(entry, now)
        _set_agent_health(name, entry, state, reason, now)

    return {"status": "ok", "agent": name, "state": state, "reason": reason}


@app.post("/agents/{name}/ban")
@app.post("/api/agents/{name}/ban")
async def ban_agent(name: str, request: Request) -> Dict[str, Any]:
    reason_default = "manual_ban"

    try:
        body = await request.json()
    except Exception:
        body = {}

    manual_reason = body.get("reason", reason_default)
    now = _now()

    with STATE_LOCK:
        entry = AGENTS.get(name)
        if not entry:
            raise HTTPException(status_code=404, detail="Unknown agent")

        entry["manual_state"] = "banned"
        entry["manual_reason"] = manual_reason

        state, reason = _compute_agent_state(entry, now)
        _set_agent_health(name, entry, state, reason, now)

    return {"status": "ok", "agent": name, "state": state, "reason": reason}


# -----------------------------------------------------------------------------
# Health events endpoints
# -----------------------------------------------------------------------------

@app.get("/events")
@app.get("/api/events")
def list_events(limit: int = 200) -> Dict[str, Any]:
    with STATE_LOCK:
        if limit <= 0:
            data = []
        else:
            data = HEALTH_EVENTS[-limit:]
    return {"events": data}


@app.post("/events/clear")
@app.post("/api/events/clear")
def clear_events() -> Dict[str, Any]:
    global HEALTH_EVENTS
    with STATE_LOCK:
        HEALTH_EVENTS = []
    return {"status": "ok", "cleared": True}


# -----------------------------------------------------------------------------
# Task leasing
# -----------------------------------------------------------------------------

@app.get("/task")
@app.get("/api/task", summary="Agent polls for next task")
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
            return Response(status_code=204)

        time.sleep(0.05)


def _enqueue_job(op: str, payload: Dict[str, Any]) -> str:
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
    """
    Pop the next job from the queue and mark it leased to `agent`.

    IMPORTANT COMPAT:
      - Returns BOTH {"id": "..."} and {"job_id": "..."} with same value.
        Older agents use job_id; newer agents use id.
    """
    global LEASED_TOTAL

    now = _now()
    agent_info = AGENTS.get(agent) or {}
    state = agent_info.get("state") or "unknown"

    if state in ("dead", "quarantined", "banned"):
        return None

    mode = _cluster_policy_mode()
    if state == "degraded" and mode == "aggressive":
        summary = _cluster_health_summary()
        if summary.get("healthy_workers", 0) > 0:
            return None

    queue_len = len(TASK_QUEUE)
    for _ in range(queue_len):
        if not TASK_QUEUE:
            return None

        job_id = TASK_QUEUE.popleft()
        job = JOBS.get(job_id)
        if not job:
            continue

        if job["status"] not in ("queued", "leased"):
            continue

        payload = job.get("payload") or {}
        prefer_gpu = bool(payload.get("prefer_gpu", False))
        min_vram_gb = payload.get("min_vram_gb")

        if prefer_gpu:
            wp = agent_info.get("worker_profile") or {}
            gpu = wp.get("gpu") or {}
            gpu_present = bool(gpu.get("gpu_present", False))
            if not gpu_present:
                TASK_QUEUE.append(job_id)
                continue

            if min_vram_gb is not None:
                try:
                    vram = float(gpu.get("vram_gb", 0) or 0)
                    min_v = float(min_vram_gb)
                except (TypeError, ValueError):
                    vram = 0.0
                    min_v = 0.0
                if vram < min_v:
                    TASK_QUEUE.append(job_id)
                    continue

        job["status"] = "leased"
        job["leased_by"] = agent
        job["leased_ts"] = now
        LEASED_TOTAL += 1

        # Return BOTH keys for compatibility
        return {
            "id": job_id,
            "job_id": job_id,
            "op": job["op"],
            "payload": job["payload"],
        }

    return None


# -----------------------------------------------------------------------------
# Results
# -----------------------------------------------------------------------------

@app.post("/result")
@app.post("/api/result")
async def post_result(request: Request) -> Dict[str, Any]:
    """
    Agent posts result (compat mode):

      {
        "id": "<job_id>" OR "job_id": "<job_id>",
        "agent": "agent-1",
        "ok": true/false,
        "result": {...} | null,
        "error": "..." | null,
        "op": "map_tokenize" | null,
        "duration_ms": float | null
      }
    """
    global COMPLETED_TOTAL, FAILED_TOTAL

    payload = await request.json()

    # Accept both id and job_id
    job_id = payload.get("id") or payload.get("job_id")
    if not job_id:
        raise HTTPException(status_code=400, detail="Missing id/job_id")

    now = _now()

    with STATE_LOCK:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Unknown job_id")

        if job.get("status") in ("completed", "failed"):
            return {"status": "ok", "job_id": job_id, "already_done": True}

        err = payload.get("error")
        ok_flag = payload.get("ok")
        if ok_flag is False and not err:
            err = "ok=false"

        result = payload.get("result")

        job["completed_ts"] = now
        duration_ms = payload.get("duration_ms")
        if duration_ms is None and job.get("leased_ts") is not None:
            duration_ms = (now - job["leased_ts"]) * 1000.0
        job["duration_ms"] = duration_ms

        op = job.get("op") or payload.get("op") or "unknown"
        if duration_ms is not None:
            OP_COUNTS[op] += 1
            OP_TOTAL_MS[op] += float(duration_ms)

        # Update controller-maintained per-agent metrics (based on lease owner)
        agent_name = job.get("leased_by") or payload.get("agent")
        if agent_name and agent_name in AGENTS:
            entry = AGENTS[agent_name]
            metrics = entry.get("metrics") or {}

            if err:
                metrics["ctrl_tasks_failed"] = int(metrics.get("ctrl_tasks_failed", 0) or 0) + 1
                metrics["ctrl_last_failure_ts"] = now
            else:
                metrics["ctrl_tasks_completed"] = int(metrics.get("ctrl_tasks_completed", 0) or 0) + 1

            # Update controller avg latency (simple EMA-ish)
            if duration_ms is not None:
                prev = metrics.get("ctrl_avg_latency_ms")
                try:
                    d = float(duration_ms)
                except (TypeError, ValueError):
                    d = None
                if d is not None:
                    if prev is None:
                        metrics["ctrl_avg_latency_ms"] = d
                    else:
                        try:
                            p = float(prev)
                        except (TypeError, ValueError):
                            p = d
                        metrics["ctrl_avg_latency_ms"] = (p * 0.9) + (d * 0.1)

            entry["metrics"] = metrics

            state, reason = _compute_agent_state(entry, now)
            _set_agent_health(agent_name, entry, state, reason, now)

        if err:
            job["status"] = "failed"
            job["error"] = err
            FAILED_TOTAL += 1
        else:
            job["status"] = "completed"
            job["result"] = result
            COMPLETED_TOTAL += 1
            COMPLETION_TIMES.append(now)

    return {"status": "ok", "job_id": job_id}


# -----------------------------------------------------------------------------
# Jobs / reporting
# -----------------------------------------------------------------------------

@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> Dict[str, Any]:
    with STATE_LOCK:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Unknown job")
        return dict(job)


@app.get("/results")
def list_results(limit: int = 50) -> Dict[str, Any]:
    with STATE_LOCK:
        jobs = list(JOBS.values())
        jobs.sort(key=lambda j: j.get("created_ts", 0), reverse=True)
        data = []
        for j in jobs:
            if j.get("status") in ("completed", "failed"):
                data.append(j)
            if len(data) >= limit:
                break
    return {"results": data}


@app.get("/timeline")
def timeline(limit: int = 200) -> Dict[str, Any]:
    with STATE_LOCK:
        jobs = list(JOBS.values())
        jobs.sort(key=lambda j: j.get("created_ts", 0))
        if limit > 0:
            jobs = jobs[-limit:]
        return {"jobs": jobs}


@app.get("/stats")
def stats() -> Dict[str, Any]:
    now = _now()
    window_s = 60.0
    window_5m = 300.0
    window_15m = 900.0

    with STATE_LOCK:
        agents_online = len(AGENTS)
        queue_len = len(TASK_QUEUE)
        leased_total = LEASED_TOTAL
        completed_total = COMPLETED_TOTAL
        failed_total = FAILED_TOTAL

        _prune_completion_times(now, window_15m)
        times = list(COMPLETION_TIMES)

        rate_1s = sum(1 for t in times if now - t <= 1.0)
        rate_60s = sum(1 for t in times if now - t <= window_s) / window_s if window_s > 0 else 0
        rate_5m = sum(1 for t in times if now - t <= window_5m) / window_5m if window_5m > 0 else 0
        rate_15m = sum(1 for t in times if now - t <= window_15m) / window_15m if window_15m > 0 else 0

        agent_states = defaultdict(int)
        gpu_agents_online = 0
        total_gpu_vram_gb = 0.0

        for info in AGENTS.values():
            st = info.get("state") or "unknown"
            agent_states[st] += 1
            wp = info.get("worker_profile") or {}
            gpu = wp.get("gpu") or {}
            if bool(gpu.get("gpu_present", False)):
                gpu_agents_online += 1
                try:
                    vram = float(gpu.get("vram_gb", 0) or 0)
                except (TypeError, ValueError):
                    vram = 0.0
                total_gpu_vram_gb += vram

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
        "op_counts": dict(OP_COUNTS),
        "op_avg_ms": {
            op: (OP_TOTAL_MS[op] / count) if count > 0 else 0.0
            for op, count in OP_COUNTS.items()
        },
        "agent_states": dict(agent_states),
        "gpu_agents_online": gpu_agents_online,
        "total_gpu_vram_gb": total_gpu_vram_gb,
    }


@app.get("/stats/sparkline")
def stats_sparkline() -> Dict[str, Any]:
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

    return {"buckets": buckets, "values": counts}


@app.get("/stats/ops")
def stats_ops() -> Dict[str, Any]:
    with STATE_LOCK:
        op_counts = dict(OP_COUNTS)
        op_avg_ms = {
            op: (OP_TOTAL_MS[op] / count) if count > 0 else 0.0
            for op, count in OP_COUNTS.items()
        }

    return {"op_counts": op_counts, "op_avg_ms": op_avg_ms}


# -----------------------------------------------------------------------------
# Intake / pipelines (digestion slice v1)
# -----------------------------------------------------------------------------

async def _run_pipeline_run(run_id: str, req: IntakeRequest) -> None:
    now = _now()
    with STATE_LOCK:
        pr = PIPELINES.get(run_id)
        if not pr:
            return
        pr["status"] = "running"
        pr["updated_ts"] = now
        pr["detail"] = "running"

    def enqueue(op: str, payload: Dict[str, Any]) -> str:
        with STATE_LOCK:
            return _enqueue_job(op, payload)

    def get_job_fn(job_id: str) -> Optional[Dict[str, Any]]:
        with STATE_LOCK:
            return JOBS.get(job_id)

    try:
        final = await asyncio.to_thread(
            run_text_pipeline,
            text=req.text,
            chunk_chars=req.chunk_chars,
            overlap_chars=req.overlap_chars,
            do_summarize=req.do_summarize,
            do_classify=req.do_classify,
            labels=req.labels,
            enqueue_job_fn=enqueue,
            get_job_fn=get_job_fn,
            timeout_s=900.0,
        )
        with STATE_LOCK:
            pr = PIPELINES.get(run_id)
            if pr:
                pr["status"] = "completed"
                pr["updated_ts"] = _now()
                pr["detail"] = "completed"
                pr["final_result"] = final
    except Exception as e:
        with STATE_LOCK:
            pr = PIPELINES.get(run_id)
            if pr:
                pr["status"] = "failed"
                pr["updated_ts"] = _now()
                pr["detail"] = f"failed: {type(e).__name__}: {e}"


@app.post("/api/intake")
@app.post("/api/ingest")
async def intake(request: Request) -> Dict[str, Any]:
    body = await request.json()
    req = parse_intake_body(body)

    if req.content_type != "text/plain":
        raise HTTPException(status_code=400, detail="v1 intake supports only content_type=text/plain")

    run_id = str(uuid.uuid4())
    now = _now()
    with STATE_LOCK:
        PIPELINES[run_id] = {
            "run_id": run_id,
            "status": "queued",
            "created_ts": now,
            "updated_ts": now,
            "detail": "queued",
            "request": {
                "content_type": req.content_type,
                "chunk_chars": req.chunk_chars,
                "overlap_chars": req.overlap_chars,
                "do_summarize": req.do_summarize,
                "do_classify": req.do_classify,
                "labels": req.labels,
            },
            "final_result": None,
        }

    asyncio.create_task(_run_pipeline_run(run_id, req))
    return {"status": "ok", "run_id": run_id}


@app.get("/api/pipelines/{run_id}")
def get_pipeline(run_id: str) -> Dict[str, Any]:
    with STATE_LOCK:
        pr = PIPELINES.get(run_id)
        if not pr:
            raise HTTPException(status_code=404, detail="Unknown run_id")
        return dict(pr)


# -----------------------------------------------------------------------------
# Seeding / purge
# -----------------------------------------------------------------------------

@app.post("/seed")
@app.post("/api/seed")
async def seed(request: Request) -> Dict[str, Any]:
    payload = await request.json()
    count = int(payload.get("count", 10))
    op = payload.get("op", "map_tokenize")
    template = payload.get("payload_template") or {
        "text": "The quick brown fox jumps over the lazy dog."
    }

    job_ids = []
    with STATE_LOCK:
        for _ in range(count):
            body = dict(template)
            body["ts"] = _now()
            job_ids.append(_enqueue_job(op, body))

    return {"status": "ok", "count": count, "op": op, "job_ids": job_ids}


@app.post("/purge")
def purge() -> Dict[str, Any]:
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
