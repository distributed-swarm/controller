import asyncio
import time
import uuid
import threading
from collections import deque, defaultdict
from typing import Any, Deque, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import PlainTextResponse, JSONResponse
from starlette.responses import Response

from pipelines.engine import run_text_pipeline
from pipelines.spec import IntakeRequest
from connectors.ingress_http import parse_intake_body
from brainstem import Brainstem

app = FastAPI(title="Distributed Swarm Controller")

# -----------------------------------------------------------------------------
# Brainstem (stable scheduling policy)
# -----------------------------------------------------------------------------
BRAIN = Brainstem()
ENABLE_LATERAL_INHIBITION = True  # flip False for instant rollback

# -----------------------------------------------------------------------------
# In-memory state
# -----------------------------------------------------------------------------

STATE_LOCK = threading.Lock()

AGENTS: Dict[str, Dict[str, Any]] = {}
JOBS: Dict[str, Dict[str, Any]] = {}

# NEW: Multiple priority queues by excitatory level (0..3)
# 3 = Reflex (Immediate SAP Trigger)
# 2 = High Priority (Human Approved)
# 1 = Standard (Default / OCR)
# 0 = Background (Cleanup)
TASK_QUEUES: Dict[int, Deque[str]] = {0: deque(), 1: deque(), 2: deque(), 3: deque()}

# ALIAS: Back-compat for legacy code (defaults to Excitatory Level 1)
# NOTE: This alias points to the specific deque object at TASK_QUEUES[1].
# Do not reassign TASK_QUEUES entirely, or this alias will point to a dead object.
TASK_QUEUE: Deque[str] = TASK_QUEUES[1]

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
# Warmup / “stretch” (prevents instant auto-quarantine on wake)
# -----------------------------------------------------------------------------

WARMUP_POLICY: Dict[str, Any] = {
    # After register (or after a long heartbeat gap), ignore error-rate/timeouts for this long
    "warmup_sec": 20.0,
    # If heartbeat gap exceeds this, we treat it like a restart and re-warm
    "restart_gap_sec": 45.0,
}

# -----------------------------------------------------------------------------
# Risk scoring (cumulative exposure) controller-side projection + config
# -----------------------------------------------------------------------------

RISK_CONFIG: Dict[str, Any] = {
    "tau_sec": 3600.0,
    "weights": {"food": 1.0, "air": 0.3, "contact": 0.6},
    "thresholds": {"warn": 10.0, "act": 20.0},
    "max_entities": 20000,
}

RISK_STATE: Dict[str, Dict[str, Any]] = {}

# -----------------------------------------------------------------------------
# Agent health / autonomic thresholds (cluster-aware policy)
# -----------------------------------------------------------------------------

HEALTH_POLICY: Dict[str, Any] = {
    "heartbeat": {
        "suspect_age_sec": 40.0,
        "dead_age_sec": 120.0,
    },

    # NEW: sustained-failure quarantine policy (rolling window)
    # Quarantine is triggered ONLY by sustained recent failures, not lifetime totals.
    "window": {
        "size": 50,                 # how many recent outcomes to keep per agent
        "min_samples": 20,          # don't degrade/quarantine until we have at least this many recent samples
        "degraded_fail_rate": 0.25, # recent fail rate to mark degraded
        "quarantine_fail_rate": 0.50,  # recent fail rate to quarantine
    },

    "modes": {
        "aggressive": {
            "error_rate_suspect": 0.05,
            "error_rate_quarantine": 0.15,
            "max_timeouts_quarantine": 5,
        },
        "conservative": {
            "error_rate_suspect": 0.10,
            "error_rate_quarantine": 0.25,
            "max_timeouts_quarantine": 8,
        },
    },
    "min_tasks_for_rate": 30,
    "ban": {
        "max_timeouts": 50,
        "max_error_rate": 0.40,
        "min_tasks": 200,
    },
    "recovery": {
        "cooldown_sec": 60.0,
        "max_error_rate": 0.02,
    },
    "latency_ms": {
        "degraded": 500.0,
    },
}

AGENT_METRIC_KEYS = [
    "cpu_util",
    "ram_mb",
    "tasks_completed",
    "tasks_failed",
    "avg_task_ms",
    "latency_ms",
]

# In-memory per-agent rolling window of outcomes:
#   0 = success
#   1 = failure (including lease-timeout)
# Stored on AGENTS[name]["health_window"] as a deque[int].
HEALTH_WINDOW_KEY = "health_window"


def _now() -> float:
    return time.time()


def _prune_completion_times(now: Optional[float] = None, window_s: float = 900.0) -> None:
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
    global HEALTH_EVENTS
    if ts is None:
        ts = _now()

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

    if len(HEALTH_EVENTS) > HEALTH_EVENTS_MAX:
        overflow = len(HEALTH_EVENTS) - HEALTH_EVENTS_MAX
        del HEALTH_EVENTS[:overflow]


def _cluster_policy_mode() -> str:
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


def _in_warmup(info: Dict[str, Any], now: float) -> bool:
    until_ts = info.get("warmup_until_ts")
    if until_ts is None:
        return False
    try:
        return now < float(until_ts)
    except (TypeError, ValueError):
        return False


def _get_health_window(entry: Dict[str, Any]) -> Deque[int]:
    w = entry.get(HEALTH_WINDOW_KEY)
    if isinstance(w, deque):
        return w
    # initialize
    try:
        size = int((HEALTH_POLICY.get("window") or {}).get("size", 50) or 50)
    except (TypeError, ValueError):
        size = 50
    w = deque(maxlen=max(1, size))
    entry[HEALTH_WINDOW_KEY] = w
    return w


def _record_outcome(entry: Dict[str, Any], failed: bool) -> None:
    """
    Record a recent outcome for sustained-failure policy.
    failed=True covers task failure AND lease-timeout.
    """
    w = _get_health_window(entry)
    w.append(1 if failed else 0)
    entry[HEALTH_WINDOW_KEY] = w


def _recent_fail_rate(entry: Dict[str, Any]) -> Tuple[int, float]:
    """
    Returns (n_samples, fail_rate) for the rolling window.
    """
    w = entry.get(HEALTH_WINDOW_KEY)
    if not isinstance(w, deque) or len(w) == 0:
        return 0, 0.0
    n = len(w)
    fails = sum(1 for x in w if x == 1)
    return n, (fails / float(n)) if n > 0 else 0.0


def _compute_agent_state(info: Dict[str, Any], now: Optional[float] = None) -> Tuple[str, str]:
    if now is None:
        now = _now()

    policy = HEALTH_POLICY

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

    age = now - last_seen_f
    if age > dead_age:
        return "dead", "missed_heartbeat"

    # Warmup: only heartbeat rules apply (prevents instant quarantine on wake)
    if _in_warmup(info, now):
        if age > suspect_age:
            return "stale", f"heartbeat_stale_{age:.1f}s"
        return "healthy", "warmup"

    metrics = info.get("metrics") or {}

    ctrl_completed = int(metrics.get("ctrl_tasks_completed", 0) or 0)
    ctrl_failed = int(metrics.get("ctrl_tasks_failed", 0) or 0)
    ctrl_timeouts = int(metrics.get("ctrl_lease_timeouts", 0) or 0)
    ctrl_avg_latency_ms = metrics.get("ctrl_avg_latency_ms")

    agent_completed = int(metrics.get("tasks_completed", 0) or 0)
    agent_failed = int(metrics.get("tasks_failed", 0) or 0)
    agent_latency_ms = metrics.get("latency_ms")

    if ctrl_completed or ctrl_failed:
        tasks_completed = ctrl_completed
        tasks_failed = ctrl_failed
    else:
        tasks_completed = agent_completed
        tasks_failed = agent_failed

    total_tasks = tasks_completed + tasks_failed

    latency_ms = ctrl_avg_latency_ms if ctrl_avg_latency_ms is not None else agent_latency_ms

    old_state = info.get("state") or "unknown"
    state = "healthy"
    reason = "normal"

    error_rate = 0.0
    if total_tasks > 0:
        error_rate = float(tasks_failed) / float(total_tasks)

    if age > suspect_age:
        return "stale", f"heartbeat_stale_{age:.1f}s"

    # -------------------------------------------------------------------------
    # NEW: sustained-failure policy (rolling window drives degraded/quarantine)
    # -------------------------------------------------------------------------
    win_cfg = policy.get("window") or {}
    try:
        win_min = int(win_cfg.get("min_samples", 20) or 20)
    except (TypeError, ValueError):
        win_min = 20
    try:
        win_degraded = float(win_cfg.get("degraded_fail_rate", 0.25) or 0.25)
    except (TypeError, ValueError):
        win_degraded = 0.25
    try:
        win_quarantine = float(win_cfg.get("quarantine_fail_rate", 0.50) or 0.50)
    except (TypeError, ValueError):
        win_quarantine = 0.50

    recent_n, recent_fail_rate = _recent_fail_rate(info)

    # Auto-ban still uses lifetime thresholds (big-hammer safety)
    min_tasks = int(policy["min_tasks_for_rate"])
    if total_tasks >= min_tasks:
        ban_cfg = policy["ban"]
        ban_min_tasks = int(ban_cfg["min_tasks"])
        ban_max_err = float(ban_cfg["max_error_rate"])
        ban_max_timeouts = int(ban_cfg["max_timeouts"])

        if total_tasks >= ban_min_tasks and error_rate >= ban_max_err:
            return "banned", f"auto_ban_error_rate_{error_rate:.2f}"

        if ctrl_timeouts >= ban_max_timeouts:
            return "banned", f"auto_ban_timeouts_{ctrl_timeouts}"

    # Sustained failure decides degraded/quarantine
    if recent_n >= win_min:
        if recent_fail_rate >= win_quarantine:
            state = "quarantined"
            reason = f"sustained_fail_rate_{recent_fail_rate:.2f}_n={recent_n}"
        elif recent_fail_rate >= win_degraded:
            state = "degraded"
            reason = f"sustained_fail_rate_{recent_fail_rate:.2f}_n={recent_n}"

    # Latency degraded (can stack)
    degraded_latency = float(policy["latency_ms"]["degraded"])
    if latency_ms is not None and float(latency_ms) >= degraded_latency:
        if state == "healthy":
            state = "degraded"
            reason = f"high_latency_{float(latency_ms):.1f}ms"
        else:
            reason = f"{reason}_high_latency_{float(latency_ms):.1f}ms"

    # Auto-recovery (prefer rolling window: if recent fail rate is low and no recent failures)
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
        # Use rolling window if we have it; fallback to lifetime error_rate otherwise
        if recent_n >= win_min:
            ok_now = recent_fail_rate <= rec_max_err
        else:
            ok_now = error_rate <= rec_max_err

        if ok_now and (last_fail_age is None or last_fail_age >= rec_cooldown):
            state = "healthy"
            reason = "recovered"

    return state, reason


def _set_agent_health(name: str, entry: Dict[str, Any], state: str, reason: str, now: Optional[float] = None) -> None:
    if now is None:
        now = _now()

    old_state = entry.get("state")
    old_reason = entry.get("health_reason")

    entry["state"] = state
    entry["health_reason"] = reason
    AGENTS[name] = entry

    _emit_health_event(name, old_state, state, old_reason, reason, ts=now)


def _refresh_agent_states(now: Optional[float] = None) -> None:
    if now is None:
        now = _now()
    for name, info in AGENTS.items():
        state, reason = _compute_agent_state(info, now)
        _set_agent_health(name, info, state, reason, now)


# -----------------------------------------------------------------------------
# Lease reaper
# -----------------------------------------------------------------------------

async def _lease_reaper_loop(interval_s: float = 1.0) -> None:
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
                    agent_name = job.get("leased_by")
                    if agent_name and agent_name in AGENTS:
                        entry = AGENTS[agent_name]
                        metrics = entry.get("metrics") or {}
                        # During warmup, don’t punish timeouts
                        if not _in_warmup(entry, now):
                            timeouts = int(metrics.get("ctrl_lease_timeouts", 0) or 0)
                            metrics["ctrl_lease_timeouts"] = timeouts + 1
                            metrics["ctrl_last_failure_ts"] = now
                            entry["metrics"] = metrics

                            # NEW: treat lease timeout as a failure in the rolling window
                            _record_outcome(entry, failed=True)

                            state, reason = _compute_agent_state(entry, now)
                            _set_agent_health(agent_name, entry, state, reason, now)

                    job["status"] = "queued"
                    job["leased_ts"] = None
                    job["leased_by"] = None
                    
                    # NEW: RE-QUEUE to correct priority level
                    lvl = int(job.get("excitatory_level", 1) or 1)
                    lvl = max(0, min(3, lvl))
                    TASK_QUEUES[lvl].append(job_id)

        await asyncio.sleep(interval_s)


@app.on_event("startup")
async def controller_startup() -> None:
    asyncio.create_task(_lease_reaper_loop())


# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------

@app.get("/healthz", response_class=PlainTextResponse)
def healthz() -> Response:
    with STATE_LOCK:
        now = _now()
        _refresh_agent_states(now)
        # NEW: Sum all queues
        queue_len = sum(len(q) for q in TASK_QUEUES.values())
        agents_online = len(AGENTS)
        active_agents = sum(1 for info in AGENTS.values() if info.get("state") != "dead")

    return PlainTextResponse(
        f"ok queue={queue_len} agents={agents_online} active_agents={active_agents}",
        media_type="text/plain",
    )


# -----------------------------------------------------------------------------
# Risk endpoints
# -----------------------------------------------------------------------------

@app.get("/risk/config")
@app.get("/api/risk/config")
def get_risk_config() -> Dict[str, Any]:
    with STATE_LOCK:
        return {"status": "ok", "config": dict(RISK_CONFIG)}


@app.post("/risk/config")
@app.post("/api/risk/config")
async def set_risk_config(request: Request) -> Dict[str, Any]:
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="config payload must be an object")

    with STATE_LOCK:
        if "tau_sec" in body:
            try:
                RISK_CONFIG["tau_sec"] = float(body["tau_sec"])
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="tau_sec must be a number")

        if "max_entities" in body:
            try:
                RISK_CONFIG["max_entities"] = int(body["max_entities"])
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="max_entities must be an int")

        if "weights" in body:
            if not isinstance(body["weights"], dict):
                raise HTTPException(status_code=400, detail="weights must be an object")
            w = dict(RISK_CONFIG.get("weights") or {})
            for k, v in body["weights"].items():
                try:
                    w[str(k)] = float(v)
                except (TypeError, ValueError):
                    raise HTTPException(status_code=400, detail=f"weight {k!r} must be a number")
            RISK_CONFIG["weights"] = w

        if "thresholds" in body:
            if not isinstance(body["thresholds"], dict):
                raise HTTPException(status_code=400, detail="thresholds must be an object")
            t = dict(RISK_CONFIG.get("thresholds") or {})
            for k, v in body["thresholds"].items():
                try:
                    t[str(k)] = float(v)
                except (TypeError, ValueError):
                    raise HTTPException(status_code=400, detail=f"threshold {k!r} must be a number")
            RISK_CONFIG["thresholds"] = t

        max_entities = int(RISK_CONFIG.get("max_entities", 20000) or 20000)
        if len(RISK_STATE) > max_entities:
            items = list(RISK_STATE.items())
            items.sort(key=lambda kv: float((kv[1] or {}).get("updated_ts", 0) or 0))
            overflow = len(items) - max_entities
            for i in range(max(0, overflow)):
                RISK_STATE.pop(items[i][0], None)

        return {"status": "ok", "config": dict(RISK_CONFIG)}


@app.get("/risk/state")
@app.get("/api/risk/state")
def get_risk_state(limit: int = 500) -> Dict[str, Any]:
    with STATE_LOCK:
        items = list(RISK_STATE.items())
        items.sort(key=lambda kv: float((kv[1] or {}).get("updated_ts", 0) or 0), reverse=True)

        if limit is not None and limit > 0:
            items = items[: int(limit)]

        data = [{**v, "entity_id": k} for k, v in items]
        return {"status": "ok", "count": len(data), "entities": data}


def _ingest_risk_accumulate_result(
    result: Any,
    job_id: str,
    agent: Optional[str],
    op: str,
    now: float,
) -> None:
    if not isinstance(result, dict):
        return
    rows = result.get("results")
    if not isinstance(rows, list):
        return

    max_entities = int(RISK_CONFIG.get("max_entities", 20000) or 20000)

    for row in rows:
        if not isinstance(row, dict):
            continue
        ent = row.get("entity_id")
        if not ent:
            continue
        ent_id = str(ent)

        try:
            B = float(row.get("B", 0.0) or 0.0)
        except (TypeError, ValueError):
            B = 0.0

        warn = bool(row.get("warn", False))
        act = bool(row.get("act", False))

        RISK_STATE[ent_id] = {
            "B": B,
            "warn": warn,
            "act": act,
            "updated_ts": now,
            "source_job_id": job_id,
            "source_agent": agent,
            "source_op": op,
        }

    if len(RISK_STATE) > max_entities:
        items = list(RISK_STATE.items())
        items.sort(key=lambda kv: float((kv[1] or {}).get("updated_ts", 0) or 0))
        overflow = len(items) - max_entities
        for i in range(max(0, overflow)):
            RISK_STATE.pop(items[i][0], None)


# -----------------------------------------------------------------------------
# Job submission (FIXED: supports job_id + pinned agent + EXCITATORY LEVELS)
# -----------------------------------------------------------------------------

def _is_agent_available_for_pin(name: str) -> bool:
    info = AGENTS.get(name)
    if not info:
        return False
    st = info.get("state") or "unknown"
    # treat degraded as “available” unless you want pins to require healthy only
    return st in ("healthy", "degraded")

def _lease_timeout_for_level(level: int) -> int:
    """Higher excitatory_level = shorter lease (fail fast)."""
    if level >= 3:
        return 15
    if level == 2:
        return 30
    if level == 1:
        return 300
    return 300

def _enqueue_job(op: str, payload: Dict[str, Any], job_id: Optional[str] = None, pinned_agent: Optional[str] = None, excitatory_level: int = 1) -> str:
    global JOBS, TASK_QUEUES

    if job_id is None:
        job_id = str(uuid.uuid4())

    # Ensure level is valid
    lvl = max(0, min(3, int(excitatory_level)))
    
    job = {
        "id": job_id,
        "op": op,
        "payload": payload,
        "created_ts": _now(),
        "leased_ts": None,
        "leased_by": None,
        "lease_timeout_s": _lease_timeout_for_level(lvl),
        "status": "queued",  # queued | leased | completed | failed
        "result": None,
        "error": None,
        "completed_ts": None,
        "duration_ms": None,
        "pinned_agent": pinned_agent,
        # NEW: Store the level
        "excitatory_level": lvl,
    }
    JOBS[job_id] = job
    
    # NEW: Route to correct queue
    TASK_QUEUES[lvl].append(job_id)
    
    return job_id


@app.post("/job")
@app.post("/api/job")
async def submit_job(request: Request) -> Dict[str, Any]:
    """
    Submit one or more jobs.
    Single: {"op": "...", "payload": {...}, "job_id": "...", "agent": "...", "excitatory_level": 3}
    """
    body = await request.json()

    def _one(item: Dict[str, Any]) -> str:
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail="Each job must be an object")
        op = item.get("op")
        payload = item.get("payload")
        req_job_id = item.get("job_id") or item.get("id")
        req_agent = item.get("agent")
        
        # NEW: Parse excitatory level
        req_level = item.get("excitatory_level")
        try:
            excitatory_level = int(req_level) if req_level is not None else 1
        except (TypeError, ValueError):
            excitatory_level = 1

        if not op or payload is None:
            raise HTTPException(status_code=400, detail="Each job requires {op, payload}")

        with STATE_LOCK:
            pinned_agent = None
            if req_agent:
                if not _is_agent_available_for_pin(req_agent):
                    raise HTTPException(
                        status_code=409,
                        detail={"error": "requested_agent_unavailable", "agent": req_agent},
                    )
                pinned_agent = req_agent

            return _enqueue_job(op=str(op), payload=payload, job_id=req_job_id, pinned_agent=pinned_agent, excitatory_level=excitatory_level)

    if isinstance(body, list):
        ids = [_one(it) for it in body]
        return {"status": "ok", "job_ids": ids}

    job_id = _one(body)
    return {"status": "ok", "job_ids": [job_id]}


# -----------------------------------------------------------------------------
# Agents (FIXED: warmup on register and after restart-like gaps)
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

    raw_worker_profile = payload.get("worker_profile")
    if raw_worker_profile is None:
        raw_worker_profile = labels.get("worker_profile")
    worker_profile = raw_worker_profile or {}

    now = _now()

    metrics: Dict[str, Any] = {}
    nested_metrics = payload.get("metrics") or {}
    for key in AGENT_METRIC_KEYS:
        if key in payload:
            metrics[key] = payload.get(key)
        elif key in nested_metrics:
            metrics[key] = nested_metrics.get(key)

    with STATE_LOCK:
        warmup_sec = float(WARMUP_POLICY.get("warmup_sec", 20.0) or 20.0)

        info: Dict[str, Any] = {
            "labels": labels,
            "capabilities": capabilities,
            "worker_profile": worker_profile,
            "last_seen": now,
            "metrics": metrics,
            "warmup_until_ts": now + warmup_sec,
        }
        # init rolling window
        _get_health_window(info)

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
        prev_seen = entry.get("last_seen")

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

        # ensure rolling window exists
        _get_health_window(entry)

        # If the agent disappeared long enough, treat as restart and re-warm
        try:
            restart_gap = float(WARMUP_POLICY.get("restart_gap_sec", 45.0) or 45.0)
        except (TypeError, ValueError):
            restart_gap = 45.0

        gap = None
        if prev_seen is not None:
            try:
                gap = now - float(prev_seen)
            except (TypeError, ValueError):
                gap = None

        if gap is None or gap >= restart_gap:
            try:
                warmup_sec = float(WARMUP_POLICY.get("warmup_sec", 20.0) or 20.0)
            except (TypeError, ValueError):
                warmup_sec = 20.0
            entry["warmup_until_ts"] = now + warmup_sec

        state, reason = _compute_agent_state(entry, now)
        _set_agent_health(name, entry, state, reason, now)

    return {"status": "ok", "agent": name, "time": now}


@app.get("/agents")
@app.get("/api/agents")
def list_agents() -> Dict[str, Any]:
    with STATE_LOCK:
        now = _now()
        _refresh_agent_states(now)

        out = {}
        for name, info in AGENTS.items():
            # expose rolling window summary (not the whole buffer)
            n, fr = _recent_fail_rate(info)
            out[name] = {
                "labels": dict(info.get("labels") or {}),
                "capabilities": dict(info.get("capabilities") or {}),
                "last_seen": info.get("last_seen"),
                "state": info.get("state", "unknown"),
                "health_reason": info.get("health_reason"),
                "metrics": dict(info.get("metrics") or {}),
                "worker_profile": dict(info.get("worker_profile") or {}),
                "warmup_until_ts": info.get("warmup_until_ts"),
                "recent": {"n": n, "fail_rate": fr},
            }
        return out


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

        # Also give it a brief warmup on restore
        try:
            warmup_sec = float(WARMUP_POLICY.get("warmup_sec", 20.0) or 20.0)
        except (TypeError, ValueError):
            warmup_sec = 20.0
        entry["warmup_until_ts"] = now + warmup_sec

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
        data = [] if limit <= 0 else HEALTH_EVENTS[-limit:]
    return {"events": data}


@app.post("/events/clear")
@app.post("/api/events/clear")
def clear_events() -> Dict[str, Any]:
    global HEALTH_EVENTS
    with STATE_LOCK:
        HEALTH_EVENTS = []
    return {"status": "ok", "cleared": True}


# -----------------------------------------------------------------------------
# Task leasing (FIXED: pinned agent honored + LEASE FIX + EXCITATORY LEVELS)
# -----------------------------------------------------------------------------

@app.get("/task")
@app.get("/api/task", summary="Agent polls for next task")
def get_task(agent: str, wait_ms: int = 0):
    deadline = _now() + (wait_ms / 1000.0)

    while True:
        with STATE_LOCK:
            task = _lease_next_job(agent)

        if task is not None:
            return JSONResponse(task)

        if _now() >= deadline:
            return Response(status_code=204)

        time.sleep(0.05)


def _lease_next_job(agent: str) -> Optional[Dict[str, Any]]:
    global LEASED_TOTAL

    now = _now()
    agent_info = AGENTS.get(agent) or {}
    state = agent_info.get("state") or "unknown"

    if state in ("dead", "quarantined", "banned", "stale"):
        return None

    mode = _cluster_policy_mode()
    if state == "degraded" and mode == "aggressive":
        summary = _cluster_health_summary()
        if summary.get("healthy_workers", 0) > 0:
            return None

    # Two-pass selection:
    #   Pass 1: jobs pinned to this agent (scan high prio -> low prio)
    #   Pass 2: unpinned jobs (scan high prio -> low prio)
    def _try_pass(require_pinned_match: bool) -> Optional[Dict[str, Any]]:
        global LEASED_TOTAL
        nonlocal now
        
        # NEW: Scan Priority Levels High -> Low
        for lvl in (3, 2, 1, 0):
            q = TASK_QUEUES[lvl]
            queue_len = len(q)
            
            for _ in range(queue_len):
                if not q:
                    break

                job_id = q.popleft()
                job = JOBS.get(job_id)
                if not job:
                    continue

                # LEASE FIX: only lease queued jobs (never re-lease leased/completed/failed)
                if job.get("status") != "queued":
                    continue

                pinned = job.get("pinned_agent")
                if require_pinned_match:
                    if pinned != agent:
                        q.append(job_id)
                        continue
                else:
                    # unpinned only
                    if pinned is not None:
                        q.append(job_id)
                        continue

                payload = job.get("payload") or {}
                prefer_gpu = bool(payload.get("prefer_gpu", False))
                min_vram_gb = payload.get("min_vram_gb")

                if prefer_gpu:
                    wp = agent_info.get("worker_profile") or {}
                    gpu = wp.get("gpu") or {}
                    gpu_present = bool(gpu.get("gpu_present", False))
                    if not gpu_present:
                        q.append(job_id)
                        continue

                    if min_vram_gb is not None:
                        try:
                            vram = float(gpu.get("vram_gb", 0) or 0)
                            min_v = float(min_vram_gb)
                        except (TypeError, ValueError):
                            vram = 0.0
                            min_v = 0.0
                        if vram < min_v:
                            q.append(job_id)
                            continue
                    # Lateral inhibition (brainstem)
                    # Pinned jobs must always win; only gate UNPINNED selection.
                    if ENABLE_LATERAL_INHIBITION:
                        op_name = job.get("op") or "unknown"
                        pinned_agent = job.get("pinned_agent")

                    if pinned_agent is None:
                        if not BRAIN.allow_lease(
                            requesting_agent=agent,
                            op=str(op_name),
                            now=now,
                    ):
                            q.append(job_id)
                            continue

                job["status"] = "leased"
                job["leased_by"] = agent
                job["leased_ts"] = now
                LEASED_TOTAL += 1

                return {
                    "id": job_id,
                    "job_id": job_id,
                    "op": job["op"],
                    "payload": job["payload"],
                }

        return None

    task = _try_pass(require_pinned_match=True)
    if task is not None:
        return task

    return _try_pass(require_pinned_match=False)


# -----------------------------------------------------------------------------
# Results
# -----------------------------------------------------------------------------

@app.post("/result")
@app.post("/api/result")
async def post_result(request: Request) -> Dict[str, Any]:
    global COMPLETED_TOTAL, FAILED_TOTAL

    payload = await request.json()
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

        agent_name = job.get("leased_by") or payload.get("agent")

        # Controller-maintained per-agent metrics
        if agent_name and agent_name in AGENTS:
            entry = AGENTS[agent_name]
            metrics = entry.get("metrics") or {}

            # Ensure window exists
            _get_health_window(entry)

            # During warmup, do NOT count failures against agent health
            if not _in_warmup(entry, now):
                if err:
                    metrics["ctrl_tasks_failed"] = int(metrics.get("ctrl_tasks_failed", 0) or 0) + 1
                    metrics["ctrl_last_failure_ts"] = now
                    _record_outcome(entry, failed=True)
                else:
                    metrics["ctrl_tasks_completed"] = int(metrics.get("ctrl_tasks_completed", 0) or 0) + 1
                    _record_outcome(entry, failed=False)
            else:
                # Keep warmup neutral, but still record successes (helps window fill without punishing)
                if not err:
                    metrics["ctrl_tasks_completed"] = int(metrics.get("ctrl_tasks_completed", 0) or 0) + 1
                    _record_outcome(entry, failed=False)

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

        if not err and op == "risk_accumulate":
            _ingest_risk_accumulate_result(
                result=result,
                job_id=job_id,
                agent=agent_name,
                op=op,
                now=now,
            )

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
# Jobs / reporting (FIXED: results filter)
# -----------------------------------------------------------------------------

@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> Dict[str, Any]:
    with STATE_LOCK:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Unknown job")
        return dict(job)


@app.get("/results")
@app.get("/api/results")
def list_results(limit: int = 50, job_id: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    with STATE_LOCK:
        jobs = list(JOBS.values())
        jobs.sort(key=lambda j: j.get("created_ts", 0), reverse=True)

        if job_id:
            data = [j for j in jobs if str(j.get("id")) == str(job_id)]
        else:
            data = [j for j in jobs if j.get("status") in ("completed", "failed")]

        if limit is not None and limit > 0:
            data = data[: int(limit)]

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
        _refresh_agent_states(now)

        agents_online = len(AGENTS)
        # NEW: Sum all queue lengths
        queue_len = sum(len(q) for q in TASK_QUEUES.values())
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

        risk_total = len(RISK_STATE)
        risk_warn = sum(1 for v in RISK_STATE.values() if bool((v or {}).get("warn", False)))
        risk_act = sum(1 for v in RISK_STATE.values() if bool((v or {}).get("act", False)))

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
        "risk": {"entities": risk_total, "warn": risk_warn, "act": risk_act},
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
    template = payload.get("payload_template") or {"text": "The quick brown fox jumps over the lazy dog."}

    job_ids = []
    with STATE_LOCK:
        for _ in range(count):
            body = dict(template)
            body["ts"] = _now()
            job_ids.append(_enqueue_job(op, body))

    return {"status": "ok", "count": count, "op": op, "job_ids": job_ids}


@app.post("/purge")
def purge() -> Dict[str, Any]:
    global JOBS, TASK_QUEUES, LEASED_TOTAL, COMPLETED_TOTAL, FAILED_TOTAL
    global COMPLETION_TIMES, OP_COUNTS, OP_TOTAL_MS
    global RISK_STATE, PIPELINES, HEALTH_EVENTS  # NEW: Added PIPELINES and HEALTH_EVENTS

    with STATE_LOCK:
        JOBS = {}
        
        # CORRECTED: Do NOT re-bind TASK_QUEUES. 
        # Clear the deque objects in-place so TASK_QUEUE alias remains valid.
        for q in TASK_QUEUES.values():
            q.clear()
            
        # NEW: Clear pipelines and health events for a true purge
        PIPELINES = {}
        HEALTH_EVENTS = []
            
        LEASED_TOTAL = 0
        COMPLETED_TOTAL = 0
        FAILED_TOTAL = 0
        COMPLETION_TIMES = []
        OP_COUNTS = defaultdict(int)
        OP_TOTAL_MS = defaultdict(float)
        RISK_STATE = {}

    return {"status": "ok", "message": "purged"}
