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

app = FastAPI(title="Distributed Swarm Controller")

# -----------------------------------------------------------------------------
# Global state
# -----------------------------------------------------------------------------

STATE_LOCK = threading.RLock()

JOBS: Dict[str, Dict[str, Any]] = {}

# NEW: Multiple priority queues by excitatory level (0..3)
TASK_QUEUES: Dict[int, Deque[str]] = {0: deque(), 1: deque(), 2: deque(), 3: deque()}
# Back-compat alias: default queue is excitatory_level=1
TASK_QUEUE: Deque[str] = TASK_QUEUES[1]

AGENTS: Dict[str, Dict[str, Any]] = {}

LEASED_TOTAL = 0
COMPLETED_TOTAL = 0
FAILED_TOTAL = 0

COMPLETION_TIMES: List[float] = []
OP_COUNTS: Dict[str, int] = defaultdict(int)
OP_TOTAL_MS: Dict[str, float] = defaultdict(float)

# -----------------------------------------------------------------------------
# Health policy / warmup / quarantine plumbing
# -----------------------------------------------------------------------------

# Health event ring buffer (controller-side)
HEALTH_EVENTS: Deque[Dict[str, Any]] = deque(maxlen=500)

# Warmup: ignore early timeouts/errors shortly after register or long heartbeat gaps
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
}

RISK_STATE: Dict[str, Dict[str, Any]] = {}  # entity_id -> {B, last_ts, score, ...}

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def _now() -> float:
    return time.time()

def _emit_health_event(kind: str, agent: Optional[str], detail: Dict[str, Any]) -> None:
    HEALTH_EVENTS.append(
        {"ts": _now(), "kind": kind, "agent": agent, "detail": dict(detail or {})}
    )

def _in_warmup(agent_entry: Dict[str, Any], now: float) -> bool:
    warmup_until = agent_entry.get("warmup_until_ts")
    if warmup_until is None:
        return False
    try:
        return float(now) < float(warmup_until)
    except (TypeError, ValueError):
        return False

def _set_warmup(agent_entry: Dict[str, Any], now: float) -> None:
    agent_entry["warmup_until_ts"] = float(now) + float(WARMUP_POLICY["warmup_sec"])

def _compute_agent_state(agent_entry: Dict[str, Any], now: float) -> Tuple[str, str]:
    """
    Computes state and reason:
      healthy | degraded | quarantined | banned | stale | dead
    """
    manual_state = agent_entry.get("manual_state")  # quarantined/banned overrides
    if manual_state in ("quarantined", "banned"):
        return manual_state, agent_entry.get("manual_reason", "")

    last_seen = agent_entry.get("last_seen")
    if last_seen is None:
        return "unknown", "no_heartbeat"
    try:
        age = float(now) - float(last_seen)
    except (TypeError, ValueError):
        return "unknown", "bad_heartbeat_ts"

    if age > 120.0:
        return "dead", f"no_heartbeat_{int(age)}s"
    if age > 30.0:
        return "stale", f"heartbeat_age_{int(age)}s"

    # Error-rate / timeouts based quarantine logic
    metrics = agent_entry.get("metrics") or {}
    errors = float(metrics.get("ctrl_error_rate", 0.0) or 0.0)
    timeouts = int(metrics.get("ctrl_lease_timeouts", 0) or 0)

    if _in_warmup(agent_entry, now):
        # During warmup, don't punish
        return "healthy", "warmup"

    if errors >= 0.25 or timeouts >= 3:
        return "quarantined", "policy_threshold"

    if errors >= 0.05 or timeouts >= 1:
        return "degraded", "elevated_errors"

    return "healthy", "ok"

def _set_agent_health(name: str, agent_entry: Dict[str, Any], state: str, reason: str, now: float) -> None:
    prev_state = agent_entry.get("state")
    agent_entry["state"] = state
    agent_entry["state_reason"] = reason
    agent_entry["state_ts"] = now
    if prev_state != state:
        _emit_health_event(
            "agent_state_change",
            name,
            {"from": prev_state, "to": state, "reason": reason},
        )

def _refresh_agent_states(now: float) -> None:
    for name, entry in list(AGENTS.items()):
        state, reason = _compute_agent_state(entry, now)
        _set_agent_health(name, entry, state, reason, now)

# -----------------------------------------------------------------------------
# Lease reaper loop (timeouts -> requeue, plus metrics)
# -----------------------------------------------------------------------------

def _lease_reaper_loop() -> None:
    while True:
        time.sleep(0.5)
        now = _now()
        with STATE_LOCK:
            # Recompute agent states periodically
            _refresh_agent_states(now)

            for job_id, job in list(JOBS.items()):
                if job.get("status") != "leased":
                    continue
                leased_ts = job.get("leased_ts")
                timeout_s = job.get("lease_timeout_s", 300)
                if leased_ts is None:
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

                            _emit_health_event(
                                "lease_timeout",
                                agent_name,
                                {"job_id": job_id, "op": job.get("op")},
                            )

                    # Requeue job
                    job["status"] = "queued"
                    job["leased_by"] = None
                    job["leased_ts"] = None

                    lvl = int(job.get("excitatory_level", 1) or 1)
                    lvl = max(0, min(3, lvl))
                    TASK_QUEUES[lvl].append(job_id)

# Start reaper thread
threading.Thread(target=_lease_reaper_loop, daemon=True).start()

# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------

@app.get("/healthz", response_class=PlainTextResponse)
def healthz() -> str:
    with STATE_LOCK:
        now = _now()
        _refresh_agent_states(now)
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
        raise HTTPException(status_code=400, detail="Expected JSON object")

    with STATE_LOCK:
        for k in ("tau_sec", "weights"):
            if k in body:
                RISK_CONFIG[k] = body[k]

    return {"status": "ok", "config": dict(RISK_CONFIG)}

@app.post("/risk/accumulate")
@app.post("/api/risk/accumulate")
async def risk_accumulate(request: Request) -> Dict[str, Any]:
    """
    Payload:
      { "entity_id": "foo", "dt_sec": 30, "channels": {"food": 1, "air": 0, "contact": 0}, "warn": bool, "act": bool }
    Controller accumulates risk signal (B) and projects score.
    """
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")

    ent = body.get("entity_id")
    if not ent:
        raise HTTPException(status_code=400, detail="Missing entity_id")

    now = _now()
    dt_sec = float(body.get("dt_sec", 0.0) or 0.0)
    channels = body.get("channels") or {}
    warn = bool(body.get("warn", False))
    act = bool(body.get("act", False))

    weights = RISK_CONFIG.get("weights") or {}
    B = 0.0
    for k, w in weights.items():
        try:
            B += float(w or 0.0) * float(channels.get(k, 0.0) or 0.0)
        except (TypeError, ValueError):
            continue

    with STATE_LOCK:
        st = RISK_STATE.get(str(ent)) or {"B": 0.0, "last_ts": now, "score": 0.0}
        tau = float(RISK_CONFIG.get("tau_sec", 3600.0) or 3600.0)

        last_ts = float(st.get("last_ts", now) or now)
        elapsed = max(0.0, now - last_ts)

        # simple exponential decay on score
        decay = 1.0
        if tau > 0:
            decay = pow(2.718281828, -elapsed / tau)

        score = float(st.get("score", 0.0) or 0.0) * decay + B * dt_sec

        st.update(
            {
                "B": B,
                "last_ts": now,
                "score": score,
                "warn": warn,
                "act": act,
            }
        )
        RISK_STATE[str(ent)] = st

    return {"status": "ok", "entity_id": str(ent), "B": B, "score": score, "warn": warn, "act": act}

@app.post("/risk/bulk_accumulate")
@app.post("/api/risk/bulk_accumulate")
async def risk_bulk_accumulate(request: Request) -> Dict[str, Any]:
    """
    Accepts a list of risk_accumulate payloads.
    """
    body = await request.json()
    if not isinstance(body, list):
        raise HTTPException(status_code=400, detail="Expected JSON list")

    now = _now()
    weights = RISK_CONFIG.get("weights") or {}
    tau = float(RISK_CONFIG.get("tau_sec", 3600.0) or 3600.0)

    with STATE_LOCK:
        for row in body:
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

            st = RISK_STATE.get(ent_id) or {"B": 0.0, "last_ts": now, "score": 0.0}
            last_ts = float(st.get("last_ts", now) or now)
            elapsed = max(0.0, now - last_ts)

            decay = 1.0
            if tau > 0:
                decay = pow(2.718281828, -elapsed / tau)

            score = float(st.get("score", 0.0) or 0.0) * decay + B

            st.update(
                {
                    "B": B,
                    "last_ts": now,
                    "score": score,
                    "warn": warn,
                    "act": act,
                }
            )
            RISK_STATE[ent_id] = st

    return {"status": "ok", "updated": len(body)}

@app.get("/risk/state")
@app.get("/api/risk/state")
def risk_state(entity_id: Optional[str] = None) -> Dict[str, Any]:
    with STATE_LOCK:
        if entity_id:
            return {"status": "ok", "entity_id": entity_id, "state": RISK_STATE.get(entity_id)}
        return {"status": "ok", "states": dict(RISK_STATE)}

# -----------------------------------------------------------------------------
# Agent enrollment / heartbeat
# -----------------------------------------------------------------------------

AGENT_METRIC_KEYS = [
    "ctrl_error_rate",
    "ctrl_lease_timeouts",
    "ctrl_last_failure_ts",
    "avg_task_ms",
    "ctrl_avg_latency_ms",
    "done",
]

@app.post("/enroll")
@app.post("/api/enroll")
async def enroll(request: Request) -> Dict[str, Any]:
    """
    Agent enroll/heartbeat endpoint.
    Accepts:
      {
        "agent": "name",
        "labels": {...},
        "capabilities": {...},
        "worker_profile": {...},
        "metrics": {...} or metric keys at top level
      }
    """
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")

    name = payload.get("agent") or payload.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Missing agent/name")

    now = _now()
    labels = payload.get("labels") or {}
    capabilities = payload.get("capabilities") or {}
    worker_profile = payload.get("worker_profile") or payload.get("profile") or {}

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
        entry["worker_profile"] = worker_profile

        # Warmup logic: if first seen or long gap => warmup
        if prev_seen is None:
            _set_warmup(entry, now)
        else:
            try:
                gap = float(now) - float(prev_seen)
            except (TypeError, ValueError):
                gap = 0.0
            if gap >= float(WARMUP_POLICY["restart_gap_sec"]):
                _set_warmup(entry, now)

        # Update controller-maintained metrics
        metrics = entry.get("metrics") or {}
        for k, v in metrics_update.items():
            metrics[k] = v
        entry["metrics"] = metrics

        # Refresh and record state
        state, reason = _compute_agent_state(entry, now)
        _set_agent_health(name, entry, state, reason, now)

        AGENTS[name] = entry

    return {"status": "ok", "agent": name, "state": state, "reason": reason}

# -----------------------------------------------------------------------------
# Agent admin endpoints (manual quarantine/ban/restore)
# -----------------------------------------------------------------------------

@app.get("/agents")
@app.get("/api/agents")
def list_agents() -> Dict[str, Any]:
    now = _now()
    with STATE_LOCK:
        _refresh_agent_states(now)
        agents_out = {}
        for name, ent in AGENTS.items():
            view = dict(ent)
            view["name"] = name
            agents_out[name] = view
        return {"status": "ok", "agents": agents_out}

@app.post("/agents/{name}/quarantine")
@app.post("/api/agents/{name}/quarantine")
async def quarantine_agent(name: str, request: Request) -> Dict[str, Any]:
    payload = await request.json()
    manual_reason = payload.get("reason", "manual_quarantine")
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

@app.post("/agents/{name}/ban")
@app.post("/api/agents/{name}/ban")
async def ban_agent(name: str, request: Request) -> Dict[str, Any]:
    payload = await request.json()
    manual_reason = payload.get("reason", "manual_ban")
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

@app.post("/agents/{name}/restore")
@app.post("/api/agents/{name}/restore")
async def restore_agent(name: str, request: Request) -> Dict[str, Any]:
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

# -----------------------------------------------------------------------------
# Health events endpoints
# -----------------------------------------------------------------------------

@app.get("/events")
@app.get("/api/events")
def health_events(limit: int = 50) -> Dict[str, Any]:
    with STATE_LOCK:
        evs = list(HEALTH_EVENTS)[-max(1, min(500, int(limit))):]
    return {"status": "ok", "events": evs}

# -----------------------------------------------------------------------------
# Job submission (FIXED: supports job_id + pinned agent)
# -----------------------------------------------------------------------------

def _is_agent_available_for_pin(name: str) -> bool:
    info = AGENTS.get(name)
    if not info:
        return False
    st = info.get("state") or "unknown"
    # treat degraded as “available” unless you want pins to require healthy only
    return st in ("healthy", "degraded")


def _lease_timeout_for_level(level: int) -> int:
    """Higher excitatory_level = shorter lease (fail fast, avoid clogging)."""
    try:
        lvl = int(level)
    except (TypeError, ValueError):
        lvl = 1
    if lvl >= 3:
        return 15
    if lvl == 2:
        return 30
    if lvl == 1:
        return 120
    return 300


def _enqueue_job(op: str, payload: Dict[str, Any], job_id: Optional[str] = None, pinned_agent: Optional[str] = None, excitatory_level: int = 1) -> str:
    global JOBS, TASK_QUEUE

    if job_id is None:
        job_id = str(uuid.uuid4())

    job = {
        "id": job_id,
        "op": op,
        "payload": payload,
        "created_ts": _now(),
        "leased_ts": None,
        "leased_by": None,
        "lease_timeout_s": _lease_timeout_for_level(excitatory_level),
        "status": "queued",  # queued | leased | completed | failed
        "result": None,
        "error": None,
        "completed_ts": None,
        "duration_ms": None,
        # NEW:
        "pinned_agent": pinned_agent,
        "excitatory_level": max(0, min(3, int(excitatory_level))) if isinstance(excitatory_level, int) else 1,
    }
    JOBS[job_id] = job
    lvl = int(job.get("excitatory_level", 1) or 1)
    lvl = max(0, min(3, lvl))
    TASK_QUEUES[lvl].append(job_id)
    return job_id


@app.post("/job")
@app.post("/api/job")
async def submit_job(request: Request) -> Dict[str, Any]:
    """
    Submit one or more jobs.

    Single:
      {"op": "...", "payload": {...}, "job_id": "...(optional)...", "agent": "...(optional pin)...", "excitatory_level": 0-3 (optional) }

    Batch:
      [{"op": "...", "payload": {...}, "job_id": "...", "agent": "..."}, ...]
    """
    body = await request.json()

    def _one(item: Dict[str, Any]) -> str:
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail="Each job must be an object")
        op = item.get("op")
        payload = item.get("payload")
        req_job_id = item.get("job_id") or item.get("id")
        req_agent = item.get("agent")

        if not op or payload is None:
            raise HTTPException(status_code=400, detail="Each job requires {op, payload}")

        with STATE_LOCK:
            pinned_agent = None
            if req_agent:
                if not _is_agent_available_for_pin(req_agent):
                    # Hard pin: either queue pinned or reject
                    raise HTTPException(
                        status_code=409,
                        detail={"error": "requested_agent_unavailable", "agent": req_agent},
                    )
                pinned_agent = req_agent

            req_level = item.get("excitatory_level")
            try:
                excitatory_level = int(req_level) if req_level is not None else 1
            except (TypeError, ValueError):
                excitatory_level = 1
            excitatory_level = max(0, min(3, excitatory_level))

            return _enqueue_job(op=str(op), payload=payload, job_id=req_job_id, pinned_agent=pinned_agent, excitatory_level=excitatory_level)

    if isinstance(body, list):
        ids = [_one(it) for it in body]
        return {"status": "ok", "job_ids": ids}

    job_id = _one(body)
    return {"status": "ok", "job_ids": [job_id]}

# -----------------------------------------------------------------------------
# Intake endpoints (ingress HTTP)
# -----------------------------------------------------------------------------

@app.post("/intake")
@app.post("/api/intake")
async def intake(request: Request) -> Dict[str, Any]:
    """
    Minimal intake endpoint; parses intake request and runs pipeline.
    """
    body = await request.body()
    intake_req: IntakeRequest = parse_intake_body(body)
    result = run_text_pipeline(intake_req)
    return {"status": "ok", "result": result}

# -----------------------------------------------------------------------------
# Lease endpoint (agent pulls work)
# -----------------------------------------------------------------------------

@app.get("/lease")
@app.get("/api/lease")
def lease(agent: str = Query(..., description="Agent name")) -> Dict[str, Any]:
    with STATE_LOCK:
        task = _lease_next_job(agent)
        if task is None:
            return Response(status_code=204)
        return task

@app.get("/lease_wait")
@app.get("/api/lease_wait")
def lease_wait(agent: str = Query(..., description="Agent name"), timeout_s: float = 10.0) -> Dict[str, Any]:
    deadline = _now() + float(timeout_s)
    while True:
        with STATE_LOCK:
            task = _lease_next_job(agent)
            if task is not None:
                return task

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

    # Two-pass lease strategy:
    #  - Pass 1: only jobs pinned to this agent
    #  - Pass 2: only unpinned jobs
    def _try_pass(require_pinned_match: bool) -> Optional[Dict[str, Any]]:
        global LEASED_TOTAL
        nonlocal now

        # Higher excitatory_level first (3 -> 0). Within a level, preserve FIFO by cycling.
        for lvl in (3, 2, 1, 0):
            q = TASK_QUEUES[lvl]
            q_len = len(q)

            for _ in range(q_len):
                if not q:
                    break

                job_id = q.popleft()
                job = JOBS.get(job_id)
                if not job:
                    continue

                if job.get("status") != "queued":
                    continue

                pinned = job.get("pinned_agent")
                if require_pinned_match:
                    if pinned != agent:
                        q.append(job_id)
                        continue
                else:
                    if pinned is not None:
                        q.append(job_id)
                        continue

                # GPU preference checks (unchanged)
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

                job["status"] = "leased"
                job["leased_by"] = agent
                job["leased_ts"] = now
                LEASED_TOTAL += 1

                return {
                    "id": job_id,
                    "job_id": job_id,
                    "op": job["op"],
                    "payload": job["payload"],
                    "excitatory_level": job.get("excitatory_level", 1),
                    "lease_timeout_s": job.get("lease_timeout_s", 0),
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

        ok = bool(payload.get("ok", True))
        result = payload.get("result")
        error = payload.get("error")

        job["completed_ts"] = now
        job["result"] = result
        job["error"] = error

        duration_ms = None
        if job.get("leased_ts"):
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

            done = int(metrics.get("done", 0) or 0)
            metrics["done"] = done + 1

            # controller error rate: naive running estimate
            if not ok:
                prev_err = float(metrics.get("ctrl_error_rate", 0.0) or 0.0)
                metrics["ctrl_error_rate"] = min(1.0, prev_err + 0.01)
                metrics["ctrl_last_failure_ts"] = now
            else:
                prev_err = float(metrics.get("ctrl_error_rate", 0.0) or 0.0)
                metrics["ctrl_error_rate"] = max(0.0, prev_err - 0.002)

            # controller avg latency estimate (duration_ms)
            if duration_ms is not None:
                prev = float(metrics.get("ctrl_avg_latency_ms", 0.0) or 0.0)
                if prev <= 0:
                    metrics["ctrl_avg_latency_ms"] = float(duration_ms)
                else:
                    metrics["ctrl_avg_latency_ms"] = 0.9 * prev + 0.1 * float(duration_ms)

            entry["metrics"] = metrics

        if ok:
            job["status"] = "completed"
            COMPLETED_TOTAL += 1
            COMPLETION_TIMES.append(now)
        else:
            job["status"] = "failed"
            FAILED_TOTAL += 1
            COMPLETION_TIMES.append(now)

    return {"status": "ok", "job_id": job_id, "ok": ok}

@app.get("/results")
@app.get("/api/results")
def get_results(job_id: Optional[str] = None) -> Dict[str, Any]:
    with STATE_LOCK:
        if job_id:
            job = JOBS.get(job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Unknown job_id")
            return {"status": "ok", "job": dict(job)}
        return {"status": "ok", "jobs": {k: dict(v) for k, v in JOBS.items()}}

# -----------------------------------------------------------------------------
# Stats
# -----------------------------------------------------------------------------

def _prune_completion_times(now: float, window_s: float) -> None:
    cutoff = now - window_s
    while COMPLETION_TIMES and COMPLETION_TIMES[0] < cutoff:
        COMPLETION_TIMES.pop(0)

@app.get("/stats")
@app.get("/api/stats")
def stats() -> Dict[str, Any]:
    now = _now()
    window_s = 60.0
    window_5m = 300.0
    window_15m = 900.0

    with STATE_LOCK:
        _refresh_agent_states(now)

        agents_online = len(AGENTS)
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

        ops = {}
        for op, cnt in OP_COUNTS.items():
            tot = OP_TOTAL_MS.get(op, 0.0)
            avg = (tot / cnt) if cnt else 0.0
            ops[op] = {"count": cnt, "avg_ms": avg}

    return {
        "status": "ok",
        "queue_len": queue_len,
        "agents_online": agents_online,
        "leased_total": leased_total,
        "completed_total": completed_total,
        "failed_total": failed_total,
        "rate_1s": rate_1s,
        "rate_60s": rate_60s,
        "rate_5m": rate_5m,
        "rate_15m": rate_15m,
        "ops": ops,
    }

@app.get("/stats/sparkline")
@app.get("/api/stats/sparkline")
def stats_sparkline() -> Dict[str, Any]:
    now = _now()
    with STATE_LOCK:
        _prune_completion_times(now, 60.0)
        times = list(COMPLETION_TIMES)
    # crude 60 bins of 1s each
    bins = [0] * 60
    for t in times:
        age = now - t
        if 0 <= age < 60:
            idx = int(59 - age)
            bins[idx] += 1
    return {"status": "ok", "bins": bins}

@app.get("/stats/ops")
@app.get("/api/stats/ops")
def stats_ops() -> Dict[str, Any]:
    with STATE_LOCK:
        ops = {}
        for op, cnt in OP_COUNTS.items():
            tot = OP_TOTAL_MS.get(op, 0.0)
            avg = (tot / cnt) if cnt else 0.0
            ops[op] = {"count": cnt, "avg_ms": avg}
    return {"status": "ok", "ops": ops}

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
    global JOBS, TASK_QUEUES, TASK_QUEUE, LEASED_TOTAL, COMPLETED_TOTAL, FAILED_TOTAL
    global COMPLETION_TIMES, OP_COUNTS, OP_TOTAL_MS
    global RISK_STATE

    with STATE_LOCK:
        JOBS = {}
        TASK_QUEUES = {0: deque(), 1: deque(), 2: deque(), 3: deque()}
        TASK_QUEUE = TASK_QUEUES[1]
        LEASED_TOTAL = 0
        COMPLETED_TOTAL = 0
        FAILED_TOTAL = 0
        COMPLETION_TIMES = []
        OP_COUNTS = defaultdict(int)
        OP_TOTAL_MS = defaultdict(float)
        RISK_STATE = {}

    return {"status": "ok", "message": "purged"}
