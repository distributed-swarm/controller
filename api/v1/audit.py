# controller/api/v1/audit.py
from __future__ import annotations

import hashlib
import os
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, asdict
from typing import Any, Deque, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query

from lifecycle.log import emit

router = APIRouter()

# ----------------------------
# Kernel-ish invariants
# ----------------------------
# Bounded memory: never unbounded growth.
_AUDIT_MAX = 2000
_MAX_AGENT_STATE = 5000  # guardrail for agent tracking maps

# Process identity (helps detect multi-worker split-brain).
_BOOT_ID = str(uuid.uuid4())
_PID = os.getpid()
_PROCESS_START_TS = time.time()

_EVENTS: Deque["AuditEvent"] = deque(maxlen=_AUDIT_MAX)

# Simple counters without requiring Prometheus wiring.
COUNTERS: Counter = Counter()

# Last-seen capability fingerprint per agent (within this process).
_LAST_NORM_HASH: Dict[str, str] = {}
_LAST_SEEN_TS: Dict[str, float] = {}

# Track which PID last produced an event for an agent (helps detect multi-process behavior).
_LAST_AGENT_PID: Dict[str, int] = {}

# Optional: per-(pid,agent) hash so we can reason about local change detection fidelity.
_LAST_NORM_HASH_BY_PID_AGENT: Dict[Tuple[int, str], str] = {}


@dataclass(frozen=True)
class AuditEvent:
    # Time
    ts: float
    ts_ms: int

    # Identity
    event_id: str
    boot_id: str
    pid: int

    # What happened
    event: str  # capability.accept | capability.coerce | capability.reject | capability.changed | capability.proc_switch
    endpoint: str
    agent: str
    namespace: Optional[str]

    # Shape + decision metadata
    shape: str
    reason_code: Optional[str]
    warning_codes: List[str]

    # Counts and fingerprints
    ops_count_received: Optional[int]
    ops_count_normalized: int
    raw_hash: str
    norm_hash: str

    request_id: Optional[str] = None

    # For change/proc-switch events
    prev_norm_hash: Optional[str] = None
    prev_pid: Optional[int] = None


def _now() -> float:
    return time.time()


def _stable_hash(obj: Any) -> str:
    """Hash without logging raw payloads. Avoids PII / giant blobs."""
    try:
        s = repr(obj)
    except Exception:
        s = "<unrepr>"
    s = s[:2000]
    return hashlib.sha256(s.encode("utf-8", errors="replace")).hexdigest()


def _ops_fingerprint(ops: List[str]) -> str:
    s = "\n".join(sorted(set(ops)))
    return hashlib.sha256(s.encode("utf-8", errors="replace")).hexdigest()


def _inc(event: str, endpoint: str, shape: str, reason_code: Optional[str]) -> None:
    COUNTERS[f"{event}_total|endpoint={endpoint}|reason={reason_code or ''}|shape={shape}"] += 1


def _prune_agent_state_if_needed() -> None:
    """Hard cap agent maps. Kernel rule: bounded state."""
    if len(_LAST_SEEN_TS) <= _MAX_AGENT_STATE:
        return

    # Drop oldest ~10% to amortize pruning cost.
    items = sorted(_LAST_SEEN_TS.items(), key=lambda kv: kv[1])
    drop_n = max(1, int(_MAX_AGENT_STATE * 0.1))
    to_drop = [k for k, _ in items[:drop_n]]

    for agent in to_drop:
        _LAST_SEEN_TS.pop(agent, None)
        _LAST_NORM_HASH.pop(agent, None)
        _LAST_AGENT_PID.pop(agent, None)
        _LAST_NORM_HASH_BY_PID_AGENT.pop((_PID, agent), None)


def _append_event(ev: AuditEvent) -> None:
    _EVENTS.append(ev)
    emit("CAPABILITY_AUDIT", **asdict(ev))


def _record_proc_switch(
    *,
    endpoint: str,
    agent: str,
    namespace: Optional[str],
    shape: str,
    prev_pid: int,
    norm_hash: str,
    ops_count_normalized: int,
) -> None:
    """
    Same agent is being handled by different PIDs.
    This is the canonical symptom of multi-worker split-brain for in-memory auditing.
    """
    now = _now()
    _inc("capability.proc_switch", endpoint, shape, None)

    _append_event(
        AuditEvent(
            ts=now,
            ts_ms=int(now * 1000),
            event_id=str(uuid.uuid4()),
            boot_id=_BOOT_ID,
            pid=_PID,
            event="capability.proc_switch",
            endpoint=endpoint,
            agent=agent,
            namespace=namespace,
            shape=shape,
            reason_code=None,
            warning_codes=["MULTI_PROCESS_OBSERVED"],
            ops_count_received=None,
            ops_count_normalized=int(ops_count_normalized),
            raw_hash="",
            norm_hash=norm_hash,
            request_id=None,
            prev_norm_hash=None,
            prev_pid=int(prev_pid),
        )
    )

    emit(
        "CAPABILITY_PROC_SWITCH",
        endpoint=endpoint,
        agent=agent,
        namespace=namespace,
        prev_pid=int(prev_pid),
        pid=_PID,
        boot_id=_BOOT_ID,
        norm_hash=norm_hash,
        ops_count_normalized=int(ops_count_normalized),
        ts=now,
    )


def _maybe_record_change(
    *,
    endpoint: str,
    agent: str,
    namespace: Optional[str],
    shape: str,
    norm_hash: str,
    ops_count_normalized: int,
) -> None:
    """
    Emit capability.changed when an agent's capability fingerprint changes.
    Kernel principle: no silent drift (but must detect multi-worker split-brain).
    """
    now = _now()

    # Update liveness and bounded state
    _LAST_SEEN_TS[agent] = now
    _prune_agent_state_if_needed()

    # Detect PID switching for the same agent (multi-worker symptom)
    prev_pid = _LAST_AGENT_PID.get(agent)
    if prev_pid is not None and prev_pid != _PID:
        _record_proc_switch(
            endpoint=endpoint,
            agent=agent,
            namespace=namespace,
            shape=shape,
            prev_pid=prev_pid,
            norm_hash=norm_hash,
            ops_count_normalized=ops_count_normalized,
        )
    _LAST_AGENT_PID[agent] = _PID

    # Per-process last hash (this is the only "truth" we can maintain without shared storage)
    prev = _LAST_NORM_HASH.get(agent)

    # Also maintain per-(pid,agent) fingerprint
    _LAST_NORM_HASH_BY_PID_AGENT[(_PID, agent)] = norm_hash

    if prev is None:
        # First time seeing this agent in this process; establish baseline.
        _LAST_NORM_HASH[agent] = norm_hash
        return

    if prev == norm_hash:
        return

    # It changed within this process: emit changed.
    _LAST_NORM_HASH[agent] = norm_hash
    _inc("capability.changed", endpoint, shape, None)

    emit(
        "CAPABILITY_CHANGED",
        endpoint=endpoint,
        agent=agent,
        namespace=namespace,
        prev_norm_hash=prev,
        norm_hash=norm_hash,
        ops_count_normalized=int(ops_count_normalized),
        ts=now,
        pid=_PID,
        boot_id=_BOOT_ID,
    )

    _append_event(
        AuditEvent(
            ts=now,
            ts_ms=int(now * 1000),
            event_id=str(uuid.uuid4()),
            boot_id=_BOOT_ID,
            pid=_PID,
            event="capability.changed",
            endpoint=endpoint,
            agent=agent,
            namespace=namespace,
            shape=shape,
            reason_code=None,
            warning_codes=[],
            ops_count_received=None,
            ops_count_normalized=int(ops_count_normalized),
            raw_hash="",
            norm_hash=norm_hash,
            request_id=None,
            prev_norm_hash=prev,
            prev_pid=None,
        )
    )


def record_capability_event(
    *,
    event: str,
    endpoint: str,
    agent: str,
    namespace: Optional[str],
    shape: str,
    ops_received: Optional[List[str]],
    ops_normalized: List[str],
    reason_code: Optional[str] = None,
    warning_codes: Optional[List[str]] = None,
    request_id: Optional[str] = None,
    raw_payload: Any = None,
) -> None:
    warning_codes = warning_codes or []
    now = _now()
    norm_hash = _ops_fingerprint(ops_normalized)

    # Bounded state updates (kernel rule)
    _LAST_SEEN_TS[str(agent)] = now
    _prune_agent_state_if_needed()

    _inc(event, endpoint, shape, reason_code)

    ev = AuditEvent(
        ts=now,
        ts_ms=int(now * 1000),
        event_id=str(uuid.uuid4()),
        boot_id=_BOOT_ID,
        pid=_PID,
        event=event,
        endpoint=endpoint,
        agent=str(agent),
        namespace=str(namespace) if namespace is not None else None,
        shape=str(shape),
        reason_code=reason_code,
        warning_codes=list(warning_codes),
        ops_count_received=(len(ops_received) if ops_received is not None else None),
        ops_count_normalized=len(ops_normalized),
        raw_hash=_stable_hash(raw_payload),
        norm_hash=norm_hash,
        request_id=request_id,
        prev_norm_hash=None,
        prev_pid=None,
    )
    _append_event(ev)

    # Change detection: meaningful only on accept/coerce (not reject).
    if event in ("capability.accept", "capability.coerce"):
        _maybe_record_change(
            endpoint=endpoint,
            agent=str(agent),
            namespace=str(namespace) if namespace is not None else None,
            shape=str(shape),
            norm_hash=norm_hash,
            ops_count_normalized=len(ops_normalized),
        )


@router.get("/audit/capabilities")
def get_capability_audit(
    agent: Optional[str] = Query(default=None),
    event: Optional[str] = Query(default=None),
    reason_code: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
) -> Dict[str, Any]:
    rows = list(_EVENTS)
    if agent:
        rows = [r for r in rows if r.agent == agent]
    if event:
        rows = [r for r in rows if r.event == event]
    if reason_code:
        rows = [r for r in rows if (r.reason_code or "") == reason_code]

    rows = rows[-limit:]

    # Runtime diagnostics: makes multi-process behavior obvious without needing `ps`.
    distinct_pids = sorted({r.pid for r in _EVENTS})
    proc_uptime_s = max(0.0, _now() - _PROCESS_START_TS)

    return {
        "ok": True,
        "count": len(rows),
        "limit": limit,
        "events": [asdict(r) for r in rows],
        "counters": dict(COUNTERS),
        "last_seen_ts": dict(list(_LAST_SEEN_TS.items())[:2000]),
        "runtime": {
            "boot_id": _BOOT_ID,
            "pid": _PID,
            "process_uptime_s": proc_uptime_s,
            "distinct_pids_in_buffer": distinct_pids,
            "distinct_pid_count_in_buffer": len(distinct_pids),
        },
    }
