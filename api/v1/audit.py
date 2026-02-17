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
_AUDIT_MAX = 2000                # bounded event buffer
_MAX_AGENT_STATE = 5000          # bounded agent maps

_BOOT_ID = str(uuid.uuid4())
_PID = os.getpid()
_PROCESS_START_TS = time.time()

_EVENTS: Deque["AuditEvent"] = deque(maxlen=_AUDIT_MAX)
COUNTERS: Counter = Counter()

# Per-agent kernel state (per process)
_LAST_NORM_HASH: Dict[str, str] = {}
_LAST_SEEN_TS: Dict[str, float] = {}
_LAST_AGENT_PID: Dict[str, int] = {}

# guard against recursion when we append derived events (capability.changed/proc_switch)
_APPENDING_DERIVED = False


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


def _emit_proc_switch(ev: AuditEvent, prev_pid: int) -> None:
    now = _now()
    _inc("capability.proc_switch", ev.endpoint, ev.shape, None)

    derived = AuditEvent(
        ts=now,
        ts_ms=int(now * 1000),
        event_id=str(uuid.uuid4()),
        boot_id=_BOOT_ID,
        pid=_PID,
        event="capability.proc_switch",
        endpoint=ev.endpoint,
        agent=ev.agent,
        namespace=ev.namespace,
        shape=ev.shape,
        reason_code=None,
        warning_codes=["MULTI_PROCESS_OBSERVED"],
        ops_count_received=None,
        ops_count_normalized=int(ev.ops_count_normalized),
        raw_hash="",
        norm_hash=ev.norm_hash,
        request_id=ev.request_id,
        prev_norm_hash=None,
        prev_pid=int(prev_pid),
    )

    emit(
        "CAPABILITY_PROC_SWITCH",
        endpoint=ev.endpoint,
        agent=ev.agent,
        namespace=ev.namespace,
        prev_pid=int(prev_pid),
        pid=_PID,
        boot_id=_BOOT_ID,
        norm_hash=ev.norm_hash,
        ops_count_normalized=int(ev.ops_count_normalized),
        ts=now,
    )

    _append_event(derived)


def _emit_changed(ev: AuditEvent, prev_norm_hash: str) -> None:
    now = _now()
    _inc("capability.changed", ev.endpoint, ev.shape, None)

    emit(
        "CAPABILITY_CHANGED",
        endpoint=ev.endpoint,
        agent=ev.agent,
        namespace=ev.namespace,
        prev_norm_hash=prev_norm_hash,
        norm_hash=ev.norm_hash,
        ops_count_normalized=int(ev.ops_count_normalized),
        ts=now,
        pid=_PID,
        boot_id=_BOOT_ID,
    )

    derived = AuditEvent(
        ts=now,
        ts_ms=int(now * 1000),
        event_id=str(uuid.uuid4()),
        boot_id=_BOOT_ID,
        pid=_PID,
        event="capability.changed",
        endpoint=ev.endpoint,
        agent=ev.agent,
        namespace=ev.namespace,
        shape=ev.shape,
        reason_code=None,
        warning_codes=[],
        ops_count_received=None,
        ops_count_normalized=int(ev.ops_count_normalized),
        raw_hash="",
        norm_hash=ev.norm_hash,
        request_id=ev.request_id,
        prev_norm_hash=prev_norm_hash,
        prev_pid=None,
    )

    _append_event(derived)


def _kernel_enforce_change_detection(ev: AuditEvent) -> None:
    """
    Kernel chokepoint enforcement:
    - for accept/coerce, update baseline and emit capability.changed if fingerprint changes
    - also detect same-agent PID switching (multi-worker symptom)
    """
    # Only apply to primary events, never derived ones.
    if ev.event not in ("capability.accept", "capability.coerce"):
        return

    agent = ev.agent
    now = _now()

    _LAST_SEEN_TS[agent] = now
    _prune_agent_state_if_needed()

    # Detect PID switch for same agent (should not happen in single-process mode).
    prev_pid = _LAST_AGENT_PID.get(agent)
    if prev_pid is not None and prev_pid != _PID:
        _emit_proc_switch(ev, prev_pid)
    _LAST_AGENT_PID[agent] = _PID

    prev = _LAST_NORM_HASH.get(agent)
    if prev is None:
        # baseline
        _LAST_NORM_HASH[agent] = ev.norm_hash
        return

    if prev == ev.norm_hash:
        return

    # changed
    _LAST_NORM_HASH[agent] = ev.norm_hash
    _emit_changed(ev, prev)


def _append_event(ev: AuditEvent) -> None:
    """
    Single chokepoint:
    - appends event
    - emits logs
    - enforces change detection for accept/coerce (kernel invariant)
    """
    global _APPENDING_DERIVED

    # Always count the primary event.
    _inc(ev.event, ev.endpoint, ev.shape, ev.reason_code)

    _EVENTS.append(ev)
    emit("CAPABILITY_AUDIT", **asdict(ev))

    # Prevent recursion when we append derived events.
    if _APPENDING_DERIVED:
        return

    # Derived events should not trigger more derived events.
    if ev.event in ("capability.changed", "capability.proc_switch"):
        return

    # Kernel enforcement runs here, not in callers.
    _APPENDING_DERIVED = True
    try:
        _kernel_enforce_change_detection(ev)
    finally:
        _APPENDING_DERIVED = False


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
    """
    Public API used by lease boundary:
    record a capability event. Change detection is enforced at the append chokepoint.
    """
    warning_codes = warning_codes or []
    now = _now()
    norm_hash = _ops_fingerprint(ops_normalized)

    ev = AuditEvent(
        ts=now,
        ts_ms=int(now * 1000),
        event_id=str(uuid.uuid4()),
        boot_id=_BOOT_ID,
        pid=_PID,
        event=str(event),
        endpoint=str(endpoint),
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
