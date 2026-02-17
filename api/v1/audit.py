# controller/api/v1/audit.py
from __future__ import annotations

import hashlib
import time
from collections import Counter, deque
from dataclasses import dataclass, asdict
from typing import Any, Deque, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query

from lifecycle.log import emit

router = APIRouter()

# Bounded, kernel-style: never unbounded memory.
_AUDIT_MAX = 2000
_EVENTS: Deque["AuditEvent"] = deque(maxlen=_AUDIT_MAX)

# Simple counters without requiring Prometheus wiring.
COUNTERS: Counter = Counter()

# Last-seen capability fingerprint per agent (bounded by number of agents).
_LAST_NORM_HASH: Dict[str, str] = {}
_LAST_SEEN_TS: Dict[str, float] = {}


@dataclass(frozen=True)
class AuditEvent:
    ts: float
    event: str                 # capability.accept | capability.coerce | capability.reject | capability.changed
    endpoint: str              # /v1/leases
    agent: str
    namespace: Optional[str]
    shape: str                 # list | dict_ops_list | dict_ops_map | none | int | ...
    reason_code: Optional[str] # for reject
    warning_codes: List[str]   # for coerce
    ops_count_received: Optional[int]
    ops_count_normalized: int
    raw_hash: str              # hash of raw payload excerpt
    norm_hash: str             # hash of normalized ops set
    request_id: Optional[str] = None

    # only meaningful for capability.changed
    prev_norm_hash: Optional[str] = None


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


def _append_event(ev: AuditEvent) -> None:
    _EVENTS.append(ev)
    emit("CAPABILITY_AUDIT", **asdict(ev))


def _inc(event: str, endpoint: str, shape: str, reason_code: Optional[str]) -> None:
    COUNTERS[f"{event}_total|endpoint={endpoint}|reason={reason_code or ''}|shape={shape}"] += 1


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
    Emit a capability.changed event when an agent's capability fingerprint changes.
    Kernel principle: no silent drift.
    """
    now = time.time()
    prev = _LAST_NORM_HASH.get(agent)

    # Always update liveness/last seen
    _LAST_SEEN_TS[agent] = now

    if prev is None:
        # First time seeing this agent's fingerprint; record it but do not emit "changed".
        _LAST_NORM_HASH[agent] = norm_hash
        return

    if prev == norm_hash:
        return

    # It changed: emit a separate audit event + counter + log.
    _LAST_NORM_HASH[agent] = norm_hash
    _inc("capability.changed", endpoint, shape, None)

    emit(
        "CAPABILITY_CHANGED",
        endpoint=endpoint,
        agent=agent,
        namespace=namespace,
        prev_norm_hash=prev,
        norm_hash=norm_hash,
        ops_count_normalized=ops_count_normalized,
        ts=now,
    )

    _append_event(
        AuditEvent(
            ts=now,
            event="capability.changed",
            endpoint=endpoint,
            agent=str(agent),
            namespace=str(namespace) if namespace is not None else None,
            shape=str(shape),
            reason_code=None,
            warning_codes=[],
            ops_count_received=None,
            ops_count_normalized=int(ops_count_normalized),
            raw_hash="",
            norm_hash=norm_hash,
            request_id=None,
            prev_norm_hash=prev,
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
    norm_hash = _ops_fingerprint(ops_normalized)

    _inc(event, endpoint, shape, reason_code)

    ev = AuditEvent(
        ts=time.time(),
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
    )
    _append_event(ev)

    # Change detection: only meaningful on accept/coerce (not reject).
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
    return {
        "ok": True,
        "count": len(rows),
        "limit": limit,
        "events": [asdict(r) for r in rows],
        "counters": dict(COUNTERS),
        "last_seen_ts": dict(list(_LAST_SEEN_TS.items())[:2000]),
    }
