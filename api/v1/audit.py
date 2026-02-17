# controller/api/v1/audit.py
from __future__ import annotations

import time
import hashlib
from collections import Counter, deque
from dataclasses import dataclass, asdict
from typing import Any, Deque, Dict, List, Optional

from fastapi import APIRouter, Query

from lifecycle.log import emit

router = APIRouter()

# Keep it bounded. Kernel-style: no unbounded memory growth.
_AUDIT_MAX = 2000

# "metrics" without depending on Prometheus wiring.
COUNTERS: Counter = Counter()

@dataclass(frozen=True)
class AuditEvent:
    ts: float
    event: str                 # "capability.accept" | "capability.coerce" | "capability.reject"
    endpoint: str              # "/v1/leases"
    agent: str
    namespace: Optional[str]
    shape: str                 # received shape category
    reason_code: Optional[str] # for reject/coerce
    warning_codes: List[str]   # for coerce
    ops_count_received: Optional[int]
    ops_count_normalized: int
    raw_hash: str              # hash of raw payload excerpt
    norm_hash: str             # hash of normalized ops set
    request_id: Optional[str] = None

_EVENTS: Deque[AuditEvent] = deque(maxlen=_AUDIT_MAX)


def _stable_hash(obj: Any) -> str:
    """Hash without logging raw payloads. Avoids PII / giant blobs."""
    try:
        s = repr(obj)
    except Exception:
        s = "<unrepr>"
    # Truncate so absurd payloads don't explode memory.
    s = s[:2000]
    return hashlib.sha256(s.encode("utf-8", errors="replace")).hexdigest()


def _ops_fingerprint(ops: List[str]) -> str:
    s = "\n".join(sorted(set(ops)))
    return hashlib.sha256(s.encode("utf-8", errors="replace")).hexdigest()


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

    COUNTERS[f"{event}_total|endpoint={endpoint}|reason={reason_code or ''}|shape={shape}"] += 1

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
        norm_hash=_ops_fingerprint(ops_normalized),
        request_id=request_id,
    )
    _EVENTS.append(ev)

    # One structured log line (your existing system can ingest this)
    emit(
        "CAPABILITY_AUDIT",
        **asdict(ev),
    )


@router.get("/audit/capabilities")
def get_capability_audit(
    agent: Optional[str] = Query(default=None),
    event: Optional[str] = Query(default=None),
    reason_code: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
) -> Dict[str, Any]:
    """
    Query recent capability audit events (in-memory ring buffer).
    """
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
    }
