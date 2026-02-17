# controller/api/v1/agents.py
from __future__ import annotations

import time
from typing import Any, Dict, Tuple, List


# -----------------------------------------------------------------------------
# Capability "kernel mode": canonical coercion + diff
# -----------------------------------------------------------------------------

def _coerce_ops_list(ops_raw: Any) -> List[str]:
    """
    Coerce ops input into a stable, sorted, unique list[str].
    Accepts:
      - None
      - str
      - list/tuple/set of strings
    """
    if ops_raw is None:
        return []

    if isinstance(ops_raw, str):
        ops = [ops_raw]
    elif isinstance(ops_raw, (list, tuple, set)):
        ops = list(ops_raw)
    else:
        return []

    cleaned: List[str] = []
    for x in ops:
        if isinstance(x, str):
            s = x.strip()
            if s:
                cleaned.append(s)

    return sorted(set(cleaned))


def _coerce_capabilities(raw: Any) -> Dict[str, Any]:
    """
    Canonicalize capabilities into a dict with stable 'ops': sorted unique list[str].

    Accepts inbound forms:
      - None -> {"ops": []}
      - list[str] -> {"ops": [...]}
      - dict -> preserves other keys, but normalizes "ops"
    """
    if raw is None:
        return {"ops": []}

    # list[str] => ops
    if isinstance(raw, list):
        return {"ops": _coerce_ops_list(raw)}

    if isinstance(raw, dict):
        out = dict(raw)
        out["ops"] = _coerce_ops_list(raw.get("ops", []))
        return out

    # Anything else: safest possible
    return {"ops": []}


def _capabilities_changed(old: Dict[str, Any], new: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    Determine whether capabilities changed (focused on ops changes).
    Returns (changed, diff_payload).
    """
    old_ops = old.get("ops") if isinstance(old.get("ops"), list) else []
    new_ops = new.get("ops") if isinstance(new.get("ops"), list) else []

    old_set = set([x for x in old_ops if isinstance(x, str)])
    new_set = set([x for x in new_ops if isinstance(x, str)])

    added = sorted(new_set - old_set)
    removed = sorted(old_set - new_set)

    changed = bool(added or removed)
    diff = {
        "ops_added": added,
        "ops_removed": removed,
        "old_ops": sorted(old_set),
        "new_ops": sorted(new_set),
    }
    return changed, diff


def _emit_capability_changed(name: str, labels: Dict[str, Any], diff: Dict[str, Any]) -> None:
    """
    Emits the canonical capability.changed event and (best-effort) records to audit module if present.
    Never breaks control-plane behavior.
    """
    payload = {
        "agent": name,
        "namespace": labels.get("namespace", "default"),
        "diff": diff,
    }

    # 1) publish_event (best-effort)
    try:
        from api.v1.events import publish_event  # type: ignore
        publish_event("capability.changed", payload)
    except Exception:
        pass

    # 2) audit hook (best-effort)
    # NOTE: We do not assume audit module shape; we just try.
    try:
        from api.v1 import audit as audit_mod  # type: ignore
        fn = getattr(audit_mod, "record_capability_event", None)
        if callable(fn):
            # Use keyword args so audit module can evolve without breaking callers.
            fn(agent=name, namespace=payload["namespace"], diff=diff)
    except Exception:
        pass


# -----------------------------------------------------------------------------
# Agent state compute (existing behavior retained)
# -----------------------------------------------------------------------------

def _best_effort_compute_state(app_mod: Any, entry: Dict[str, Any], now: float) -> Tuple[str, str]:
    """
    Compute (state, reason) using controller's canonical logic if present.
    Falls back safely if not available.
    """
    # Prefer the exact helper used elsewhere in app.py
    fn = getattr(app_mod, "_compute_agent_state", None)
    if callable(fn):
        try:
            state, reason = fn(entry, now)  # expected signature: (entry, now) -> (state, reason)
            if isinstance(state, str) and isinstance(reason, str):
                return state, reason
        except TypeError:
            # Signature mismatch; try alternate common shapes
            try:
                state, reason = fn(entry)  # (entry) -> (state, reason)
                if isinstance(state, str) and isinstance(reason, str):
                    return state, reason
            except Exception:
                pass
        except Exception:
            pass

    # Alternate name (just in case)
    fn2 = getattr(app_mod, "compute_agent_state", None)
    if callable(fn2):
        try:
            state, reason = fn2(entry, now)
            if isinstance(state, str) and isinstance(reason, str):
                return state, reason
        except Exception:
            pass

    # Safe fallback
    return "healthy", "no_state_fn"


# -----------------------------------------------------------------------------
# Public helpers used by v1 endpoints
# -----------------------------------------------------------------------------

def upsert_agent(
    name: str,
    labels: Dict[str, Any] | None = None,
    capabilities: Any | None = None,          # accept list|dict|None safely
    worker_profile: Dict[str, Any] | None = None,
    metrics: Dict[str, Any] | None = None,
) -> Tuple[str, str]:
    """
    Canonical v1 helper that keeps the in-memory AGENTS store fresh.

    Writes to app.AGENTS (the same store legacy endpoints use),
    so scheduling/gating has one source of truth.

    Also: kernel-mode capability normalization + changed detection + event/audit emit.

    Returns (state, reason) computed via the controller's canonical state logic.
    """
    import app  # type: ignore

    now = time.time()
    labels = labels or {}
    worker_profile = worker_profile or {}
    metrics = metrics or {}

    agents = getattr(app, "AGENTS", None)
    if agents is None or not isinstance(agents, dict):
        # If app.AGENTS ever disappears, don't crash leasing paths.
        return "unknown", "agents_store_missing"

    entry = agents.get(name) or {}
    if not isinstance(entry, dict):
        entry = {}

    entry.setdefault("labels", {})
    if isinstance(entry["labels"], dict):
        entry["labels"].update(labels)

    # --- kernel-mode capability update (canonical) ---
    entry.setdefault("capabilities", {})
    old_caps = entry["capabilities"] if isinstance(entry.get("capabilities"), dict) else {"ops": []}
    new_caps = _coerce_capabilities(capabilities)

    changed, diff = _capabilities_changed(old_caps, new_caps)
    entry["capabilities"] = new_caps

    # Optional convenience for filtering/debugging (kept in sync)
    entry["ops"] = new_caps.get("ops", [])

    if changed:
        lbls = entry.get("labels") if isinstance(entry.get("labels"), dict) else {}
        _emit_capability_changed(name=name, labels=lbls, diff=diff)

    entry.setdefault("worker_profile", {})
    if isinstance(entry["worker_profile"], dict):
        entry["worker_profile"].update(worker_profile)

    entry.setdefault("metrics", {})
    if isinstance(entry["metrics"], dict):
        entry["metrics"].update(metrics)

    # Reclamation uses these.
    entry.setdefault("tombstoned_at", None)
    entry.setdefault("tombstone_reason", None)
    entry.setdefault("deleted_at", None)

    # Liveness marker.
    entry["last_seen"] = now

    # Compute and store health state (best-effort, canonical if present).
    state, reason = _best_effort_compute_state(app, entry, now)
    entry["state"] = state
    entry["state_reason"] = reason

    agents[name] = entry
    return state, reason


def tombstone_agent(
    name: str,
    tombstoned_at: float | None = None,
    reason: str = "stale_heartbeat",
    emit_type: str = "agent_tombstoned",
) -> bool:
    """
    Marks an agent as tombstoned in app.AGENTS.
    Idempotent: calling multiple times is fine.
    Returns True if the agent existed and is now tombstoned (or already was).

    emit_type allows API delete instrumentation to emit AGENT_TOMBSTONED_BY_API
    without changing reaper behavior.
    """
    import app  # type: ignore

    agents = getattr(app, "AGENTS", None)
    if agents is None or not isinstance(agents, dict):
        return False

    entry = agents.get(name)
    if not isinstance(entry, dict):
        return False

    ts = tombstoned_at if tombstoned_at is not None else time.time()

    # Only set tombstoned_at if not already set (keeps original timestamp stable)
    if entry.get("tombstoned_at") is None:
        entry["tombstoned_at"] = ts

    # Always keep the latest reason (cheap debugging value)
    entry["tombstone_reason"] = reason

    agents[name] = entry
    _emit_agent_event(
        emit_type,
        {"name": name, "tombstoned_at": entry.get("tombstoned_at"), "reason": reason},
    )
    return True


def delete_agent(
    name: str,
    deleted_at: float | None = None,
    emit_type: str = "agent_deleted",
) -> bool:
    """
    Hard-deletes an agent from app.AGENTS.
    Returns True if the agent existed and was removed.

    emit_type allows API delete instrumentation to emit AGENT_DELETED_BY_API
    without changing reaper behavior.
    """
    import app  # type: ignore

    agents = getattr(app, "AGENTS", None)
    if agents is None or not isinstance(agents, dict):
        return False

    if name not in agents:
        return False

    # Optional: record deletion time before removal (best-effort)
    entry = agents.get(name)
    if isinstance(entry, dict):
        entry["deleted_at"] = deleted_at if deleted_at is not None else time.time()

    agents.pop(name, None)
    _emit_agent_event(emit_type, {"name": name})
    return True


def _emit_agent_event(event_type: str, payload: Dict[str, Any]) -> None:
    """
    Best-effort event emission. If events module isn’t present or publish_event
    changes, reclamation should still function.

    FIXED: publish_event(event_type, payload) — previous code passed a dict wrapper.
    """
    try:
        from api.v1.events import publish_event  # type: ignore
    except Exception:
        return

    try:
        publish_event(event_type, payload)
    except Exception:
        # Don’t let observability break control-plane logic.
        return
