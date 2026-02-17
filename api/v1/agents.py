# controller/api/v1/agents.py
from __future__ import annotations

import time
from typing import Any, Dict, Tuple, List, Optional, Union

# --- NEW: canonical capability normalization + diff --------------------------

def _coerce_capabilities(raw: Any) -> Dict[str, Any]:
    """
    Canonicalize capabilities into a dict with stable 'ops': sorted unique list[str].
    Accepts:
      - None
      - list[str]  (treated as ops)
      - dict       (uses dict.get("ops", []), tolerates ops as list/tuple/set/str)
    """
    if raw is None:
        return {"ops": []}

    # list[str] => ops
    if isinstance(raw, list):
        ops_raw = raw
        ops = _coerce_ops_list(ops_raw)
        return {"ops": ops}

    if isinstance(raw, dict):
        ops_raw = raw.get("ops", [])
        ops = _coerce_ops_list(ops_raw)
        out = dict(raw)  # preserve other keys, but normalize ops
        out["ops"] = ops
        return out

    # Anything else: safest possible
    return {"ops": []}


def _coerce_ops_list(ops_raw: Any) -> List[str]:
    if ops_raw is None:
        return []
    if isinstance(ops_raw, str):
        ops = [ops_raw]
    elif isinstance(ops_raw, (list, tuple, set)):
        ops = [x for x in ops_raw]
    else:
        # unknown type -> drop
        return []

    # keep only non-empty strings
    cleaned: List[str] = []
    for x in ops:
        if isinstance(x, str):
            s = x.strip()
            if s:
                cleaned.append(s)

    # stable canonical ordering
    return sorted(set(cleaned))


def _capabilities_changed(old: Dict[str, Any], new: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    Returns (changed, diff_payload).
    Diff focuses on ops changes; includes full old/new ops for auditability.
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


# --- existing code ----------------------------------------------------------

def _best_effort_compute_state(app_mod: Any, entry: Dict[str, Any], now: float) -> Tuple[str, str]:
    ...
    # (unchanged)
    ...


def upsert_agent(
    name: str,
    labels: Dict[str, Any] | None = None,
    capabilities: Any | None = None,          # <-- accept any inbound type safely
    worker_profile: Dict[str, Any] | None = None,
    metrics: Dict[str, Any] | None = None,
) -> Tuple[str, str]:
    """
    Canonical v1 helper that keeps the in-memory AGENTS store fresh.
    Also: kernel-mode capability normalization + changed detection + event/audit emit.
    """
    import app  # type: ignore

    now = time.time()
    labels = labels or {}
    worker_profile = worker_profile or {}
    metrics = metrics or {}

    agents = getattr(app, "AGENTS", None)
    if agents is None or not isinstance(agents, dict):
        return "unknown", "agents_store_missing"

    entry = agents.get(name) or {}
    if not isinstance(entry, dict):
        entry = {}

    entry.setdefault("labels", {})
    if isinstance(entry["labels"], dict):
        entry["labels"].update(labels)

    # --- kernel-mode capability update (NEW) ---
    entry.setdefault("capabilities", {})
    old_caps = entry["capabilities"] if isinstance(entry.get("capabilities"), dict) else {}
    new_caps = _coerce_capabilities(capabilities)

    changed, diff = _capabilities_changed(old_caps, new_caps)
    entry["capabilities"] = new_caps

    # Optional: write-through convenience field for fast filtering
    entry["ops"] = new_caps.get("ops", [])

    # Emit capability.changed if needed
    if changed:
        _emit_capability_changed(name=name, labels=entry.get("labels") if isinstance(entry.get("labels"), dict) else {}, diff=diff)

    # --- remaining merges ---
    entry.setdefault("worker_profile", {})
    if isinstance(entry["worker_profile"], dict):
        entry["worker_profile"].update(worker_profile)

    entry.setdefault("metrics", {})
    if isinstance(entry["metrics"], dict):
        entry["metrics"].update(metrics)

    # Reclamation markers
    entry.setdefault("tombstoned_at", None)
    entry.setdefault("tombstone_reason", None)
    entry.setdefault("deleted_at", None)

    entry["last_seen"] = now

    state, reason = _best_effort_compute_state(app, entry, now)
    entry["state"] = state
    entry["state_reason"] = reason

    agents[name] = entry
    return state, reason


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
    try:
        from api.v1 import audit as audit_mod  # type: ignore
        fn = getattr(audit_mod, "record_capability_event", None)
        if callable(fn):
            fn(agent=name, namespace=payload["namespace"], diff=diff)
    except Exception:
        pass


def _emit_agent_event(event_type: str, payload: Dict[str, Any]) -> None:
    """
    Best-effort event emission. If events module isn’t present or publish_event
    changes, reclamation should still function.
    """
    try:
        from api.v1.events import publish_event  # type: ignore
    except Exception:
        return

    # FIX: publish_event(event_type, data) — not publish_event({type,payload})
    try:
        publish_event(event_type, payload)
    except Exception:
        return
