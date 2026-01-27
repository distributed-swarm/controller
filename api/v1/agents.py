# controller/api/v1/agents.py
from __future__ import annotations

import time
from typing import Any, Dict, Optional


def upsert_agent(
    name: str,
    labels: Dict[str, Any] | None = None,
    capabilities: Dict[str, Any] | None = None,
    worker_profile: Dict[str, Any] | None = None,
    metrics: Dict[str, Any] | None = None,
) -> None:
    """
    Canonical v1 helper that keeps the in-memory AGENTS store fresh.

    Writes to app.AGENTS (the same store legacy endpoints use),
    so scheduling/gating has one source of truth.
    """
    import app  # type: ignore

    now = time.time()
    labels = labels or {}
    capabilities = capabilities or {}
    worker_profile = worker_profile or {}
    metrics = metrics or {}

    agents = getattr(app, "AGENTS", None)
    if agents is None or not isinstance(agents, dict):
        # If app.AGENTS ever disappears, don't crash leasing paths.
        return

    entry = agents.get(name) or {}
    if not isinstance(entry, dict):
        entry = {}

    entry.setdefault("labels", {})
    if isinstance(entry["labels"], dict):
        entry["labels"].update(labels)

    entry.setdefault("capabilities", {})
    if isinstance(entry["capabilities"], dict):
        entry["capabilities"].update(capabilities)

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

    agents[name] = entry


def tombstone_agent(
    name: str,
    tombstoned_at: float | None = None,
    reason: str = "stale_heartbeat",
) -> bool:
    """
    Marks an agent as tombstoned in app.AGENTS.
    Idempotent: calling multiple times is fine.
    Returns True if the agent existed and is now tombstoned (or already was).
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
    _emit_agent_event("agent_tombstoned", {"name": name, "tombstoned_at": entry.get("tombstoned_at"), "reason": reason})
    return True


def delete_agent(name: str, deleted_at: float | None = None) -> bool:
    """
    Hard-deletes an agent from app.AGENTS.
    Returns True if the agent existed and was removed.
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
    _emit_agent_event("agent_deleted", {"name": name})
    return True


def _emit_agent_event(event_type: str, payload: Dict[str, Any]) -> None:
    """
    Best-effort event emission. If events module isn’t present or publish_event
    changes, reclamation should still function.
    """
    try:
        from api.v1.events import publish_event  # type: ignore
    except Exception:
        return

    try:
        publish_event({"type": event_type, "payload": payload})
    except Exception:
        # Don’t let observability break control-plane logic.
        return
