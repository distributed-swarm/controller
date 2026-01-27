from __future__ import annotations

import time
from typing import Any, Dict


def upsert_agent(
    name: str,
    labels: Dict[str, Any] | None = None,
    capabilities: Dict[str, Any] | None = None,
    worker_profile: Dict[str, Any] | None = None,
    metrics: Dict[str, Any] | None = None,
) -> None:
    """
    Canonical v1 helper used by /v1/leases to keep the in-memory AGENTS store fresh.

    This intentionally writes to app.AGENTS (the same store legacy endpoints use),
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
        return

    entry = agents.get(name) or {}
    entry.setdefault("labels", {})
    entry.setdefault("capabilities", {})
    entry.setdefault("worker_profile", {})
    entry.setdefault("metrics", {})
    entry.setdefault("tombstoned_at", None)

    entry["labels"].update(labels)
    entry["capabilities"].update(capabilities)
    entry["worker_profile"].update(worker_profile)
    entry["metrics"].update(metrics)

    # liveness
    entry["last_seen"] = now

    agents[name] = entry


def tombstone_agent(name: str, tombstoned_at: float) -> bool:
    """
    Apply-phase store op: mark an agent as tombstoned.
    Idempotent: if already tombstoned, returns True.
    """
    import app  # type: ignore

    agents = getattr(app, "AGENTS", None)
    if agents is None or not isinstance(agents, dict):
        return False

    entry = agents.get(name)
    if not isinstance(entry, dict):
        return False

    if entry.get("tombstoned_at") is not None:
        return True

    entry["tombstoned_at"] = tombstoned_at
    return True


def delete_agent(name: str) -> bool:
    """
    Apply-phase store op: hard-delete an agent record.
    Returns True if it existed.
    """
    import app  # type: ignore

    agents = getattr(app, "AGENTS", None)
    if agents is None or not isinstance(agents, dict):
        return False

    return agents.pop(name, None) is not None
