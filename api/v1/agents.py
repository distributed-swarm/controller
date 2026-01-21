# controller/api/v1/agents.py
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
    # Deferred import to avoid circulars at import time.
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
    entry.setdefault("labels", {})
    entry["labels"].update(labels)

    entry.setdefault("capabilities", {})
    # capabilities should look like {"ops": [...]}
    entry["capabilities"].update(capabilities)

    entry.setdefault("worker_profile", {})
    entry["worker_profile"].update(worker_profile)

    entry.setdefault("metrics", {})
    entry["metrics"].update(metrics)

    entry["last_seen"] = now
    # Preserve any existing fields like warmup_until_ts, health_window, etc.
    agents[name] = entry
