from __future__ import annotations

import time
from typing import Any, Dict, List

from fastapi import APIRouter, Query

from api.v1.agents import delete_agent, tombstone_agent
from api.v1.events import publish_event
from domain.reclamation.candidates import AgentSnapshot, plan_agent_sweep
from domain.reclamation.policy import AgentReclamationPolicy

# IMPORTANT:
# api/v1/__init__.py imports "router" from this module, so it MUST exist.
router = APIRouter()


@router.post("/reclamation/agents/sweep")
def sweep_agents(dry_run: bool = Query(True)) -> Dict[str, Any]:
    """
    Sweep agent registry:
      - Identify stale agents -> tombstone
      - Identify tombstoned long enough -> delete
    Domain decides WHAT; this endpoint applies minimal mutations to app.AGENTS.

    dry_run=true: compute + report plan only (no mutations, no events)
    dry_run=false: apply mutations + emit SSE audit events
    """
    import app  # type: ignore

    now = time.time()
    policy = AgentReclamationPolicy()

    agents_store = getattr(app, "AGENTS", None)
    if agents_store is None or not isinstance(agents_store, dict):
        return {
            "now": now,
            "policy": policy.__dict__,
            "dry_run": dry_run,
            "tombstone_candidates": [],
            "tombstoned": [],
            "identified_for_deletion": [],
            "actually_deleted": [],
            "errors": [{"op": "init", "error": "app.AGENTS not available"}],
        }

    # Snapshot to avoid: RuntimeError: dictionary changed size during iteration
    safe_items = list(agents_store.items())

    snapshots: List[AgentSnapshot] = []
    for name, entry in safe_items:
        if not isinstance(entry, dict):
            continue
        snapshots.append(
            AgentSnapshot(
                name=name,
                last_seen=entry.get("last_seen"),
                tombstoned_at=entry.get("tombstoned_at"),
            )
        )

    plan = plan_agent_sweep(now=now, agents=snapshots, policy=policy)

    tombstoned: List[str] = []
    deleted: List[str] = []
    errors: List[Dict[str, Any]] = []

    if not dry_run:
        # APPLY: tombstone
        for a in plan.to_tombstone:
            try:
                ok = tombstone_agent(a.name, tombstoned_at=now)
                if ok:
                    tombstoned.append(a.name)
                    publish_event("agent.tombstoned", {"name": a.name, "tombstoned_at": now})
                else:
                    errors.append({"op": "tombstone", "name": a.name, "error": "agent_not_found"})
            except Exception as e:
                errors.append({"op": "tombstone", "name": a.name, "error": str(e)})

        # APPLY: delete
        for a in plan.to_delete:
            try:
                ok = delete_agent(a.name)
                if ok:
                    deleted.append(a.name)
                    publish_event("agent.deleted", {"name": a.name})
                else:
                    errors.append({"op": "delete", "name": a.name, "error": "agent_not_found"})
            except Exception as e:
                errors.append({"op": "delete", "name": a.name, "error": str(e)})

    # Always report plan (even if dry_run)
    return {
        "now": now,
        "policy": policy.__dict__,
        "dry_run": dry_run,
        "tombstone_candidates": [{"name": a.name, "last_seen": a.last_seen} for a in plan.to_tombstone],
        "tombstoned": tombstoned,
        "identified_for_deletion": [{"name": a.name, "tombstoned_at": a.tombstoned_at} for a in plan.to_delete],
        "actually_deleted": deleted,
        "errors": errors,
    }
