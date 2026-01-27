from __future__ import annotations

import time
from fastapi import APIRouter, Query

from domain.reclamation.policy import AgentReclamationPolicy
from domain.reclamation.candidates import AgentSnapshot, plan_agent_sweep

router = APIRouter(prefix="/reclamation/agents", tags=["reclamation"])


@router.post("/sweep")
def sweep_agents(dry_run: bool = Query(True)) -> dict:
    """
    Dry-run agent reclamation sweep.

    Reads current app.AGENTS, computes who *would* be tombstoned/deleted, and returns the plan.
    This file does NOT mutate state yet (mutation comes next step).
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
            "tombstoned": [],
            "deleted": [],
            "error": "app.AGENTS not available",
        }

    snapshots = []
    for name, entry in agents_store.items():
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

    return {
        "now": now,
        "policy": policy.__dict__,
        "dry_run": dry_run,
        "tombstoned": [{"name": a.name, "last_seen": a.last_seen} for a in plan.to_tombstone],
        "deleted": [{"name": a.name, "tombstoned_at": a.tombstoned_at} for a in plan.to_delete],
    }
