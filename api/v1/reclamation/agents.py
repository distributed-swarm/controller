from __future__ import annotations

import time
from fastapi import APIRouter, Query

from api.v1.agents import tombstone_agent, delete_agent
from api.v1.events import publish_event
from domain.reclamation.candidates import AgentSnapshot, plan_agent_sweep
from domain.reclamation.policy import AgentReclamationPolicy

router = APIRouter(prefix="/reclamation/agents", tags=["reclamation"])


@router.post("/sweep")
def sweep_agents(dry_run: bool = Query(True)) -> dict:
    """
    Agent reclamation sweep.

    - dry_run=true  -> report only (no mutation)
    - dry_run=false -> APPLY tombstoning + deletion

    Notes:
    - We snapshot app.AGENTS items to avoid RuntimeError if the dict changes during iteration.
    - We emit SSE events when we actually mutate state (tombstone/delete).
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
            "errors": [],
            "error": "app.AGENTS not available",
        }

    # Snapshot to avoid "dictionary changed size during iteration"
    safe_items = list(agents_store.items())

    snapshots: list[AgentSnapshot] = []
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

    applied_tombstones: list[str] = []
    actually_deleted: list[str] = []
    errors: list[dict] = []

    if not dry_run:
        # 1) APPLY tombstoning
        for a in plan.to_tombstone:
            try:
                ok = tombstone_agent(a.name, tombstoned_at=now)
                if ok:
                    applied_tombstones.append(a.name)
                    publish_event("agent.tombstoned", {"name": a.name, "tombstoned_at": now})
                else:
                    errors.append({"op": "tombstone", "name": a.name, "error": "agent_not_found"})
            except Exception as e:
                errors.append({"op": "tombstone", "name": a.name, "error": str(e)})

        # 2) APPLY deletion (only for candidates the domain marked deletable)
        for a in plan.to_delete:
            try:
                ok = delete_agent(a.name)
                if ok:
                    actually_deleted.append(a.name)
                    publish_event("agent.deleted", {"name": a.name})
                else:
                    errors.append({"op": "delete", "name": a.name, "error": "agent_not_found"})
            except Exception as e:
                errors.append({"op": "delete", "name": a.name, "error": str(e)})

    return {
        "now": now,
        "policy": policy.__dict__,
        "dry_run": dry_run,
        # Always report the plan, regardless of whether apply happened.
        "tombstone_candidates": [{"name": a.name, "last_seen": a.last_seen} for a in plan.to_tombstone],
        "tombstoned": applied_tombstones,
        "identified_for_deletion": [{"name": a.name, "tombstoned_at": a.tombstoned_at} for a in plan.to_delete],
        "actually_deleted": actually_deleted,
        "errors": errors,
    }
