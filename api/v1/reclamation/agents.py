# controller/api/v1/reclamation/agents.py
from __future__ import annotations

import time
from typing import Any, Dict, List

from fastapi import APIRouter, Query

from api.v1.agents import delete_agent, tombstone_agent
from domain.reclamation.policy import AgentReclamationPolicy
from domain.reclamation.state_machine import AgentSnapshot, plan_agent_sweep

router = APIRouter(prefix="/reclamation/agents")


@router.post("/sweep")
def sweep_agents(dry_run: bool = Query(True)) -> dict:
    """
    Sweep agents based on last_seen/tombstoned_at.
    dry_run=True returns the plan without mutating state.
    """
    import app  # type: ignore

    now = time.time()
    policy = AgentReclamationPolicy()

    agents_store = getattr(app, "AGENTS", None)
    if agents_store is None or not isinstance(agents_store, dict):
        return {
            "now": now,
            "dry_run": dry_run,
            "error": "AGENTS store missing or invalid",
            "tombstone_candidates": [],
            "tombstoned": [],
            "identified_for_deletion": [],
            "actually_deleted": [],
            "errors": [{"op": "sweep", "error": "agents_store_unavailable"}],
        }

    # Snapshot keys/values to avoid RuntimeError if store mutates while iterating.
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

    applied_tombstones: List[Dict[str, Any]] = []
    actually_deleted: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    if not dry_run:
        # APPLY tombstones
        for a in plan.to_tombstone:
            try:
                ok = tombstone_agent(a.name, tombstoned_at=now, reason="stale_heartbeat")
                if ok:
                    applied_tombstones.append({"name": a.name, "last_seen": a.last_seen})
                else:
                    errors.append({"op": "tombstone", "name": a.name, "error": "agent_not_found"})
            except Exception as e:
                errors.append({"op": "tombstone", "name": a.name, "error": str(e)})

        # APPLY deletes (hard delete). If you want “identify only”, keep dry_run=True.
        for a in plan.to_delete:
            try:
                ok = delete_agent(a.name, deleted_at=now)
                if ok:
                    actually_deleted.append({"name": a.name, "tombstoned_at": a.tombstoned_at})
                else:
                    errors.append({"op": "delete", "name": a.name, "error": "agent_not_found"})
            except Exception as e:
                errors.append({"op": "delete", "name": a.name, "error": str(e)})

    return {
        "now": now,
        "policy": getattr(policy, "__dict__", {}),
        "dry_run": dry_run,
        # Always report the plan (candidates) regardless of apply.
        "tombstone_candidates": [{"name": a.name, "last_seen": a.last_seen} for a in plan.to_tombstone],
        "tombstoned": applied_tombstones if not dry_run else [{"name": a.name, "last_seen": a.last_seen} for a in plan.to_tombstone],
        "identified_for_deletion": [{"name": a.name, "tombstoned_at": a.tombstoned_at} for a in plan.to_delete],
        "actually_deleted": actually_deleted,
        "errors": errors,
    }
