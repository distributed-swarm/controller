# controller/api/v1/heartbeat.py
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from lifecycle.lifecycle_log import lifecycle

from api.v1.agents import upsert_agent, tombstone_agent, delete_agent

router = APIRouter(prefix="/agents", tags=["agents"])


class HeartbeatRequest(BaseModel):
    """
    "Magic rock" (yes, really):

    This endpoint exists because *liveness is a control-plane invariant*.
    We cannot infer liveness from /v1/leases alone because healthy agents may be idle.
    Reclamation/expiry/HA correctness depends on an explicit, periodic signal.

    Contract:
    - Agent sends POST /v1/agents/heartbeat every N seconds.
    - Controller updates app.AGENTS[name].last_seen (and merges metadata).
    - Reclamation uses last_seen/tombstoned_at for tombstone->delete progression.
    """
    name: str
    labels: Optional[Dict[str, Any]] = None
    capabilities: Optional[Dict[str, Any]] = None
    worker_profile: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, Any]] = None


@router.post("/heartbeat")
def heartbeat(req: HeartbeatRequest) -> Dict[str, Any]:
    upsert_agent(
        name=req.name,
        labels=req.labels,
        capabilities=req.capabilities,
        worker_profile=req.worker_profile,
        metrics=req.metrics,
    )

    lifecycle(
        "agent_seen",
        agent=req.name,
        namespace=(req.labels or {}).get("namespace", "default"),
    )

    return {"ok": True}


@router.delete("/{name}")
def delete_agent_v1(
    name: str,
    namespace: str = Query(..., description="Namespace (required)"),
) -> Dict[str, Any]:
    """
    Idempotent agent delete with namespace fencing.

    Route mounts under /v1, so full path is:
      DELETE /v1/agents/{name}?namespace=...

    Behavior:
    - If agent not found OR namespace mismatch: ok=true, deleted=false
    - If found: tombstone (best-effort) then hard delete: ok=true, deleted=true

    Emits lifecycle events for endpoint instrumentation (#5).
    """
    lifecycle("agent_delete_requested", agent=name, namespace=namespace)

    # Enforce namespace fencing before mutating anything.
    try:
        import app  # type: ignore
        agents = getattr(app, "AGENTS", None)
        if agents is None or not isinstance(agents, dict):
            lifecycle("agent_delete_failed", agent=name, namespace=namespace, error="agents_store_missing")
            raise HTTPException(status_code=500, detail="AGENTS store missing")

        entry = agents.get(name)
        if not isinstance(entry, dict):
            lifecycle("agent_delete_not_found", agent=name, namespace=namespace)
            return {"ok": True, "deleted": False}

        labels = entry.get("labels") if isinstance(entry.get("labels"), dict) else {}
        if labels.get("namespace") != namespace:
            # Do not leak existence across namespaces.
            lifecycle("agent_delete_not_found", agent=name, namespace=namespace)
            return {"ok": True, "deleted": False}

        # Tombstone first (best-effort, keeps reaper alignment and gives observability).
        try:
            # If tombstone_agent supports emit_type, use it; otherwise fall back.
            tombstone_agent(name, reason="api_delete", emit_type="AGENT_TOMBSTONED_BY_API")  # type: ignore[arg-type]
        except TypeError:
            tombstone_agent(name, reason="api_delete")

        # Hard delete (use emit_type override if available).
        try:
            ok = delete_agent(name, emit_type="AGENT_DELETED_BY_API")  # type: ignore[arg-type]
        except TypeError:
            ok = delete_agent(name)

        if not ok:
            # Race/idempotency: treat as not found.
            lifecycle("agent_delete_not_found", agent=name, namespace=namespace)
            return {"ok": True, "deleted": False}

        return {"ok": True, "deleted": True}

    except HTTPException:
        raise
    except Exception as e:
        lifecycle("agent_delete_failed", agent=name, namespace=namespace, error=str(e))
        raise
