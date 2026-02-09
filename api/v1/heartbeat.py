# controller/api/v1/heartbeat.py
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from api.v1.agents import upsert_agent

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
    return {"ok": True}
