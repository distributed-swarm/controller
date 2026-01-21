# controller/api/v1/leases.py
from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field

router = APIRouter()


class LeaseRequest(BaseModel):
    agent: str = Field(..., description="Agent name/id (e.g. 'cpu-1').")
    capabilities: List[str] = Field(
        default_factory=list,
        description="Ops this agent can run (future-proofing).",
    )
    max_tasks: int = Field(
        default=1,
        ge=1,
        le=64,
        description="Max tasks to lease in one call.",
    )
    timeout_ms: int = Field(
        default=25000,
        ge=0,
        le=120000,
        description="Long-poll timeout in milliseconds.",
    )


class LeaseTask(BaseModel):
    job_id: str
    op: str
    # Back-compat: payload may be any JSON value, not only dicts.
    payload: Any = Field(default_factory=dict)
    pinned_agent: Optional[str] = None


class LeaseResponse(BaseModel):
    lease_id: str
    tasks: List[LeaseTask]


def _normalize_job(job: Dict[str, Any]) -> LeaseTask:
    job_id = job.get("job_id") or job.get("id") or job.get("jobId")
    op = job.get("op") or job.get("type") or job.get("job_type") or job.get("jobType")

    if not job_id or not op:
        raise HTTPException(status_code=500, detail=f"Invalid leased job shape: {job}")

    return LeaseTask(
        job_id=str(job_id),
        op=str(op),
        payload=job.get("payload"),
        pinned_agent=job.get("pinned_agent") or job.get("pinnedAgent"),
    )


def _job_allows_agent(job: Dict[str, Any], agent_name: str) -> bool:
    """
    Ensure pinned jobs never get returned to the wrong agent.
    (This is a safety net in case the underlying leasing function is permissive.)
    """
    pinned = job.get("pinned_agent") or job.get("pinnedAgent")
    if pinned and str(pinned) != str(agent_name):
        return False
    return True


def _publish_job_leased_event(job: Dict[str, Any], lease_id: str, agent: str) -> None:
    """
    Best-effort: emit job.leased to SSE. This must never break leasing.
    """
    try:
        from api.v1.events import publish_event  # deferred to avoid circular imports
    except Exception:
        return

    job_id = job.get("job_id") or job.get("id") or job.get("jobId")
    op = job.get("op") or job.get("type") or job.get("job_type") or job.get("jobType")
    pinned = job.get("pinned_agent") or job.get("pinnedAgent")

    try:
        publish_event(
            "job.leased",
            {
                "job_id": str(job_id) if job_id is not None else None,
                "lease_id": str(lease_id),
                "agent": str(agent),
                "op": str(op) if op is not None else None,
                "pinned_agent": str(pinned) if pinned is not None else None,
            },
        )
    except Exception:
        # Never fail the lease call because SSE/event plumbing had a bad day.
        return

def _upsert_agent_from_lease(req: LeaseRequest) -> None:
    """
    Minimal agent bookkeeping from /v1/leases.
    This is what makes /v1/agents usable and enables pinned_agent routing later.
    """
    try:
        # Prefer the canonical v1 agent store helper if present.
        from api.v1.agents import upsert_agent  # type: ignore
    except Exception:
        # If the agents module isn't available (or name differs), don't break leasing.
        return

    upsert_agent(
        name=req.agent,
        labels={},
        capabilities={"ops": list(req.capabilities or [])},
        worker_profile=req.worker_profile or {},
        metrics=req.metrics or {},
    )

@router.post("/leases", response_model=LeaseResponse, responses={204: {"description": "No work available"}})
def lease_work(req: LeaseRequest) -> Response | LeaseResponse:
    """
    Lease work for an agent.

    Behavior:
    - Long-polls up to timeout_ms.
    - Tries to lease up to max_tasks jobs.
    - Returns 204 with empty body if no work is available by timeout.
    """
    _upsert_agent_from_lease(req)

    # Defer import to avoid circular wiring issues while we build v1.
    try:
        from app import _lease_next_job  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Controller lease function not available: {e}")

    deadline = time.time() + (req.timeout_ms / 1000.0 if req.timeout_ms else 0.0)
    tasks: List[LeaseTask] = []
    lease_id = str(uuid.uuid4())

    def try_lease_once() -> Optional[Dict[str, Any]]:
        try:
            # Note: capabilities currently unused here; underlying scheduler may evolve.
            return _lease_next_job(req.agent)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Leasing failed: {e}")

    def drain_available_work_once() -> None:
        """
        Attempt to lease up to max_tasks immediately (no sleeping inside).
        Applies pinned-agent safety check.
        """
        while len(tasks) < req.max_tasks:
            job = try_lease_once()
            if not job:
                break

            # Safety: never hand a pinned job to the wrong agent.
            if not _job_allows_agent(job, req.agent):
                # If underlying leasing already marked it leased, that's a bug upstream.
                # We refuse to return it to the wrong agent and keep polling.
                break

            # Normalize and append
            tasks.append(_normalize_job(job))

            # Emit SSE event (best-effort)
            _publish_job_leased_event(job, lease_id=lease_id, agent=req.agent)

    # Fast path: timeout_ms == 0 => one attempt and return immediately.
    if req.timeout_ms == 0:
        drain_available_work_once()
        if not tasks:
            return Response(status_code=204)
        return LeaseResponse(lease_id=lease_id, tasks=tasks)

    # Long-poll loop
    while True:
        drain_available_work_once()
        if tasks:
            return LeaseResponse(lease_id=lease_id, tasks=tasks)

        if time.time() >= deadline:
            return Response(status_code=204)

        time.sleep(0.1)
