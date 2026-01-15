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
    capabilities: List[str] = Field(default_factory=list, description="Ops this agent can run (future-proofing).")
    max_tasks: int = Field(default=1, ge=1, le=64, description="Max tasks to lease in one call.")
    timeout_ms: int = Field(default=25000, ge=0, le=120000, description="Long-poll timeout in milliseconds.")


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


@router.post("/leases", response_model=LeaseResponse, responses={204: {"description": "No work available"}})
def lease_work(req: LeaseRequest) -> Response | LeaseResponse:
    """
    Lease work for an agent.

    Behavior:
    - Long-polls up to timeout_ms.
    - Tries to lease up to max_tasks jobs.
    - Returns 204 with empty body if no work is available by timeout.
    """
    # Defer import to avoid circular wiring issues while we build v1.
    try:
        from app import _lease_next_job  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Controller lease function not available: {e}")

    deadline = time.time() + (req.timeout_ms / 1000.0 if req.timeout_ms else 0.0)
    tasks: List[LeaseTask] = []

    def try_lease_once() -> Optional[Dict[str, Any]]:
        try:
            return _lease_next_job(req.agent)
        except HTTPException:
            raise
        except Exception as e:
            # If leasing fails, that's a controller bug or agent state issue.
            raise HTTPException(status_code=500, detail=f"Leasing failed: {e}")

    # Fast path: timeout_ms == 0 => one attempt and return immediately.
    if req.timeout_ms == 0:
        job = try_lease_once()
        if not job:
            return Response(status_code=204)
        tasks.append(_normalize_job(job))
        return LeaseResponse(lease_id=str(uuid.uuid4()), tasks=tasks)

    # Long-poll loop
    while True:
        while len(tasks) < req.max_tasks:
            job = try_lease_once()
            if not job:
                break
            tasks.append(_normalize_job(job))

        if tasks:
            return LeaseResponse(lease_id=str(uuid.uuid4()), tasks=tasks)

        if time.time() >= deadline:
            return Response(status_code=204)

        time.sleep(0.1)
