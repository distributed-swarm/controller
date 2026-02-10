# controller/api/v1/jobs.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()


class JobConstraints(BaseModel):
    pinned_agent: Optional[str] = Field(default=None, description="If set, only this agent may lease the job.")


class CreateJobRequest(BaseModel):
    # HARD boundary: namespace is REQUIRED
    namespace: str = Field(..., min_length=1, description="Tenant/namespace (required).")

    op: str = Field(..., min_length=1, description="Operation name (e.g. 'echo').")
    payload: Any = Field(..., description="Arbitrary JSON payload for the op.")

    # Optional caller-provided id (idempotency-ish)
    job_id: Optional[str] = Field(default=None, description="Optional job id (client-supplied).")
    id: Optional[str] = Field(default=None, description="Alias for job_id (client-supplied).")

    # Priority (maps to excitatory_level 0..3)
    priority: Optional[int] = Field(default=None, ge=0, le=3, description="0..3 (3 highest).")
    excitatory_level: Optional[int] = Field(default=None, ge=0, le=3, description="Alias for priority (0..3).")

    constraints: Optional[JobConstraints] = Field(default=None)


class CreateJobResponse(BaseModel):
    ok: bool = True
    job_id: str


@router.post("/jobs", response_model=CreateJobResponse)
def create_job(req: CreateJobRequest) -> CreateJobResponse:
    """
    v1 job submission.

    This is the missing syscall: create a job so it can be leased via POST /v1/leases.
    """
    ns = (req.namespace or "").strip()
    if not ns:
        raise HTTPException(status_code=400, detail="namespace is required")

    try:
        import app as app_mod  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Runtime module 'app' not available: {e}")

    state_lock = getattr(app_mod, "STATE_LOCK", None)
    enqueue = getattr(app_mod, "_enqueue_job", None)
    is_avail = getattr(app_mod, "_is_agent_available_for_pin", None)

    if state_lock is None or enqueue is None or is_avail is None:
        raise HTTPException(
            status_code=500,
            detail="Controller internals not available (STATE_LOCK/_enqueue_job/_is_agent_available_for_pin)",
        )

    pinned = None
    if req.constraints and req.constraints.pinned_agent:
        pinned = str(req.constraints.pinned_agent)

    # Priority mapping
    lvl = req.excitatory_level if req.excitatory_level is not None else req.priority
    if lvl is None:
        lvl = 1
    try:
        lvl_i = int(lvl)
    except (TypeError, ValueError):
        lvl_i = 1
    lvl_i = max(0, min(3, lvl_i))

    # Caller-supplied job id
    caller_job_id = req.job_id or req.id

    with state_lock:
        if pinned:
            try:
                if not bool(is_avail(pinned)):
                    raise HTTPException(
                        status_code=409,
                        detail={"error": "requested_agent_unavailable", "agent": pinned},
                    )
            except HTTPException:
                raise
            except Exception:
                # If the health subsystem is broken, be conservative and reject pinning
                raise HTTPException(status_code=409, detail={"error": "requested_agent_unavailable", "agent": pinned})

        try:
            job_id = enqueue(
                op=str(req.op),
                payload=req.payload,
                job_id=str(caller_job_id) if caller_job_id else None,
                pinned_agent=pinned,
                excitatory_level=lvl_i,
                namespace=ns,
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"enqueue failed: {type(e).__name__}: {e}")

    return CreateJobResponse(ok=True, job_id=str(job_id))
