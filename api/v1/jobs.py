# controller/api/v1/jobs.py
from __future__ import annotations

import threading
import uuid
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()


# --- In-memory idempotency cache (good enough for now; DB later) ---
# Maps idempotency_key -> job_id
_IDEMPOTENCY: Dict[str, str] = {}
_IDEMPOTENCY_LOCK = threading.Lock()


class JobSubmitRequest(BaseModel):
    op: str = Field(..., description="Operation name (e.g. 'echo', 'hailo_infer', 'map_summarize').")
    payload: Any = Field(default_factory=dict, description="Operation payload (JSON object).")

    # Optional scheduling knobs
    constraints: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Scheduling constraints (optional). Stored/passed through for now.",
    )
    priority: Optional[int] = Field(
        default=1,
        ge=0,
        le=3,
        description="Priority 0..3 (maps to controller excitatory_level).",
    )
    pinned_agent: Optional[str] = Field(
        default=None,
        description="If set, only this agent may lease the job.",
    )

    # Optional safety knob for flaky clients / retries
    idempotency_key: Optional[str] = Field(
        default=None,
        description="If provided, retries with the same key return the same job_id.",
    )


class JobSubmitResponse(BaseModel):
    job_id: str


@router.post("/jobs", response_model=JobSubmitResponse)
def submit_job(req: JobSubmitRequest) -> JobSubmitResponse:
    """
    Submit a job into the controller.

    Logic:
    - If idempotency_key is provided and we've seen it, return the same job_id.
    - Otherwise generate a job_id and enqueue the job via the controller's existing enqueue function.
    """
    # Idempotency: same key => same job_id
    if req.idempotency_key:
        with _IDEMPOTENCY_LOCK:
            existing = _IDEMPOTENCY.get(req.idempotency_key)
            if existing:
                return JobSubmitResponse(job_id=existing)

    job_id = str(uuid.uuid4())

    # Defer importing controller internals to avoid circular imports until we wire everything.
    try:
        # Expectation: controller exposes _enqueue_job(op, payload, job_id=None, pinned_agent=None, excitatory_level=1)
        from app import _enqueue_job  # type: ignore
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Runtime enqueue function not available: {e}",
        )

    # Pass constraints through for now (store inside payload so it's not lost)
    payload = dict(req.payload or {})
    if req.constraints is not None:
        payload.setdefault("_constraints", req.constraints)

    try:
        _enqueue_job(
            op=req.op,
            payload=payload,
            job_id=job_id,
            pinned_agent=req.pinned_agent,
            excitatory_level=req.priority if req.priority is not None else 1,
        )
    except TypeError:
        # If the controller signature differs, fall back to positional call with safest ordering.
        try:
            _enqueue_job(req.op, payload, job_id, req.pinned_agent, req.priority if req.priority is not None else 1)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to enqueue job: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to enqueue job: {e}")

    if req.idempotency_key:
        with _IDEMPOTENCY_LOCK:
            _IDEMPOTENCY[req.idempotency_key] = job_id

    return JobSubmitResponse(job_id=job_id)

