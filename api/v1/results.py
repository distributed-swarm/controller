# controller/api/v1/results.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()


class ResultRequest(BaseModel):
    lease_id: str = Field(..., description="Lease identifier returned by POST /v1/leases.")
    job_id: str = Field(..., description="Job identifier.")
    status: str = Field(..., description="Either 'succeeded' or 'failed'.")
    result: Optional[Dict[str, Any]] = Field(default=None, description="Result payload (JSON).")
    error: Optional[Dict[str, Any]] = Field(default=None, description="Error payload (JSON) if failed.")


class ResultResponse(BaseModel):
    ok: bool = True


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.post("/results", response_model=ResultResponse)
def post_result(req: ResultRequest) -> ResultResponse:
    """
    Report job completion.

    v1 contract:
      status: succeeded|failed

    Internal controller (current) often uses:
      completed|failed

    This endpoint is intentionally idempotent:
      - If the job is already terminal, we accept and return ok.
    """
    status_norm = (req.status or "").strip().lower()
    if status_norm not in ("succeeded", "failed"):
        raise HTTPException(status_code=422, detail="status must be 'succeeded' or 'failed'")

    internal_status = "completed" if status_norm == "succeeded" else "failed"

    # Prefer calling an existing controller function if one exists
    try:
        import app as app  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Controller module not available: {e}")

    # 1) If controller provides a dedicated result function, use it.
    # We try a few likely names so we don't have to refactor your old code immediately.
    result_fn = None
    for name in ("post_result", "_post_result", "handle_result", "_handle_result"):
        if hasattr(app, name):
            result_fn = getattr(app, name)
            break

    if callable(result_fn):
        try:
            # Try keyword-first (most robust)
            result_fn(
                job_id=req.job_id,
                status=internal_status,
                result=req.result,
                error=req.error,
                lease_id=req.lease_id,
            )
            return ResultResponse(ok=True)
        except TypeError:
            # Fallback: some implementations may not accept lease_id, or use different arg order
            try:
                result_fn(req.job_id, internal_status, req.result, req.error)
                return ResultResponse(ok=True)
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to record result: {e}")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to record result: {e}")

    # 2) Fallback: update in-memory JOBS directly (common in your current controller)
    jobs = getattr(app, "JOBS", None)
    if jobs is None or not isinstance(jobs, dict):
        raise HTTPException(status_code=500, detail="Controller JOBS store not found; cannot record results")

    job = jobs.get(req.job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {req.job_id}")

    # Idempotency: if already terminal, accept without changing anything
    existing = str(job.get("status", "")).lower()
    if existing in ("completed", "failed", "succeeded"):  # handle both vocabularies
        return ResultResponse(ok=True)

    # Update job record
    job["status"] = internal_status
    job["updated_at"] = _now_iso()
    job["completed_at"] = _now_iso()

    if internal_status == "completed":
        job["result"] = req.result if req.result is not None else job.get("result")
        job.pop("error", None)
    else:
        job["error"] = req.error if req.error is not None else job.get("error")
        # It's fine to keep result absent on failure.

    # If your controller tracks leased_by / lease_id, we store it defensively
    job.setdefault("lease_id", req.lease_id)

    return ResultResponse(ok=True)

