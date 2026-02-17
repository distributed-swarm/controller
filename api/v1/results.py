# controller/api/v1/results.py
from __future__ import annotations

import inspect
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from lifecycle.log import emit

router = APIRouter()


class ResultRequest(BaseModel):
    lease_id: str = Field(..., description="Lease identifier returned by POST /v1/leases.")
    job_id: str = Field(..., description="Job identifier.")
    job_epoch: int = Field(..., description="Epoch fencing token returned with the lease.")
    status: str = Field(..., description="Either 'succeeded' or 'failed'.")
    result: Optional[Any] = Field(default=None, description="Result payload (any JSON).")
    error: Optional[Any] = Field(default=None, description="Error payload (any JSON) if failed.")


class ResultResponse(BaseModel):
    ok: bool = True


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _try_publish_event(event: str, data: Dict[str, Any]) -> None:
    """
    Best-effort: emit event to SSE. Must never break result posting.
    """
    try:
        from api.v1.events import publish_event  # deferred to avoid circular imports
    except Exception:
        return

    try:
        publish_event(event, data)
    except Exception:
        return


def _publish_job_completed_event(
    *,
    job_id: str,
    lease_id: str,
    status_norm: str,
    agent: Optional[str],
    error: Optional[Any],
) -> None:
    _try_publish_event(
        "job.completed",
        {
            "job_id": job_id,
            "lease_id": lease_id,
            "status": status_norm,
            "agent": agent,
            "error": error,
            "ts": _now_iso(),
        },
    )


def _publish_job_result_duplicate_event(*, job_id: str, req: ResultRequest, job: Optional[Dict[str, Any]]) -> None:
    _try_publish_event(
        "job.result.duplicate",
        {
            "job_id": job_id,
            "lease_id": req.lease_id,
            "job_epoch": req.job_epoch,
            "status": req.status,
            "ts": _now_iso(),
        },
    )


def _publish_job_result_accepted_event(*, job_id: str, req: ResultRequest, job: Optional[Dict[str, Any]]) -> None:
    _try_publish_event(
        "job.result.accepted",
        {
            "job_id": job_id,
            "lease_id": req.lease_id,
            "job_epoch": req.job_epoch,
            "status": req.status,
            "ts": _now_iso(),
        },
    )


def _publish_job_result_rejected_event(*, job_id: str, req: ResultRequest, job: Optional[Dict[str, Any]], reason: str) -> None:
    _try_publish_event(
        "job.result.rejected",
        {
            "job_id": job_id,
            "lease_id": req.lease_id,
            "job_epoch": req.job_epoch,
            "status": req.status,
            "reason": reason,
            "ts": _now_iso(),
        },
    )


def _infer_agent_from_job(job: Optional[Dict[str, Any]]) -> Optional[str]:
    """Best effort: if the controller stores leased_by / agent info on the job, surface it."""
    if not isinstance(job, dict):
        return None
    for k in ("leased_by", "agent", "agent_id", "worker", "worker_id"):
        v = job.get(k)
        if v:
            return str(v)
    return None


def _is_expired(job: Dict[str, Any], now_ts: float) -> bool:
    exp = job.get("lease_expires_at")
    if exp is None:
        return False
    try:
        return now_ts > float(exp)
    except (TypeError, ValueError):
        return False


def _stale(code: str, *, job_id: str, req: ResultRequest, job: Optional[Dict[str, Any]] = None) -> None:
    """
    Reject stale/zombie results. 409 is correct: result conflicts with current lease/epoch.

    Ruthless addition: always emit job.result.rejected (best-effort) before raising.
    """
    _publish_job_result_rejected_event(job_id=job_id, req=req, job=job, reason=code)

    emit(
        "JOB_RESULT_REJECTED",
        job_id=job_id,
        lease_id=req.lease_id,
        job_epoch=req.job_epoch,
        reason=code,
        expected=(job.get("job_epoch") if isinstance(job, dict) else None),
        expected_lease_id=(job.get("lease_id") if isinstance(job, dict) else None),
        agent=_infer_agent_from_job(job),
    )

    detail: Dict[str, Any] = {"error": code, "job_id": job_id}

    # Provide alias requested: STALE_EPOCH primary, STALE_LEASE alias.
    if code == "STALE_EPOCH":
        detail["alias"] = "STALE_LEASE"
    elif code == "STALE_LEASE":
        detail["alias"] = "STALE_EPOCH"

    raise HTTPException(status_code=409, detail=detail)


@router.post("/results", response_model=ResultResponse)
def post_result(req: ResultRequest) -> ResultResponse:
    """
    Commit a job result. Uses epoch fencing to prevent zombies.

    Rules:
    - job_epoch must match current job_epoch stored on job
    - lease_id must match current lease_id stored on job
    """
    job_id = str(req.job_id)

    try:
        import app as app_mod  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Runtime module 'app' not available: {e}")

    state_lock = getattr(app_mod, "STATE_LOCK", None)
    jobs = getattr(app_mod, "JOBS", None)

    if state_lock is None or not isinstance(jobs, dict):
        raise HTTPException(status_code=500, detail="Controller internals not available (STATE_LOCK/JOBS)")

    status_norm = str(req.status or "").strip().lower()
    if status_norm not in ("succeeded", "failed"):
        raise HTTPException(status_code=400, detail={"error": "BAD_STATUS", "status": req.status})

    now_ts = time.time()

    with state_lock:
        job = jobs.get(job_id)
        if not isinstance(job, dict):
            raise HTTPException(status_code=404, detail={"error": "NOT_FOUND", "job_id": job_id})

        # Expired leases are always stale
        if _is_expired(job, now_ts):
            _stale("STALE_EPOCH", job_id=job_id, req=req, job=job)

        # Epoch fence
        expected_epoch = job.get("job_epoch")
        try:
            expected_epoch_i = int(expected_epoch) if expected_epoch is not None else 1
        except (TypeError, ValueError):
            expected_epoch_i = 1
        if int(req.job_epoch) != expected_epoch_i:
            _stale("STALE_EPOCH", job_id=job_id, req=req, job=job)

        # Lease fence
        expected_lease_id = job.get("lease_id")
        if expected_lease_id and str(req.lease_id) != str(expected_lease_id):
            _stale("STALE_LEASE", job_id=job_id, req=req, job=job)

        # If already terminal, idempotent accept if same lease+epoch, otherwise stale
        state = str(job.get("state") or job.get("status") or "").lower()
        if state in ("succeeded", "failed", "completed"):
            _publish_job_result_duplicate_event(job_id=job_id, req=req, job=job)
            emit(
                "JOB_RESULT_DUPLICATE",
                job_id=req.job_id,
                lease_id=req.lease_id,
                job_epoch=req.job_epoch,
                status=status_norm,
                agent=_infer_agent_from_job(job),
            )
            return ResultResponse(ok=True)

        # Commit
        job["state"] = status_norm
        job["status"] = "completed"
        job["completed_ts"] = now_ts
        job["completed_at"] = _now_iso()

        if status_norm == "succeeded":
            job["result"] = req.result
            job["error"] = None
        else:
            job["result"] = None
            job["error"] = req.error

        agent = _infer_agent_from_job(job)

        _publish_job_result_accepted_event(job_id=job_id, req=req, job=job)
        emit(
            "JOB_RESULT_ACCEPTED",
            job_id=req.job_id,
            lease_id=req.lease_id,
            job_epoch=req.job_epoch,
            status=status_norm,
            agent=_infer_agent_from_job(job),
        )

        _publish_job_completed_event(
            job_id=req.job_id,
            lease_id=req.lease_id,
            status_norm=status_norm,
            agent=agent,
            error=req.error,
        )
        emit(
            "JOB_COMPLETED",
            job_id=req.job_id,
            lease_id=req.lease_id,
            job_epoch=req.job_epoch,
            status=status_norm,
            agent=agent,
        )

    return ResultResponse(ok=True)
