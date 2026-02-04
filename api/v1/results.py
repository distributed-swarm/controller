# controller/api/v1/results.py
from __future__ import annotations

import inspect
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

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


def _publish_job_completed_event(
    *,
    job_id: str,
    lease_id: str,
    status_norm: str,
    agent: Optional[str],
    error: Optional[Any],
) -> None:
    """Best-effort: emit job.completed to SSE. Must never break result posting."""
    try:
        from api.v1.events import publish_event  # deferred to avoid circular imports
    except Exception:
        return

    try:
        publish_event(
            "job.completed",
            {
                "job_id": job_id,
                "lease_id": lease_id,
                "agent": agent,
                "status": status_norm,  # external vocab: succeeded/failed
                "error": error,
                "completed_at": _now_iso(),
            },
        )
    except Exception:
        return


def _force_transition_job(job: Dict[str, Any], *, internal_status: str, req: ResultRequest) -> None:
    """
    Force the job dict into a terminal state, using the controller's timestamp conventions.
    """
    now_ts = time.time()
    now_iso = _now_iso()

    job["status"] = internal_status

    # Float timestamps
    job["updated_ts"] = job.get("updated_ts") or now_ts
    job["completed_ts"] = job.get("completed_ts") or now_ts

    # ISO timestamps (legacy/back-compat)
    job["updated_at"] = job.get("updated_at") or now_iso
    job["completed_at"] = job.get("completed_at") or now_iso

    # Track lease id defensively
    job.setdefault("lease_id", req.lease_id)

    if internal_status == "completed":
        if req.result is not None:
            job["result"] = req.result
        job["error"] = None
        job["state"] = job.get("state") or "succeeded"
    else:
        if req.error is not None:
            job["error"] = req.error
        job["state"] = job.get("state") or "failed"


def _job_is_terminal(job: Dict[str, Any]) -> bool:
    s = str(job.get("status", "")).strip().lower()
    return s in ("completed", "failed", "succeeded")  # tolerate both vocabularies


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
    """Reject stale/zombie results. 409 is correct: result conflicts with current lease/epoch."""
    detail: Dict[str, Any] = {"error": code, "job_id": job_id}

    # Provide alias requested: STALE_EPOCH primary, STALE_LEASE alias.
    if code == "STALE_EPOCH":
        detail["alias"] = "STALE_LEASE"

    if job is not None:
        detail["expected"] = {
            "job_epoch": job.get("job_epoch"),
            "lease_id": job.get("lease_id"),
            "lease_expires_at": job.get("lease_expires_at"),
        }
    detail["got"] = {"job_epoch": req.job_epoch, "lease_id": req.lease_id}
    raise HTTPException(status_code=409, detail=detail)


def _enforce_fencing(job: Dict[str, Any], req: ResultRequest, now_ts: float) -> None:
    """
    Enforce epoch + lease fencing rules on a job dict.
    This is used for BOTH terminal and non-terminal jobs.
    """
    expected_epoch = job.get("job_epoch")
    expected_lease = job.get("lease_id")

    if expected_epoch is not None:
        try:
            exp_epoch_i = int(expected_epoch)
            got_epoch_i = int(req.job_epoch)
        except (TypeError, ValueError):
            _stale("STALE_EPOCH", job_id=req.job_id, req=req, job=job)

        if got_epoch_i != exp_epoch_i:
            _stale("STALE_EPOCH", job_id=req.job_id, req=req, job=job)

    if expected_lease is not None and str(req.lease_id) != str(expected_lease):
        _stale("STALE_LEASE", job_id=req.job_id, req=req, job=job)

    if _is_expired(job, now_ts):
        _stale("STALE_LEASE", job_id=req.job_id, req=req, job=job)


@router.post("/results", response_model=ResultResponse)
async def post_result(req: ResultRequest) -> ResultResponse:
    """
    Report job completion.

    v1 contract:
      status: succeeded|failed

    Internal controller uses:
      leased|completed|failed

    This endpoint is intentionally idempotent:
      - If the job is already terminal, accept ONLY if fencing tokens match.
      - Otherwise enforce fencing, record result, and ensure job transitions terminal.
    """
    status_norm = (req.status or "").strip().lower()
    if status_norm not in ("succeeded", "failed"):
        raise HTTPException(status_code=422, detail="status must be 'succeeded' or 'failed'")

    internal_status = "completed" if status_norm == "succeeded" else "failed"
    now_ts = time.time()

    try:
        import app as app  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Runtime module 'app' not available: {e}")

    jobs = getattr(app, "JOBS", None)
    job: Optional[Dict[str, Any]] = None

    # If we can locate the job in memory, enforce fencing early.
    if isinstance(jobs, dict):
        maybe = jobs.get(req.job_id)
        if isinstance(maybe, dict):
            job = maybe

            # IMPORTANT: even if terminal, only accept idempotently if tokens match.
            if _job_is_terminal(job):
                _enforce_fencing(job, req, now_ts)
                return ResultResponse(ok=True)

            # Non-terminal: enforce fencing before recording.
            _enforce_fencing(job, req, now_ts)

    # Try calling an existing controller result function if one exists.
    result_fn = None
    for name in ("_post_result", "handle_result", "_handle_result"):
        fn = getattr(app, name, None)
        if callable(fn):
            result_fn = fn
            break

    if callable(result_fn):
        # Attempt 1: keyword args
        try:
            r = result_fn(
                job_id=req.job_id,
                status=internal_status,
                result=req.result,
                error=req.error,
                lease_id=req.lease_id,
            )
            if inspect.isawaitable(r):
                await r
        except TypeError:
            # Attempt 2: pass the Pydantic object (legacy handler expects .json())
            try:
                req.status = internal_status
                r = result_fn(req)
                if inspect.isawaitable(r):
                    await r
            except TypeError:
                # Attempt 3: positional fallback (job_id, status, result, error)
                try:
                    r = result_fn(req.job_id, internal_status, req.result, req.error)
                    if inspect.isawaitable(r):
                        await r
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Failed to record result: {e}")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to record result: {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to record result: {e}")

        # Truthfulness: ensure the job actually transitioned. If not, force it.
        if isinstance(jobs, dict):
            maybe2 = jobs.get(req.job_id)
            if isinstance(maybe2, dict) and not _job_is_terminal(maybe2):
                _force_transition_job(maybe2, internal_status=internal_status, req=req)
                job = maybe2
            elif isinstance(maybe2, dict):
                job = maybe2

        _publish_job_completed_event(
            job_id=req.job_id,
            lease_id=req.lease_id,
            status_norm=status_norm,
            agent=_infer_agent_from_job(job),
            error=req.error if status_norm == "failed" else None,
        )
        return ResultResponse(ok=True)

    # No controller function available => update in-memory JOBS directly.
    if not isinstance(jobs, dict):
        raise HTTPException(status_code=500, detail="Controller JOBS store not found; cannot record results")

    job2 = jobs.get(req.job_id)
    if not isinstance(job2, dict):
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {req.job_id}")

    if _job_is_terminal(job2):
        _enforce_fencing(job2, req, now_ts)
        return ResultResponse(ok=True)

    _enforce_fencing(job2, req, now_ts)
    _force_transition_job(job2, internal_status=internal_status, req=req)

    _publish_job_completed_event(
        job_id=req.job_id,
        lease_id=req.lease_id,
        status_norm=status_norm,
        agent=_infer_agent_from_job(job2),
        error=req.error if status_norm == "failed" else None,
    )
    return ResultResponse(ok=True)
