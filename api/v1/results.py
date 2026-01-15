# controller/api/v1/results.py
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()


class ResultRequest(BaseModel):
    lease_id: str = Field(..., description="Lease identifier returned by POST /v1/leases.")
    job_id: str = Field(..., description="Job identifier.")
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
    """
    Best-effort: emit job.completed to SSE. Must never break result posting.
    """
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
                # v1 caller uses succeeded/failed; keep that externally (demo-friendly).
                "status": status_norm,
                "error": error,
                "completed_at": _now_iso(),
            },
        )
    except Exception:
        return


def _force_transition_job(job: Dict[str, Any], *, internal_status: str, req: ResultRequest) -> None:
    """
    Force the job dict into a terminal state, using the controller's timestamp conventions.

    We set:
      - status: completed|failed
      - completed_ts / updated_ts (float epoch seconds)
      - completed_at / updated_at (ISO UTC strings) for legacy compatibility
      - lease_id (if not already set)
      - result / error appropriately
    """
    now_ts = time.time()
    now_iso = _now_iso()

    job["status"] = internal_status

    # Float timestamps (preferred in current controller responses)
    job["updated_ts"] = job.get("updated_ts") or now_ts
    job["completed_ts"] = job.get("completed_ts") or now_ts

    # ISO timestamps (legacy/back-compat)
    job["updated_at"] = job.get("updated_at") or now_iso
    job["completed_at"] = job.get("completed_at") or now_iso

    # Track lease id defensively (harmless if already present)
    job.setdefault("lease_id", req.lease_id)

    if internal_status == "completed":
        # Preserve controller echo/shape if already set, otherwise fill from v1.
        if req.result is not None:
            job["result"] = req.result
        job["error"] = None
    else:
        if req.error is not None:
            job["error"] = req.error
        # Leave result as-is (often None) on failure.


def _job_is_terminal(job: Dict[str, Any]) -> bool:
    s = str(job.get("status", "")).strip().lower()
    return s in ("completed", "failed", "succeeded")  # tolerate both vocabularies


def _infer_agent_from_job(job: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    Best effort: if the controller stores leased_by / agent info on the job, surface it.
    """
    if not isinstance(job, dict):
        return None
    for k in ("leased_by", "agent", "agent_id", "worker", "worker_id"):
        v = job.get(k)
        if v:
            return str(v)
    return None


@router.post("/results", response_model=ResultResponse)
def post_result(req: ResultRequest) -> ResultResponse:
    """
    Report job completion.

    v1 contract:
      status: succeeded|failed

    Internal controller uses:
      leased|completed|failed

    This endpoint is intentionally idempotent:
      - If the job is already terminal, accept and return ok.
      - If the controller result function returns ok but doesn't transition state,
        we force the in-memory JOBS record into a terminal state (truthful v1 behavior).
    """
    status_norm = (req.status or "").strip().lower()
    if status_norm not in ("succeeded", "failed"):
        raise HTTPException(status_code=422, detail="status must be 'succeeded' or 'failed'")

    internal_status = "completed" if status_norm == "succeeded" else "failed"

    try:
        import app as app  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Runtime module 'app' not available: {e}")

    jobs = getattr(app, "JOBS", None)
    job: Optional[Dict[str, Any]] = None
    if isinstance(jobs, dict):
        maybe = jobs.get(req.job_id)
        if isinstance(maybe, dict):
            job = maybe
            if _job_is_terminal(job):
                # Emit event even on idempotent repeats? Usually no.
                # We'll avoid double-firing completed events.
                return ResultResponse(ok=True)

    # Try calling an existing controller result function if one exists.
    result_fn = None
    for name in ("post_result", "_post_result", "handle_result", "_handle_result"):
        fn = getattr(app, name, None)
        if callable(fn):
            result_fn = fn
            break

    if callable(result_fn):
        # Attempt 1: keyword args
        try:
            result_fn(
                job_id=req.job_id,
                status=internal_status,
                result=req.result,
                error=req.error,
                lease_id=req.lease_id,
            )
        except TypeError:
            # Attempt 2: single payload dict (this matches your current controller signature)
            try:
                payload: Dict[str, Any] = {
                    "lease_id": req.lease_id,
                    "job_id": req.job_id,
                    "status": internal_status,
                    "result": req.result,
                    "error": req.error,
                }
                result_fn(payload)
            except TypeError:
                # Attempt 3: positional fallback (job_id, status, result, error)
                try:
                    result_fn(req.job_id, internal_status, req.result, req.error)
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

        # Emit SSE completed event (best-effort)
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
        return ResultResponse(ok=True)

    _force_transition_job(job2, internal_status=internal_status, req=req)

    # Emit SSE completed event (best-effort)
    _publish_job_completed_event(
        job_id=req.job_id,
        lease_id=req.lease_id,
        status_norm=status_norm,
        agent=_infer_agent_from_job(job2),
        error=req.error if status_norm == "failed" else None,
    )
    return ResultResponse(ok=True)
