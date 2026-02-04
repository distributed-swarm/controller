# controller/api/v1/leases.py
from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field

router = APIRouter()


class LeaseRequest(BaseModel):
    agent: str = Field(..., description="Agent name/id (e.g. 'cpu-1').")
    capabilities: Optional[Union[List[str], Dict[str, Any]]] = Field(
        default=None,
        description="Agent capabilities. Accepts legacy list[str] or v1 {'ops':[...]} dict.",
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
    # Note: labels/metrics/worker_profile may be sent by newer agents.
    # We accept them permissively via getattr() in _upsert_agent_from_lease.


class LeaseTask(BaseModel):
    job_id: str
    job_epoch: int = Field(..., description="Current job epoch; required for /v1/results.")
    op: str
    # Back-compat: payload may be any JSON value, not only dicts.
    payload: Any = Field(default_factory=dict)
    pinned_agent: Optional[str] = None


class LeaseResponse(BaseModel):
    lease_id: str
    tasks: List[LeaseTask]


# -----------------------------
# Time / TTL helpers
# -----------------------------

def _now_ts() -> float:
    return time.time()


def _lease_ttl_seconds(req: LeaseRequest) -> int:
    """
    One TTL rule. Keep it bounded so an agent can't request nonsense.
    Uses timeout_ms as a hint so leases outlive long-poll by a bit.
    """
    # e.g. timeout 25s -> ttl ~30s
    base = int((req.timeout_ms or 0) / 1000)
    ttl = base + 5
    return max(5, min(ttl, 120))


# -----------------------------
# Job normalization / safety
# -----------------------------

def _extract_job_epoch(job: Dict[str, Any]) -> int:
    """
    Extract a job epoch from possible key variants. Controller should store it under 'job_epoch'.
    Agents require it to post /v1/results.
    """
    epoch = (
        job.get("job_epoch")
        if "job_epoch" in job
        else job.get("jobEpoch")
        if "jobEpoch" in job
        else job.get("epoch")
        if "epoch" in job
        else None
    )
    if epoch is None:
        # Defaulting to 1 is safe only if job_ids are not reused.
        return 1
    try:
        return int(epoch)
    except Exception:
        raise HTTPException(status_code=500, detail=f"Invalid job_epoch type/value: {epoch!r} for job={job!r}")


def _normalize_job(job: Dict[str, Any]) -> LeaseTask:
    job_id = job.get("job_id") or job.get("id") or job.get("jobId")
    op = job.get("op") or job.get("type") or job.get("job_type") or job.get("jobType")

    if not job_id or not op:
        raise HTTPException(status_code=500, detail=f"Invalid leased job shape: {job}")

    job_epoch = _extract_job_epoch(job)

    payload = job.get("payload")
    if payload is None:
        payload = {}

    return LeaseTask(
        job_id=str(job_id),
        job_epoch=job_epoch,
        op=str(op),
        payload=payload,
        pinned_agent=job.get("pinned_agent") or job.get("pinnedAgent"),
    )


def _job_allows_agent(job: Dict[str, Any], agent_name: str) -> bool:
    """
    Ensure pinned jobs never get returned to the wrong agent.
    """
    pinned = job.get("pinned_agent") or job.get("pinnedAgent")
    if pinned and str(pinned) != str(agent_name):
        return False
    return True


# -----------------------------
# SSE event publishing (best-effort)
# -----------------------------

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
        job_epoch = _extract_job_epoch(job)
    except Exception:
        job_epoch = None

    try:
        publish_event(
            "job.leased",
            {
                "job_id": str(job_id) if job_id is not None else None,
                "job_epoch": job_epoch,
                "lease_id": str(lease_id),
                "agent": str(agent),
                "op": str(op) if op is not None else None,
                "pinned_agent": str(pinned) if pinned is not None else None,
            },
        )
    except Exception:
        return


# -----------------------------
# Agent bookkeeping (best-effort)
# -----------------------------

def _upsert_agent_from_lease(req: LeaseRequest) -> None:
    """
    Minimal agent bookkeeping from /v1/leases.
    This makes /v1/agents usable and enables routing later.
    """
    try:
        from api.v1.agents import upsert_agent  # type: ignore
    except Exception:
        return

    caps = req.capabilities
    if isinstance(caps, dict):
        caps = caps.get("ops") or []
    if caps is None:
        caps = []
    caps = [str(x) for x in caps]

    upsert_agent(
        name=req.agent,
        labels={},
        capabilities={"ops": caps},
        worker_profile=getattr(req, "worker_profile", None) or {},
        metrics=getattr(req, "metrics", None) or {},
    )


# -----------------------------
# Epoch fencing support (the missing pieces)
# -----------------------------

def _set_authoritative_lease_fields(
    job: Dict[str, Any],
    *,
    lease_id: str,
    now: float,
    ttl_s: int,
) -> None:
    """
    Authoritatively stamp the job with epoch + lease ownership.
    This is required for /v1/results fencing to be truthful.
    """
    # If missing, start at epoch 1
    job["job_epoch"] = int(job.get("job_epoch") or 1)
    job["lease_id"] = lease_id
    job["lease_expires_at"] = now + float(ttl_s)

    # If your controller uses status fields, keep them consistent.
    if job.get("status") in (None, "queued", "ready"):
        job["status"] = "leased"


def _expire_leases_best_effort(now: float) -> None:
    """
    Minimal controller-only "clock tick":
    - If a job is leased and expired, bump epoch and clear lease fields.
    - Mark the job available again (best-effort).
    """
    try:
        import app  # type: ignore
    except Exception:
        return

    jobs_store = getattr(app, "JOBS", None)
    if not isinstance(jobs_store, dict):
        # No visible job store to sweep; can't do anything here.
        return

    # Optional hooks if your scheduler/queue needs explicit requeueing.
    requeue_fn = getattr(app, "_requeue_job", None)
    mark_available_fn = getattr(app, "_mark_job_available", None)

    for job_id, job in list(jobs_store.items()):
        if not isinstance(job, dict):
            continue
        lease_id = job.get("lease_id")
        exp = job.get("lease_expires_at")
        if not lease_id or exp is None:
            continue

        try:
            exp_f = float(exp)
        except Exception:
            # If exp is garbage, force-expire it rather than getting stuck.
            exp_f = -1.0

        if now > exp_f:
            job["job_epoch"] = int(job.get("job_epoch") or 1) + 1
            job["lease_id"] = None
            job["lease_expires_at"] = None

            # Mark available again in whatever way your system expects.
            if job.get("status") == "leased":
                job["status"] = "queued"

            # If your scheduler requires requeue, try it (best-effort).
            try:
                if callable(mark_available_fn):
                    mark_available_fn(job_id)
                elif callable(requeue_fn):
                    requeue_fn(job_id)
            except Exception:
                # Never break leasing because requeue helper had a bad day.
                pass


# -----------------------------
# Endpoint
# -----------------------------

@router.post("/leases", response_model=LeaseResponse, responses={204: {"description": "No work available"}})
def lease_work(req: LeaseRequest) -> Response | LeaseResponse:
    """
    Lease work for an agent.

    Behavior:
    - Long-polls up to timeout_ms.
    - Tries to lease up to max_tasks jobs.
    - Returns 204 with empty body if no work is available by timeout.

    Epoch fencing:
    - On lease, controller stamps job_epoch (default 1), lease_id, lease_expires_at.
    - Before leasing, controller expires old leases: bumps epoch and clears lease fields.
    """
    _upsert_agent_from_lease(req)

    # Defer import to avoid circular wiring issues while we build v1.
    try:
        from app import _lease_next_job  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Controller lease function not available: {e}")

    now = _now_ts()
    _expire_leases_best_effort(now)

    lease_id = str(uuid.uuid4())
    ttl_s = _lease_ttl_seconds(req)

    deadline = now + (req.timeout_ms / 1000.0 if req.timeout_ms else 0.0)
    tasks: List[LeaseTask] = []

    def try_lease_once() -> Optional[Dict[str, Any]]:
        try:
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
            # "Clock tick" before each attempt, so long-polling advances epochs over time.
            _expire_leases_best_effort(_now_ts())

            job = try_lease_once()
            if not job:
                break

            if not _job_allows_agent(job, req.agent):
                # Safety: never hand a pinned job to the wrong agent.
                break

            # AUTHORITATIVE LEASE WRITE (the missing piece)
            # NOTE: This assumes `job` is a reference to your stored job dict.
            # If `_lease_next_job` returns a copy, you MUST persist these fields
            # in your scheduler/store (e.g., via an app._commit_lease hook).
            _set_authoritative_lease_fields(job, lease_id=lease_id, now=_now_ts(), ttl_s=ttl_s)

            tasks.append(_normalize_job(job))
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

        if _now_ts() >= deadline:
            return Response(status_code=204)

        time.sleep(0.1)
