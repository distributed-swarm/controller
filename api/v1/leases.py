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

    # Accept both legacy list[str] and dict {"ops":[...]} (agents may send either)
    capabilities: Optional[Union[List[str], Dict[str, Any]]] = Field(
        default=None,
        description="Agent capabilities. Accepts list[str] or dict containing ops.",
    )

    max_tasks: int = Field(default=1, ge=1, le=64)
    timeout_ms: int = Field(default=25000, ge=0, le=120000)


class LeaseTask(BaseModel):
    job_id: str
    job_epoch: int
    op: str
    payload: Any = Field(default_factory=dict)
    pinned_agent: Optional[str] = None


class LeaseResponse(BaseModel):
    lease_id: str
    tasks: List[LeaseTask]


def _now_ts() -> float:
    return time.time()


def _extract_job_id(job_like: Dict[str, Any]) -> Optional[str]:
    v = job_like.get("job_id") or job_like.get("id") or job_like.get("jobId")
    return str(v) if v else None


def _normalize_task_from_job(job: Dict[str, Any]) -> LeaseTask:
    job_id = _extract_job_id(job)
    op = job.get("op") or job.get("type") or job.get("job_type") or job.get("jobType")
    if not job_id or not op:
        raise HTTPException(status_code=500, detail=f"Invalid job shape for lease: {job}")

    payload = job.get("payload")
    if payload is None:
        payload = {}

    pinned = job.get("pinned_agent") or job.get("pinnedAgent")

    try:
        epoch = int(job.get("job_epoch") or 1)
    except (TypeError, ValueError):
        epoch = 1

    return LeaseTask(
        job_id=str(job_id),
        job_epoch=epoch,
        op=str(op),
        payload=payload,
        pinned_agent=str(pinned) if pinned is not None else None,
    )


def _publish_job_leased_event(*, job_id: str, lease_id: str, agent: str, job_epoch: int) -> None:
    """
    Best-effort: emit job.leased via SSE. Must never break leasing.
    """
    try:
        from api.v1.events import publish_event  # deferred import
    except Exception:
        return

    try:
        publish_event(
            "job.leased",
            {"job_id": job_id, "lease_id": lease_id, "agent": agent, "job_epoch": job_epoch, "ts": _now_ts()},
        )
    except Exception:
        return


def _upsert_agent_from_lease(req: LeaseRequest) -> None:
    """
    Best-effort: keep /v1/agents happy if you’re using it.
    Your app.py already has rich agent register/heartbeat; this is just for v1 lease callers.
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
    if not isinstance(caps, list):
        caps = [caps]
    ops = [str(x) for x in caps if x is not None]

    try:
        upsert_agent(name=req.agent, labels={}, capabilities={"ops": ops}, worker_profile={}, metrics={})
    except Exception:
        return


def _expire_leases_and_bump_epochs(now: float) -> None:
    """
    The missing “clock tick”:
    If a job is leased and now > lease_expires_at, then:
      - job_epoch += 1
      - clear lease_id / lease_expires_at
      - clear leased_by / leased_ts (so it’s truly un-owned)
      - status -> queued
      - re-enqueue back into TASK_QUEUES by excitatory_level
    """
    try:
        import app as app  # type: ignore
    except Exception:
        return

    jobs = getattr(app, "JOBS", None)
    state_lock = getattr(app, "STATE_LOCK", None)
    task_queues = getattr(app, "TASK_QUEUES", None)

    if not isinstance(jobs, dict) or state_lock is None or not isinstance(task_queues, dict):
        return

    # caller should already hold STATE_LOCK, but be tolerant:
    # we won't acquire if it's not a threading.Lock-like
    for job_id, job in list(jobs.items()):
        if not isinstance(job, dict):
            continue

        if job.get("status") != "leased":
            continue

        exp = job.get("lease_expires_at")
        if exp is None:
            # If no expires_at is set, fall back to existing legacy timeout behavior:
            # leased_ts + lease_timeout_s
            leased_ts = job.get("leased_ts")
            timeout_s = job.get("lease_timeout_s") or 0
            try:
                if leased_ts is None or float(timeout_s) <= 0:
                    continue
                exp = float(leased_ts) + float(timeout_s)
            except (TypeError, ValueError):
                continue

        try:
            exp_f = float(exp)
        except (TypeError, ValueError):
            # garbage exp => treat as expired
            exp_f = -1.0

        if now <= exp_f:
            continue

        # EXPIRE IT
        try:
            job["job_epoch"] = int(job.get("job_epoch") or 1) + 1
        except (TypeError, ValueError):
            job["job_epoch"] = 2

        job["lease_id"] = None
        job["lease_expires_at"] = None

        job["status"] = "queued"
        job["leased_ts"] = None
        job["leased_by"] = None

        # Re-enqueue by excitatory_level (your scheduler expects this)
        try:
            lvl = int(job.get("excitatory_level", 1) or 1)
        except (TypeError, ValueError):
            lvl = 1
        lvl = max(0, min(3, lvl))
        try:
            task_queues[lvl].append(job_id)
        except Exception:
            # if queue is weird, don't crash leasing
            pass


def _stamp_authoritative_lease(
    *,
    job: Dict[str, Any],
    lease_id: str,
    now: float,
) -> int:
    """
    Write the authoritative lease fields into the STORED job dict.
    Returns the (possibly defaulted) job_epoch.
    """
    # default epoch
    try:
        epoch = int(job.get("job_epoch") or 1)
    except (TypeError, ValueError):
        epoch = 1
    job["job_epoch"] = epoch

    # lease identity + TTL
    job["lease_id"] = lease_id

    # TTL source of truth:
    # Prefer legacy lease_timeout_s if present so you don't have competing timers.
    timeout_s = job.get("lease_timeout_s")
    if timeout_s is not None:
        try:
            ttl = float(timeout_s)
        except (TypeError, ValueError):
            ttl = 30.0
    else:
        # fallback tied to long-poll
        ttl = max(5.0, min((0.001 * 5.0) + 30.0, 120.0))  # basically 30s default

    job["lease_expires_at"] = now + float(ttl)

    return epoch


@router.post("/leases", response_model=LeaseResponse, responses={204: {"description": "No work available"}})
def lease_work(req: LeaseRequest) -> Response | LeaseResponse:
    """
    v1 leasing endpoint.

    This is the authoritative writer for:
      - job_epoch (default 1)
      - lease_id
      - lease_expires_at
    and it advances epochs on expiry.
    """
    _upsert_agent_from_lease(req)

    try:
        import app as app  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Runtime module 'app' not available: {e}")

    state_lock = getattr(app, "STATE_LOCK", None)
    jobs = getattr(app, "JOBS", None)
    lease_next = getattr(app, "_lease_next_job", None)

    if state_lock is None or not isinstance(jobs, dict) or not callable(lease_next):
        raise HTTPException(status_code=500, detail="Controller internals not available (STATE_LOCK/JOBS/_lease_next_job)")

    lease_id = str(uuid.uuid4())
    deadline = _now_ts() + (req.timeout_ms / 1000.0 if req.timeout_ms else 0.0)

    tasks: List[LeaseTask] = []

    def try_lease_batch_once() -> None:
        """
        Attempt to lease up to max_tasks and stamp authoritative fields.
        Must hold STATE_LOCK because your scheduler mutates JOBS/queues.
        """
        nonlocal tasks
        now = _now_ts()

        # Clock tick first
        _expire_leases_and_bump_epochs(now)

        while len(tasks) < req.max_tasks:
            leased_preview = lease_next(req.agent)  # returns {"id","job_id","op","payload"} or None
            if leased_preview is None:
                return

            job_id = _extract_job_id(leased_preview)
            if not job_id:
                # shouldn't happen, but don't explode the controller
                return

            job = jobs.get(job_id)
            if not isinstance(job, dict):
                # scheduler returned something that isn't in JOBS; ignore
                return

            # Stamp authoritative lease fields on the stored job
            epoch = _stamp_authoritative_lease(job=job, lease_id=lease_id, now=_now_ts())

            # Normalize response task from stored job (so it includes epoch)
            tasks.append(_normalize_task_from_job(job))

            # SSE best-effort
            _publish_job_leased_event(job_id=job_id, lease_id=lease_id, agent=req.agent, job_epoch=epoch)

            # Keep expiring between picks to avoid “stuck leased” jobs during long polls
            _expire_leases_and_bump_epochs(_now_ts())

    # Fast path: no wait requested
    if req.timeout_ms == 0:
        with state_lock:
            try_lease_batch_once()
        if not tasks:
            return Response(status_code=204)
        return LeaseResponse(lease_id=lease_id, tasks=tasks)

    # Long-poll loop
    while True:
        with state_lock:
            try_lease_batch_once()
            if tasks:
                return LeaseResponse(lease_id=lease_id, tasks=tasks)

        if _now_ts() >= deadline:
            return Response(status_code=204)

        time.sleep(0.1)
