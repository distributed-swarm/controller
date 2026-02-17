# controller/api/v1/leases.py
from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field

from lifecycle.log import emit

router = APIRouter()


class LeaseRequest(BaseModel):
    agent: str = Field(..., description="Agent name/id (e.g. 'cpu-1').")

    # B1: hard boundary — namespace is REQUIRED
    namespace: str = Field(..., min_length=1, description="Tenant/namespace (required).")

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


class ForceExpireRequest(BaseModel):
    job_id: str = Field(..., description="Job ID to force-expire/requeue.")
    namespace: str = Field(..., min_length=1, description="Tenant/namespace (required).")
    reason: str = Field(default="forced", description="Reason for forced expiry (audit/logging).")


def _now_ts() -> float:
    return time.time()


def _lease_ttl_seconds_from_request(req: LeaseRequest) -> int:
    base = int((req.timeout_ms or 0) / 1000)
    ttl = base + 5
    return max(5, min(ttl, 120))


def _coerce_positive_float(v: Any) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f <= 0:
        return None
    return f


def _effective_ttl_seconds(*, job: Dict[str, Any], req_ttl_s: int) -> float:
    policy = _coerce_positive_float(job.get("lease_timeout_s"))
    if policy is None:
        return float(req_ttl_s)
    return max(5.0, min(float(policy), 120.0))


def _extract_job_id(leased_preview: Any) -> Optional[str]:
    if leased_preview is None:
        return None
    if isinstance(leased_preview, str):
        return leased_preview
    if isinstance(leased_preview, dict):
        for k in ("job_id", "id"):
            v = leased_preview.get(k)
            if v:
                return str(v)
    return None


def _job_allows_agent(job: Dict[str, Any], agent: str) -> bool:
    pinned = job.get("pinned_agent")
    if not pinned:
        return True
    return str(pinned) == str(agent)


def _normalize_task_from_job(job: Dict[str, Any]) -> LeaseTask:
    return LeaseTask(
        job_id=str(job.get("id") or job.get("job_id") or ""),
        job_epoch=int(job.get("job_epoch") or 1),
        op=str(job.get("op") or ""),
        payload=job.get("payload") or {},
        pinned_agent=str(job.get("pinned_agent")) if job.get("pinned_agent") else None,
    )


def _requeue_job(task_queues: Dict[str, Any], job_id: str, job: Dict[str, Any]) -> None:
    # Best-effort: put job back in its queue based on excitatory_level / priority.
    lvl = job.get("excitatory_level")
    try:
        lvl_i = int(lvl) if lvl is not None else 1
    except (TypeError, ValueError):
        lvl_i = 1
    lvl_i = max(0, min(3, lvl_i))
    q = task_queues.get(str(lvl_i))
    if isinstance(q, list):
        q.append(job_id)


def _try_publish_event(event: str, data: Dict[str, Any]) -> None:
    try:
        from api.v1.events import publish_event  # deferred to avoid circular imports
    except Exception:
        return
    try:
        publish_event(event, data)
    except Exception:
        return


def _publish_job_leased_event(*, namespace: str, job_id: str, lease_id: str, agent: str, job_epoch: int) -> None:
    _try_publish_event(
        "job.leased",
        {
            "namespace": namespace,
            "job_id": job_id,
            "lease_id": lease_id,
            "agent": agent,
            "job_epoch": job_epoch,
        },
    )


def _publish_job_expired_event(*, namespace: str, job_id: str, lease_id: Optional[str], job_epoch: int, reason: str) -> None:
    _try_publish_event(
        "job.expired",
        {
            "namespace": namespace,
            "job_id": job_id,
            "lease_id": lease_id,
            "job_epoch": job_epoch,
            "reason": reason,
        },
    )


def _upsert_agent_from_lease(req: LeaseRequest) -> None:
    """
    Keep agent registry warm from lease requests (v1-only systems may not send heartbeats).
    """
    try:
        import api.v1.agents as agents_mod  # type: ignore
    except Exception:
        return

    upsert = getattr(agents_mod, "upsert_agent", None) or getattr(agents_mod, "_upsert_agent", None)
    if not callable(upsert):
        return

    caps = req.capabilities
    ops: List[str] = []
    if isinstance(caps, list):
        ops = [str(x) for x in caps if x is not None]
    elif isinstance(caps, dict):
        v = caps.get("ops")
        if isinstance(v, list):
            ops = [str(x) for x in v if x is not None]

    try:
        upsert(
            req.agent,
            labels={"namespace": req.namespace},
            capabilities={"ops": ops},
        )
    except Exception:
        return


def _expire_leases_and_bump_epochs(app_mod: Any, now_ts: float) -> None:
    jobs = getattr(app_mod, "JOBS", None)
    task_queues = getattr(app_mod, "TASK_QUEUES", None)

    if not isinstance(jobs, dict) or not isinstance(task_queues, dict):
        return

    for job_id, job in list(jobs.items()):
        if not isinstance(job, dict):
            continue
        exp = job.get("lease_expires_at")
        if exp is None:
            continue
        try:
            exp_f = float(exp)
        except (TypeError, ValueError):
            continue
        if now_ts <= exp_f:
            continue

        # Expired: bump epoch, clear lease, requeue
        prev_lease_id = job.get("lease_id")
        ns = job.get("namespace") or job.get("labels", {}).get("namespace") or "default"

        try:
            job["job_epoch"] = int(job.get("job_epoch") or 1) + 1
        except (TypeError, ValueError):
            job["job_epoch"] = 2

        job["lease_id"] = None
        job["lease_expires_at"] = None
        job["leased_ts"] = None
        job["leased_by"] = None
        job["status"] = "queued"

        _requeue_job(task_queues, str(job_id), job)

        emit(
            "JOB_EXPIRED",
            namespace=str(ns),
            job_id=str(job_id),
            lease_id=str(prev_lease_id) if prev_lease_id else None,
            job_epoch=int(job["job_epoch"]),
            reason="ttl",
        )

        _publish_job_expired_event(
            namespace=str(ns),
            job_id=str(job_id),
            lease_id=str(prev_lease_id) if prev_lease_id else None,
            job_epoch=int(job["job_epoch"]),
            reason="ttl",
        )


def _stamp_authoritative_lease(*, job: Dict[str, Any], lease_id: str, now: float, req_ttl_s: int) -> int:
    # Ensure epoch exists
    try:
        epoch = int(job.get("job_epoch") or 1)
    except (TypeError, ValueError):
        epoch = 1
    job["job_epoch"] = epoch

    job["lease_id"] = lease_id
    job["leased_ts"] = now

    ttl = _effective_ttl_seconds(job=job, req_ttl_s=req_ttl_s)
    job["lease_expires_at"] = now + ttl
    job["lease_ttl_effective_s"] = ttl

    return epoch


@router.post("/leases", response_model=LeaseResponse, responses={204: {"description": "No work available"}})
def lease_work(req: LeaseRequest) -> Response | LeaseResponse:
    ns = (req.namespace or "").strip()
    if not ns:
        emit("LEASE_REJECTED", namespace=ns, agent=req.agent, reason="missing_namespace")
        raise HTTPException(status_code=400, detail="namespace is required")

    # --- Namespace fence: stored agent namespace is authoritative ---
    existing_ns: Optional[str] = None
    try:
        import api.v1.agents as agents_mod  # type: ignore

        getter = getattr(agents_mod, "get_agent", None)
        if callable(getter):
            existing = getter(req.agent)
            if isinstance(existing, dict):
                labels = existing.get("labels") or {}
                if isinstance(labels, dict):
                    existing_ns = labels.get("namespace")

        store = getattr(agents_mod, "AGENTS", None) or getattr(agents_mod, "_AGENTS", None)
        if isinstance(store, dict):
            for ns_key, by_name in store.items():
                if isinstance(by_name, dict) and req.agent in by_name:
                    existing_ns = str(ns_key)
                    break
    except Exception:
        existing_ns = None

    if existing_ns:
        if str(existing_ns) != str(ns):
            emit(
                "LEASE_REJECTED",
                namespace=ns,
                agent=req.agent,
                reason="AGENT_NAMESPACE_MISMATCH",
                agent_namespace=existing_ns,
                requested_namespace=ns,
            )
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "AGENT_NAMESPACE_MISMATCH",
                    "agent": req.agent,
                    "agent_namespace": existing_ns,
                    "requested_namespace": ns,
                },
            )
        ns = str(existing_ns)
    # ---------------------------------------------------------------

    _upsert_agent_from_lease(req)

    try:
        import app as app_mod  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Runtime module 'app' not available: {e}")

    state_lock = getattr(app_mod, "STATE_LOCK", None)
    jobs = getattr(app_mod, "JOBS", None)
    lease_next = getattr(app_mod, "_lease_next_job", None)

    if state_lock is None or not isinstance(jobs, dict) or not callable(lease_next):
        raise HTTPException(status_code=500, detail="Controller internals not available (STATE_LOCK/JOBS/_lease_next_job)")

    lease_id = str(uuid.uuid4())
    req_ttl_s = _lease_ttl_seconds_from_request(req)
    deadline = _now_ts() + (req.timeout_ms / 1000.0 if req.timeout_ms else 0.0)
    tasks: List[LeaseTask] = []

    def _call_lease_next():
        try:
            return lease_next(req.agent, ns)
        except TypeError:
            return lease_next(req.agent)

    def try_lease_batch_once() -> None:
        nonlocal tasks

        _expire_leases_and_bump_epochs(app_mod, _now_ts())

        while len(tasks) < req.max_tasks:
            leased_preview = _call_lease_next()
            if leased_preview is None:
                return

            job_id = _extract_job_id(leased_preview)
            if not job_id:
                return

            stored = jobs.get(job_id)
            if not isinstance(stored, dict):
                return

            # Hard namespace guard (defense-in-depth)
            if str(stored.get("namespace") or "") != ns:
                return

            if not _job_allows_agent(stored, req.agent):
                return

            epoch = _stamp_authoritative_lease(job=stored, lease_id=lease_id, now=_now_ts(), req_ttl_s=req_ttl_s)
            tasks.append(_normalize_task_from_job(stored))

            emit(
                "JOB_LEASED",
                namespace=ns,
                job_id=str(job_id),
                lease_id=lease_id,
                leased_by=req.agent,
                job_epoch=epoch,
                op=str(stored.get("op") or ""),
            )

            _publish_job_leased_event(
                namespace=ns,
                job_id=str(job_id),
                lease_id=lease_id,
                agent=req.agent,
                job_epoch=epoch,
            )

            _expire_leases_and_bump_epochs(app_mod, _now_ts())

    if req.timeout_ms == 0:
        with state_lock:
            try_lease_batch_once()
        if not tasks:
            emit("LEASE_EMPTY", namespace=ns, agent=req.agent, max_tasks=req.max_tasks)
            return Response(status_code=204)
        return LeaseResponse(lease_id=lease_id, tasks=tasks)

    while True:
        with state_lock:
            _expire_leases_and_bump_epochs(app_mod, _now_ts())
            try_lease_batch_once()
            if tasks:
                return LeaseResponse(lease_id=lease_id, tasks=tasks)

        if _now_ts() >= deadline:
            emit("LEASE_EMPTY", namespace=ns, agent=req.agent, max_tasks=req.max_tasks)
            return Response(status_code=204)

        time.sleep(0.1)


@router.post("/leases/expire")
def force_expire(req: ForceExpireRequest) -> Dict[str, Any]:
    ns = (req.namespace or "").strip()
    if not ns:
        emit("LEASE_EXPIRE_REJECTED", namespace=ns, job_id=req.job_id, reason="missing_namespace")
        raise HTTPException(status_code=400, detail="namespace is required")

    try:
        import app as app_mod  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Runtime module 'app' not available: {e}")

    state_lock = getattr(app_mod, "STATE_LOCK", None)
    jobs = getattr(app_mod, "JOBS", None)
    task_queues = getattr(app_mod, "TASK_QUEUES", None)

    if state_lock is None or not isinstance(jobs, dict) or not isinstance(task_queues, dict):
        raise HTTPException(status_code=500, detail="Controller internals not available (STATE_LOCK/JOBS/TASK_QUEUES)")

    with state_lock:
        job = jobs.get(req.job_id)
        if not isinstance(job, dict):
            raise HTTPException(status_code=404, detail={"error": "NOT_FOUND", "job_id": req.job_id})

        if str(job.get("namespace") or "") != ns:
            raise HTTPException(status_code=404, detail={"error": "NOT_FOUND", "job_id": req.job_id})

        prev_lease_id = job.get("lease_id")
        try:
            job["job_epoch"] = int(job.get("job_epoch") or 1) + 1
        except (TypeError, ValueError):
            job["job_epoch"] = 2

        job["lease_id"] = None
        job["lease_expires_at"] = None
        job["leased_ts"] = None
        job["leased_by"] = None
        job["status"] = "queued"

        _requeue_job(task_queues, req.job_id, job)

        emit(
            "JOB_EXPIRED",
            namespace=ns,
            job_id=req.job_id,
            lease_id=str(prev_lease_id) if prev_lease_id else None,
            job_epoch=int(job["job_epoch"]),
            reason=req.reason,
        )

        _publish_job_expired_event(
            namespace=ns,
            job_id=req.job_id,
            lease_id=str(prev_lease_id) if prev_lease_id else None,
            job_epoch=int(job["job_epoch"]),
            reason=req.reason,
        )

    return {"ok": True, "job_id": req.job_id, "job_epoch": job.get("job_epoch"), "status": job.get("status")}
