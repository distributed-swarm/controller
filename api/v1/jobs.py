# controller/api/v1/jobs.py
from __future__ import annotations

from typing import Any, Dict, Optional, Set

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from lifecycle.log import emit

router = APIRouter()


def _ops_from_capabilities(caps: Any) -> Set[str]:
    """
    Best-effort extract ops from shapes seen across agents/registry:
      - {"ops": ["echo", ...]}
      - {"ops": {"echo": true, ...}}
      - ["echo", ...]
    """
    out: Set[str] = set()
    if caps is None:
        return out

    if isinstance(caps, dict):
        v = caps.get("ops")
        if isinstance(v, list):
            for x in v:
                s = str(x).strip() if x is not None else ""
                if s:
                    out.add(s)
            return out
        if isinstance(v, dict):
            for k, val in v.items():
                if not val:
                    continue
                s = str(k).strip()
                if s:
                    out.add(s)
            return out
        if isinstance(v, str):
            s = v.strip()
            if s:
                out.add(s)
            return out
        return out

    if isinstance(caps, list):
        for x in caps:
            s = str(x).strip() if x is not None else ""
            if s:
                out.add(s)
        return out

    return out


def _collect_known_ops(namespace: str) -> Dict[str, Any]:
    """
    Attempts to read the v1 agent registry and return:
      {
        "ops": set[str],                    # union of all ops in the namespace
        "by_agent": {name: set[str], ...},  # ops per agent
        "ok": bool                          # whether we could read registry at all
      }

    IMPORTANT: if registry can't be read, we return ok=False and the caller
    should fail-open to avoid bricking cold start.
    """
    ops: Set[str] = set()
    by_agent: Dict[str, Set[str]] = {}

    try:
        # Most likely location for the in-memory registry used by /v1/agents
        import api.v1.agents as agents_mod  # type: ignore
    except Exception:
        return {"ops": ops, "by_agent": by_agent, "ok": False}

    store = getattr(agents_mod, "AGENTS", None) or getattr(agents_mod, "_AGENTS", None)
    if not isinstance(store, dict):
        return {"ops": ops, "by_agent": by_agent, "ok": False}

    # store might be either:
    #   { "default": { "cpu-1": {...}, ... }, ... }
    # or:
    #   { "cpu-1": {...}, ... }  (flat)
    ns_bucket = store.get(namespace) if namespace in store else None
    items = ns_bucket.items() if isinstance(ns_bucket, dict) else store.items()

    for name, info in items:
        if not isinstance(info, dict):
            continue
        caps = info.get("capabilities")
        aops = _ops_from_capabilities(caps)
        if not aops:
            continue
        aname = str(name)
        by_agent[aname] = aops
        ops |= aops

    return {"ops": ops, "by_agent": by_agent, "ok": True}


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

    FIX: Validate op at submission time to avoid "accepted bad job that can never lease".
    """
    ns = (req.namespace or "").strip()
    if not ns:
        emit("JOB_REJECTED", namespace=ns, reason="missing_namespace", op=req.op)
        raise HTTPException(status_code=400, detail="namespace is required")

    op_name = (req.op or "").strip()
    if not op_name:
        emit("JOB_REJECTED", namespace=ns, reason="missing_op", op=req.op)
        raise HTTPException(status_code=400, detail="op is required")

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
        # ----------------------------
        # FIX: submit-time op validation
        # ----------------------------
        known = _collect_known_ops(ns)
        known_ops: Set[str] = set(known.get("ops") or set())
        by_agent: Dict[str, Set[str]] = dict(known.get("by_agent") or {})
        registry_ok: bool = bool(known.get("ok"))

        # Fail-open only if we couldn't read registry at all (cold start / module mismatch).
        # If registry is readable AND has any ops, enforce validation.
        if registry_ok and known_ops:
            if op_name not in known_ops:
                emit("JOB_REJECTED", namespace=ns, reason="UNKNOWN_OP", op=op_name, pinned_agent=pinned)
                raise HTTPException(
                    status_code=422,
                    detail={
                        "error": "UNKNOWN_OP",
                        "op": op_name,
                        "namespace": ns,
                        "hint": "Call /v1/agents?namespace=... to see supported ops.",
                        "available_ops_sample": sorted(list(known_ops))[:200],
                    },
                )

            if pinned:
                aops = by_agent.get(str(pinned))
                # If we know this agent and it can't run the op, reject immediately.
                if aops is not None and op_name not in aops:
                    emit(
                        "JOB_REJECTED",
                        namespace=ns,
                        reason="PINNED_AGENT_CANNOT_RUN_OP",
                        op=op_name,
                        pinned_agent=pinned,
                    )
                    raise HTTPException(
                        status_code=422,
                        detail={
                            "error": "PINNED_AGENT_CANNOT_RUN_OP",
                            "op": op_name,
                            "agent": pinned,
                            "namespace": ns,
                        },
                    )

        # Existing pinned availability check (liveness/health)
        if pinned:
            try:
                if not bool(is_avail(pinned)):
                    emit(
                        "JOB_REJECTED",
                        namespace=ns,
                        reason="requested_agent_unavailable",
                        pinned_agent=pinned,
                        op=op_name,
                    )
                    raise HTTPException(
                        status_code=409,
                        detail={"error": "requested_agent_unavailable", "agent": pinned},
                    )
            except HTTPException:
                raise
            except Exception:
                # If the health subsystem is broken, be conservative and reject pinning
                emit(
                    "JOB_REJECTED",
                    namespace=ns,
                    reason="requested_agent_unavailable",
                    pinned_agent=pinned,
                    op=op_name,
                )
                raise HTTPException(status_code=409, detail={"error": "requested_agent_unavailable", "agent": pinned})

        try:
            job_id = enqueue(
                op=str(op_name),
                payload=req.payload,
                job_id=str(caller_job_id) if caller_job_id else None,
                pinned_agent=pinned,
                excitatory_level=lvl_i,
                namespace=ns,
            )
            emit(
                "JOB_CREATED",
                namespace=ns,
                job_id=str(job_id),
                op=str(op_name),
                pinned_agent=pinned,
                priority=lvl_i,
                client_job_id=str(caller_job_id) if caller_job_id else None,
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"enqueue failed: {type(e).__name__}: {e}")

    return CreateJobResponse(ok=True, job_id=str(job_id))
