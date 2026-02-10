# controller/api/v1/read.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter()


# ---------- helpers ----------

def _require_namespace(namespace: Optional[str]) -> str:
    ns = (namespace or "").strip()
    if not ns:
        raise HTTPException(status_code=400, detail="namespace is required")
    return ns


def _parse_since(since: Optional[str]) -> Optional[datetime]:
    if not since:
        return None
    try:
        return datetime.fromisoformat(since.replace("Z", "+00:00"))
    except Exception:
        raise HTTPException(status_code=422, detail="since must be an ISO-8601 timestamp")


def _to_v1_state(internal_state: str) -> str:
    s = (internal_state or "").lower()
    if s == "completed":
        return "succeeded"
    return s


def _job_matches_filters(
    job: Dict[str, Any],
    state: Optional[str],
    agent: Optional[str],
    op: Optional[str],
    since_dt: Optional[datetime],
) -> bool:
    if state:
        want = state.lower()
        got = _to_v1_state(str(job.get("status", "")))
        if got != want:
            return False

    if agent:
        leased_by = job.get("leased_by") or job.get("agent") or job.get("worker") or job.get("claimed_by")
        pinned = job.get("pinned_agent")
        if str(leased_by) != agent and str(pinned) != agent:
            return False

    if op:
        if str(job.get("op", "")).lower() != op.lower():
            return False

    if since_dt:
        ts = job.get("updated_at") or job.get("created_at") or job.get("submitted_at")
        if not ts:
            return False
        try:
            job_dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            return False
        if job_dt < since_dt:
            return False

    return True


def _load_stores() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Pull in-memory stores from the running app module.
    IMPORTANT: This file may exist in repos where the runtime module is either
    'app' (image build) or 'controller.app' (editable install).
    We'll try both.
    """
    app_mod = None
    err: Optional[Exception] = None

    for modname in ("app", "controller.app"):
        try:
            app_mod = __import__(modname, fromlist=["*"])
            break
        except Exception as e:
            err = e
            continue

    if app_mod is None:
        raise HTTPException(status_code=500, detail=f"Runtime app module not available: {err}")

    jobs = getattr(app_mod, "JOBS", None)
    agents = getattr(app_mod, "AGENTS", None)

    if jobs is None or not isinstance(jobs, dict):
        jobs = {}
    if agents is None or not isinstance(agents, dict):
        agents = {}

    return jobs, agents


def _normalize_job_for_v1(job: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(job)
    out["state"] = _to_v1_state(str(job.get("status", "")))
    if out.get("status") == "completed":
        out["status"] = "completed"
    return out


def _agent_namespace(info: Dict[str, Any]) -> Optional[str]:
    # Allow multiple shapes:
    # - info["namespace"]
    # - info["labels"]["namespace"]
    ns = info.get("namespace")
    if ns:
        return str(ns)
    labels = info.get("labels")
    if isinstance(labels, dict) and labels.get("namespace"):
        return str(labels.get("namespace"))
    return None


# ---------- response models ----------

class JobsListResponse(BaseModel):
    jobs: List[Dict[str, Any]]


# ---------- endpoints ----------

@router.get("/jobs/{job_id}", response_model=Dict[str, Any])
def get_job(job_id: str, namespace: str = Query(..., description="Namespace (required)")) -> Dict[str, Any]:
    """
    Read one job. Namespace is required; mismatch returns 404 (no existence leak).
    """
    ns = _require_namespace(namespace)
    jobs, _agents = _load_stores()

    job = jobs.get(job_id)
    if not job or not isinstance(job, dict):
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")

    if str(job.get("namespace") or "") != ns:
        # Do not leak that the job exists in another namespace
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")

    out = _normalize_job_for_v1(job)
    out.setdefault("job_id", job_id)
    return out


@router.get("/jobs", response_model=JobsListResponse)
def list_jobs(
    namespace: str = Query(..., description="Namespace (required)"),
    state: Optional[str] = Query(default=None, description="Filter by state (queued|leased|running|succeeded|failed)"),
    agent: Optional[str] = Query(default=None, description="Filter by agent (leased_by or pinned_agent)"),
    op: Optional[str] = Query(default=None, description="Filter by op"),
    since: Optional[str] = Query(default=None, description="Filter by updated_at >= since (ISO-8601)"),
    limit: int = Query(default=200, ge=1, le=5000, description="Max jobs returned"),
) -> JobsListResponse:
    """
    Namespace-filtered job list.
    """
    ns = _require_namespace(namespace)
    since_dt = _parse_since(since)

    jobs, _agents = _load_stores()

    items: List[Dict[str, Any]] = []
    for jid, job in jobs.items():
        if not isinstance(job, dict):
            continue

        if str(job.get("namespace") or "") != ns:
            continue

        if _job_matches_filters(job, state, agent, op, since_dt):
            out = _normalize_job_for_v1(job)
            out.setdefault("job_id", jid)
            items.append(out)

    def sort_key(j: Dict[str, Any]) -> str:
        return str(j.get("updated_at") or j.get("created_at") or "")

    items.sort(key=sort_key, reverse=True)
    if len(items) > limit:
        items = items[:limit]

    return JobsListResponse(jobs=items)


@router.get("/agents", response_model=Dict[str, Any])
def list_agents(namespace: str = Query(..., description="Namespace (required)")) -> Dict[str, Any]:
    """
    Namespace-filtered agent list.
    """
    ns = _require_namespace(namespace)
    _jobs, agents = _load_stores()

    out: List[Dict[str, Any]] = []
    for name, info in agents.items():
        row = dict(info or {})
        row["name"] = name
        row["last_seen_at"] = (info or {}).get("last_seen")

        a_ns = _agent_namespace(row)
        if a_ns != ns:
            continue

        out.append(row)

    return {"agents": out}


@router.delete("/agents/{agent_name}", response_model=Dict[str, Any])
def delete_agent_v1(agent_name: str) -> Dict[str, Any]:
    from api.v1.agents import delete_agent  # local import avoids import-time coupling
    deleted = delete_agent(agent_name)
    return {"ok": True, "deleted": bool(deleted), "name": agent_name}
