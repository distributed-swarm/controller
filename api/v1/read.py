# api/v1/read.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()


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
        ts = job.get("updated_at") or job.get("created_at") or job.get("submitted_at") or job.get("created_ts")
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
    # In the GHCR image, the runtime is /app/app.py as module "app".
    try:
        import app as app_mod  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Runtime app module not available: {e}")

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
    return out


def _agent_namespace(info: Dict[str, Any]) -> Optional[str]:
    ns = info.get("namespace")
    if ns:
        return str(ns)
    labels = info.get("labels")
    if isinstance(labels, dict) and labels.get("namespace"):
        return str(labels.get("namespace"))
    return None


def _is_active_agent(info: Dict[str, Any]) -> bool:
    """
    Active means: not tombstoned and (if lifecycle metadata exists) state == alive.

    We honor both:
      - tombstoned_at (api/v1/agents tombstone field)
      - _reap.state (lifecycle reaper metadata: alive/stale/tombstoned)
    """
    # If other subsystems explicitly tombstone agents, honor it.
    if info.get("tombstoned_at") is not None:
        return False

    meta = info.get("_reap")

    # Most common: dict persisted in AGENTS store
    if isinstance(meta, dict):
        state = (meta.get("state") or "").lower()
        if state and state != "alive":
            return False
        return True

    # Defensive: if meta is ever a dataclass instance
    state = (getattr(meta, "state", "") or "").lower()
    if state and state != "alive":
        return False

    return True


class JobsListResponse(BaseModel):
    jobs: List[Dict[str, Any]]


# PATCH: namespace is OPTIONAL for GET by id (defaults to "default")
@router.get("/jobs/{job_id}", response_model=Dict[str, Any])
def get_job(
    job_id: str,
    namespace: str = Query("default", description="Namespace (optional; defaults to 'default')"),
) -> Dict[str, Any]:
    ns = (namespace or "default").strip()
    jobs, _agents = _load_stores()

    job = jobs.get(job_id)
    if not job or not isinstance(job, dict):
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")

    if str(job.get("namespace") or "") != ns:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")

    out = _normalize_job_for_v1(job)
    out.setdefault("job_id", job_id)
    return out


@router.get("/jobs", response_model=JobsListResponse)
def list_jobs(
    namespace: str = Query(..., description="Namespace (required)"),
    state: Optional[str] = Query(default=None),
    agent: Optional[str] = Query(default=None),
    op: Optional[str] = Query(default=None),
    since: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=5000),
) -> JobsListResponse:
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

    items.sort(key=lambda j: str(j.get("updated_at") or j.get("created_at") or j.get("created_ts") or ""), reverse=True)
    if len(items) > limit:
        items = items[:limit]
    return JobsListResponse(jobs=items)


@router.get("/agents", response_model=Dict[str, Any])
def list_agents(
    namespace: str = Query(..., description="Namespace (required)"),
    include_tombstoned: bool = Query(default=False, description="Include stale/tombstoned agents"),
) -> Dict[str, Any]:
    ns = _require_namespace(namespace)
    _jobs, agents = _load_stores()

    out: List[Dict[str, Any]] = []
    for name, info in agents.items():
        row = dict(info or {})
        row["name"] = name

        a_ns = _agent_namespace(row)
        if a_ns != ns:
            continue

        if not include_tombstoned and not _is_active_agent(row):
            continue

        out.append(row)

    return {"agents": out}
