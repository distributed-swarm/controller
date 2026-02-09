# controller/api/v1/read.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter()


# ---------- helpers ----------

def _parse_since(since: Optional[str]) -> Optional[datetime]:
    """
    Accepts ISO-ish timestamps. If parsing fails, we treat it as invalid input.
    """
    if not since:
        return None
    try:
        # Handles "2026-01-13T12:34:56" and "2026-01-13T12:34:56Z" (strip Z)
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
        # Try common fields. Your controller uses some combination of leased_by / pinned_agent / agent.
        leased_by = job.get("leased_by") or job.get("agent") or job.get("worker") or job.get("claimed_by")
        pinned = job.get("pinned_agent")
        # If filtering by agent, match either who leased it OR who it's pinned to.
        if str(leased_by) != agent and str(pinned) != agent:
            return False

    if op:
        if str(job.get("op", "")).lower() != op.lower():
            return False

    if since_dt:
        # Prefer updated_at, fall back to created_at / submitted_at
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
    Pull in-memory stores from the running app.py module.
    """
    try:
        import app as app  # type: ignore
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Runtime module 'app' not available: {e}",
        )

    jobs = getattr(app, "JOBS", None)
    agents = getattr(app, "AGENTS", None)

    if jobs is None or not isinstance(jobs, dict):
        jobs = {}
    if agents is None or not isinstance(agents, dict):
        agents = {}

    return jobs, agents


def _normalize_job_for_v1(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a shallow copy of the job with v1-facing state naming.
    """
    out = dict(job)
    out["state"] = _to_v1_state(str(job.get("status", "")))
    # Keep original too, but the UI should use "state"
    # out["status"] remains for backward visibility.
    if out.get("status") == "completed":
        out["status"] = "completed"  # keep internal for debugging
    return out


# ---------- response models ----------

class JobView(BaseModel):
    job_id: str
    op: str
    state: str
    payload: Dict[str, Any] = Field(default_factory=dict)

    # Optional metadata
    pinned_agent: Optional[str] = None
    leased_by: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    completed_at: Optional[str] = None

    # Terminal outputs
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None


class JobsListResponse(BaseModel):
    jobs: List[Dict[str, Any]]


# ---------- endpoints ----------

@router.get("/jobs/{job_id}", response_model=Dict[str, Any])
def get_job(job_id: str) -> Dict[str, Any]:
    """
    Single source of truth for one job (UI detail view).
    """
    jobs, _agents = _load_stores()
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    out = _normalize_job_for_v1(job)
    # Ensure job_id exists in output even if internal dict uses a different key.
    out.setdefault("job_id", job_id)
    return out


@router.get("/jobs", response_model=JobsListResponse)
def list_jobs(
    state: Optional[str] = Query(default=None, description="Filter by state (queued|leased|running|succeeded|failed)"),
    agent: Optional[str] = Query(default=None, description="Filter by agent (leased_by or pinned_agent)"),
    op: Optional[str] = Query(default=None, description="Filter by op"),
    since: Optional[str] = Query(default=None, description="Filter by updated_at >= since (ISO-8601)"),
    limit: int = Query(default=200, ge=1, le=5000, description="Max jobs returned"),
) -> JobsListResponse:
    """
    List/filter jobs for UI tables. Read-only.
    """
    since_dt = _parse_since(since)
    jobs, _agents = _load_stores()

    items: List[Dict[str, Any]] = []
    for jid, job in jobs.items():
        if not isinstance(job, dict):
            continue
        if _job_matches_filters(job, state, agent, op, since_dt):
            out = _normalize_job_for_v1(job)
            out.setdefault("job_id", jid)
            items.append(out)

    # Sort newest-first by updated_at (fallback created_at)
    def sort_key(j: Dict[str, Any]) -> str:
        return str(j.get("updated_at") or j.get("created_at") or "")

    items.sort(key=sort_key, reverse=True)
    if len(items) > limit:
        items = items[:limit]

    return JobsListResponse(jobs=items)


@router.get("/agents", response_model=Dict[str, Any])
def list_agents() -> Dict[str, Any]:
    """
    Return known agents (UI worker list).
    """
    jobs, agents = _load_stores()
    out = []
    for name, info in agents.items():
        row = dict(info or {})
        row["name"] = name
        row["last_seen_at"] = info.get("last_seen")
        out.append(row)
    return {"agents": out}
@router.delete("/agents/{agent_name}", response_model=Dict[str, Any])
def delete_agent_v1(agent_name: str) -> Dict[str, Any]:
    """
    Hard-delete an agent from the in-memory AGENTS store.
    Idempotent: returns ok even if the agent did not exist.
    """
    from api.v1.agents import delete_agent  # local import avoids import-time coupling

    deleted = delete_agent(agent_name)
    return {"ok": True, "deleted": bool(deleted), "name": agent_name}
    

