# controller/api/v1/__init__.py

from fastapi import APIRouter

# v1 router (mounted by app.py at /v1)
router = APIRouter()

# Canonical in-memory state for v1
from .state import AGENTS, JOBS, STATE_LOCK  # noqa: E402

# Sub-routers
from .jobs import router as jobs_router  # noqa: E402
from .leases import router as leases_router  # noqa: E402
from .results import router as results_router  # noqa: E402
from .read import router as read_router  # noqa: E402
from .events import router as events_router  # noqa: E402
from .heartbeat import router as heartbeat_router  # noqa: E402

from .audit import router as audit_router  # noqa: E402
router.include_router(audit_router, tags=["v1/audit"])

# Optional legacy/manual reclamation endpoint (safe to keep for now)
try:
    from .reclamation.agents import router as reclamation_agents_router  # noqa: E402
except Exception:
    reclamation_agents_router = None


# Write endpoints (mutations)
router.include_router(jobs_router, tags=["v1/jobs"])
router.include_router(leases_router, tags=["v1/leases"])
router.include_router(results_router, tags=["v1/results"])

# Read + realtime
router.include_router(read_router, tags=["v1/read"])
router.include_router(events_router, tags=["v1/events"])

# Control-plane hygiene
router.include_router(heartbeat_router, tags=["v1/agents"])

if reclamation_agents_router is not None:
    router.include_router(reclamation_agents_router, tags=["v1/reclamation"])


__all__ = ["router", "AGENTS", "JOBS", "STATE_LOCK"]
