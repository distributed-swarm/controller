# controller/api/v1/__init__.py
from fastapi import APIRouter

# Import sub-routers (we'll implement these next, one file at a time)
from .jobs import router as jobs_router
from .leases import router as leases_router
from .results import router as results_router
from .read import router as read_router
from .events import router as events_router
from .reclamation.agents import router as reclamation_agents_router

router = APIRouter()

# Write endpoints
router.include_router(jobs_router, tags=["v1/jobs"])
router.include_router(leases_router, tags=["v1/leases"])
router.include_router(results_router, tags=["v1/results"])

# Read endpoints + realtime
router.include_router(read_router, tags=["v1/read"])
router.include_router(events_router, tags=["v1/events"])
router.include_router(reclamation_agents_router, tags=["v1/reclamation"])

