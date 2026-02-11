# controller/api/v1/state.py
import threading
from typing import Any, Dict

STATE_LOCK = threading.Lock()

# Canonical in-memory stores for v1
AGENTS: Dict[str, Dict[str, Any]] = {}
JOBS: Dict[str, Dict[str, Any]] = {}
