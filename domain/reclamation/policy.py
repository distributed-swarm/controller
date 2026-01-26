from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class AgentReclamationPolicy:
    stale_after_seconds: int = 120
    delete_after_seconds: int = 3600
    max_delete_per_sweep: int = 200
