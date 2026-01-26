from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Optional

class AgentGCState(str, Enum):
    LIVE = "live"
    STALE = "stale"
    TOMBSTONED = "tombstoned"
    DELETABLE = "deletable"

def classify_agent_state(
    *,
    now: float,
    last_seen: Optional[float],
    tombstoned_at: Optional[float],
    stale_after_seconds: int,
    delete_after_seconds: int,
) -> AgentGCState:
    if tombstoned_at is not None:
        if now - tombstoned_at >= delete_after_seconds:
            return AgentGCState.DELETABLE
        return AgentGCState.TOMBSTONED

    if last_seen is None:
        # Never seen -> treat as stale.
        return AgentGCState.STALE

    if now - last_seen >= stale_after_seconds:
        return AgentGCState.STALE

    return AgentGCState.LIVE
