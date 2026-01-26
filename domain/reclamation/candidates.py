from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Sequence, List, Dict, Any

from domain.reclamation.policy import AgentReclamationPolicy
from domain.reclamation.state_machine import classify_agent_state, AgentGCState

@dataclass(frozen=True)
class AgentSnapshot:
    name: str
    last_seen: Optional[float]
    tombstoned_at: Optional[float]

@dataclass(frozen=True)
class AgentAction:
    name: str
    state: str
    last_seen: Optional[float]
    tombstoned_at: Optional[float]

@dataclass(frozen=True)
class AgentSweepPlan:
    to_tombstone: List[AgentAction]
    to_delete: List[AgentAction]

def plan_agent_sweep(
    *,
    now: float,
    agents: Sequence[AgentSnapshot],
    policy: AgentReclamationPolicy,
) -> AgentSweepPlan:
    to_tombstone: List[AgentAction] = []
    to_delete: List[AgentAction] = []

    for a in agents:
        state = classify_agent_state(
            now=now,
            last_seen=a.last_seen,
            tombstoned_at=a.tombstoned_at,
            stale_after_seconds=policy.stale_after_seconds,
            delete_after_seconds=policy.delete_after_seconds,
        )

        if state == AgentGCState.STALE:
            to_tombstone.append(
                AgentAction(name=a.name, state=state.value, last_seen=a.last_seen, tombstoned_at=a.tombstoned_at)
            )
        elif state == AgentGCState.DELETABLE:
            to_delete.append(
                AgentAction(name=a.name, state=state.value, last_seen=a.last_seen, tombstoned_at=a.tombstoned_at)
            )

    # Safety valve
    if len(to_delete) > policy.max_delete_per_sweep:
        to_delete = to_delete[: policy.max_delete_per_sweep]

    return AgentSweepPlan(to_tombstone=to_tombstone, to_delete=to_delete)
