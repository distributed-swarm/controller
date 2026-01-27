# controller/domain/reclamation/state_machine.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

from .policy import AgentReclamationPolicy


@dataclass(frozen=True)
class AgentSnapshot:
    """
    Minimal immutable view of an agent needed for reclamation decisions.
    """
    name: str
    last_seen: Optional[float] = None
    tombstoned_at: Optional[float] = None


@dataclass(frozen=True)
class AgentSweepPlan:
    """
    Output of the domain decision step.
    The API layer applies these via store-ops (tombstone/delete).
    """
    to_tombstone: List[AgentSnapshot]
    to_delete: List[AgentSnapshot]


def plan_agent_sweep(
    *,
    now: float,
    agents: Iterable[AgentSnapshot],
    policy: AgentReclamationPolicy,
) -> AgentSweepPlan:
    """
    Pure domain logic. No FastAPI. No storage writes.

    Rules:
      - If tombstoned_at is None and agent is stale => tombstone candidate.
      - If tombstoned_at is set and tombstoned long enough => delete candidate.
    """
    stale_after = float(getattr(policy, "stale_after_seconds", 120))
    delete_after = float(getattr(policy, "delete_after_seconds", 3600))

    to_tombstone: List[AgentSnapshot] = []
    to_delete: List[AgentSnapshot] = []

    for a in agents:
        # Already tombstoned? Maybe delete.
        if a.tombstoned_at is not None:
            if (now - a.tombstoned_at) >= delete_after:
                to_delete.append(a)
            continue

        # Not tombstoned: decide staleness.
        if a.last_seen is None:
            # No heartbeat recorded -> treat as stale immediately.
            to_tombstone.append(a)
            continue

        if (now - a.last_seen) >= stale_after:
            to_tombstone.append(a)

    return AgentSweepPlan(to_tombstone=to_tombstone, to_delete=to_delete)
