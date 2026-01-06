# controller/brainstem.py
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional, Tuple


@dataclass
class OpPerf:
    ema_ms: float
    samples: int = 0


@dataclass
class InhibitState:
    agent: str
    until_ts: float


class Brainstem:
    """
    Controller brainstem: policy + scheduling helpers.

    Design goals:
      - No FastAPI imports
      - Operates on passed-in state dicts/deques (owned by app)
      - Keeps its own small internal state (perf + inhibition) only
      - Can be wired into app with minimal disruption
    """

    def __init__(
        self,
        *,
        perf_min_samples: int = 5,
        inhibit_ttl_sec: float = 20.0,
        dominate_ratio: float = 0.70,   # winner if ema <= median * dominate_ratio
        ema_alpha: float = 0.10,        # new = old*(1-a) + x*a
    ) -> None:
        self.perf_min_samples = int(perf_min_samples)
        self.inhibit_ttl_sec = float(inhibit_ttl_sec)
        self.dominate_ratio = float(dominate_ratio)
        self.ema_alpha = float(ema_alpha)

        # agent -> op -> OpPerf
        self._perf: Dict[str, Dict[str, OpPerf]] = defaultdict(dict)
        # op -> InhibitState
        self._inhibit: Dict[str, InhibitState] = {}

    # -------------------------------------------------------------------------
    # Perf + inhibition (result-side)
    # -------------------------------------------------------------------------

    def record_result(
        self,
        *,
        agent: Optional[str],
        op: str,
        duration_ms: Optional[float],
        ok: bool,
        now: Optional[float] = None,
    ) -> None:
        """
        Update per-(agent,op) performance on successful results and
        possibly set short-lived inhibition for the op.

        Safe defaults:
          - only uses duration_ms if ok and duration_ms is valid
          - requires perf_min_samples before using an agent as a "winner"
        """
        if now is None:
            now = time.time()

        if not agent:
            return

        op = str(op or "unknown")

        # Only learn speed from successes with a valid duration
        if not ok or duration_ms is None:
            return

        try:
            x = float(duration_ms)
        except (TypeError, ValueError):
            return
        if x <= 0:
            return

        # Update EMA for this agent/op
        p = self._perf[agent].get(op)
        if p is None:
            p = OpPerf(ema_ms=x, samples=0)

        # EMA update
        a = self.ema_alpha
        p.ema_ms = (p.ema_ms * (1.0 - a)) + (x * a)
        p.samples += 1
        self._perf[agent][op] = p

        # Not enough samples? don't inhibit yet.
        if p.samples < self.perf_min_samples:
            return

        # Compute median across peers for this op
        peer_emas = []
        for _agent, ops in self._perf.items():
            pp = ops.get(op)
            if pp and pp.samples >= self.perf_min_samples:
                peer_emas.append(pp.ema_ms)

        if len(peer_emas) < 3:
            return

        peer_emas.sort()
        median = peer_emas[len(peer_emas) // 2]

        # Winner rule: materially better than typical peer
        if median > 0 and p.ema_ms <= median * self.dominate_ratio:
            self._inhibit[op] = InhibitState(agent=agent, until_ts=now + self.inhibit_ttl_sec)

    # -------------------------------------------------------------------------
    # Scheduling-side helper
    # -------------------------------------------------------------------------

    def inhibited_winner(self, op: str, now: Optional[float] = None) -> Optional[str]:
        """
        Returns the winning agent if op is currently inhibited, else None.
        """
        if now is None:
            now = time.time()
        st = self._inhibit.get(str(op or "unknown"))
        if not st:
            return None
        if now >= st.until_ts:
            # expire lazily
            self._inhibit.pop(str(op or "unknown"), None)
            return None
        return st.agent

    def allow_lease(self, *, requesting_agent: str, op: str, now: Optional[float] = None) -> bool:
        """
        Hard inhibition gate:
          - if op is inhibited and requesting agent is not the winner, deny
          - else allow
        """
        winner = self.inhibited_winner(op, now=now)
        if winner is None:
            return True
        return str(requesting_agent) == str(winner)

    # -------------------------------------------------------------------------
    # Debug/introspection
    # -------------------------------------------------------------------------

    def snapshot(self, now: Optional[float] = None) -> Dict[str, Any]:
        """
        Returns a small debug snapshot you can expose via an endpoint later.
        """
        if now is None:
            now = time.time()

        inhibit = {}
        for op, st in list(self._inhibit.items()):
            if now >= st.until_ts:
                continue
            inhibit[op] = {"agent": st.agent, "until_ts": st.until_ts, "ttl_s": max(0.0, st.until_ts - now)}

        # Keep perf output small: only show agents/ops that have enough samples
        perf = {}
        for agent, ops in self._perf.items():
            for op, p in ops.items():
                if p.samples >= self.perf_min_samples:
                    perf.setdefault(agent, {})[op] = {"ema_ms": p.ema_ms, "samples": p.samples}

        return {"inhibit": inhibit, "perf": perf}
