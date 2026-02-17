# lifecycle/reaper.py
#
# The Reaper walks quietly.
#
# It does not schedule.
# It does not judge.
# It does not cancel.
#
# It simply observes who has stopped breathing,
# marks the time,
# and ensures the system does not lie about the dead.
#
# Agents go: alive → stale → tombstoned → deleted.
#
# No drama.
# No zombies.
# No ghosts.

import os
import time
import threading
from dataclasses import dataclass
from typing import Dict, Any, Callable, Optional, Tuple

# Canonical helpers. These should be the *only* place that mutates agent lifecycle state.
# If these helpers currently live under api.v1.agents, that's fine for now—what we're
# explicitly removing is any "API reaper" / route-level reaping logic.
from api.v1.agents import tombstone_agent, delete_agent


# -----------------------------
# Tunables (seconds)
# -----------------------------
REAPER_INTERVAL_S = float(os.getenv("REAPER_INTERVAL_S", "2"))
STALE_AFTER_S = float(os.getenv("STALE_AFTER_S", "15"))
TOMBSTONE_AFTER_S = float(os.getenv("TOMBSTONE_AFTER_S", "30"))
DELETE_AFTER_S = float(os.getenv("DELETE_AFTER_S", "60"))

# If you want very chatty logs in demos, set REAPER_LOG_EVERY_N=1.
# Otherwise it will log "idle cycles" every N loops.
REAPER_LOG_EVERY_N = int(os.getenv("REAPER_LOG_EVERY_N", "10"))

AGENT_ALIVE = "alive"
AGENT_STALE = "stale"
AGENT_TOMBSTONED = "tombstoned"


@dataclass
class AgentReapMeta:
    state: str = AGENT_ALIVE
    stale_since_ts: Optional[float] = None
    tombstoned_ts: Optional[float] = None


def _now() -> float:
    return time.time()


def _get_last_seen(agent_record: Dict[str, Any]) -> float:
    """
    Prefer controller-native field. Fall back to compat. If missing, treat as 0.0.
    """
    last = agent_record.get("last_seen")
    if last is None:
        last = agent_record.get("last_heartbeat_ts")
    if last is None:
        return 0.0
    try:
        return float(last)
    except Exception:
        return 0.0


def _meta(agent_record: Dict[str, Any]) -> AgentReapMeta:
    meta = agent_record.get("_reap")
    if isinstance(meta, AgentReapMeta):
        return meta
    # If prior code wrote a dict or something weird, normalize.
    meta_obj = AgentReapMeta()
    agent_record["_reap"] = meta_obj
    return meta_obj


def _log(msg: str) -> None:
    # Keeping it dead simple: grep-friendly and demo-friendly.
    print(msg, flush=True)


def start_reaper(
    agents: Dict[str, Dict[str, Any]],
    jobs: Dict[str, Dict[str, Any]],
    publish_event: Callable[[str, Dict[str, Any]], None],
    lock: threading.Lock,
) -> threading.Thread:
    """
    Start the agent reaper thread.

    agents: mapping agent_name -> agent_record
      agent_record SHOULD contain last_seen: float (unix seconds)
      agent_record MAY contain last_heartbeat_ts: float (compat)
      agent_record MAY contain _reap: AgentReapMeta (added if missing)

    jobs: mapping job_id -> job_record
      Reaper does not mutate job/lease state.

    publish_event(event_name, data)
    lock: same lock used by request handlers

    NOTE: This is intentionally lifecycle-driven (startup -> thread -> loop).
          Do not add API endpoints that "trigger reaping".
    """

    # Guard against double-start (uvicorn reload, accidental double import, etc.)
    # We attach state to the function object to avoid globals leaking across modules.
    if getattr(start_reaper, "_thread", None) is not None:
        t = getattr(start_reaper, "_thread")
        if isinstance(t, threading.Thread) and t.is_alive():
            _log("[reaper] already running; skip start")
            return t

    def loop() -> None:
        _log(
            "[reaper] started "
            f"(interval={REAPER_INTERVAL_S}s stale_after={STALE_AFTER_S}s "
            f"tombstone_after={TOMBSTONE_AFTER_S}s delete_after={DELETE_AFTER_S}s)"
        )

        cycle = 0
        while True:
            cycle += 1
            now = _now()

            # We compute decisions while holding the lock so we're consistent with request handlers.
            # We call delete/tombstone helpers while still under lock to keep lifecycle transitions atomic
            # relative to other writes (this matches your previous behavior).
            with lock:
                total = len(agents)
                stale_transitions = 0
                tombstones = 0
                deletions = 0

                # Collect deletions to perform after iteration.
                to_delete: list[str] = []

                # Debug counters for narrative clarity
                alive_count = 0
                stale_count = 0
                tomb_count = 0

                for name, a in list(agents.items()):
                    meta = _meta(a)
                    last = _get_last_seen(a)
                    age = now - last

                    # Track current state counts
                    if meta.state == AGENT_ALIVE:
                        alive_count += 1
                    elif meta.state == AGENT_STALE:
                        stale_count += 1
                    elif meta.state == AGENT_TOMBSTONED:
                        tomb_count += 1

                    # ---------------------------
                    # ALIVE -> STALE
                    # ---------------------------
                    if meta.state == AGENT_ALIVE and age > STALE_AFTER_S:
                        meta.state = AGENT_STALE
                        meta.stale_since_ts = now
                        stale_transitions += 1

                        publish_event(
                            "agent.stale",
                            {
                                "agent": name,
                                "last_seen": last,
                                "now": now,
                                "age_s": age,
                                "stale_after_s": STALE_AFTER_S,
                            },
                        )

                        _log(
                            f"[reaper] stale agent={name} last_seen={last:.3f} "
                            f"age_s={age:.3f} threshold_s={STALE_AFTER_S}"
                        )

                    # ---------------------------
                    # STALE -> TOMBSTONED
                    # ---------------------------
                    if meta.state == AGENT_STALE and meta.stale_since_ts is not None:
                        stale_for = now - meta.stale_since_ts
                        if stale_for > TOMBSTONE_AFTER_S:
                            meta.state = AGENT_TOMBSTONED
                            meta.tombstoned_ts = now
                            tombstones += 1

                            _log(
                                f"[reaper] tombstone agent={name} stale_for_s={stale_for:.3f} "
                                f"threshold_s={TOMBSTONE_AFTER_S}"
                            )

                            # Canonical helper (should emit its own event too)
                            tombstone_agent(name, reason="reaper")

                    # ---------------------------
                    # TOMBSTONED -> DELETE
                    # ---------------------------
                    if meta.state == AGENT_TOMBSTONED and meta.tombstoned_ts is not None:
                        tomb_for = now - meta.tombstoned_ts
                        if tomb_for > DELETE_AFTER_S:
                            to_delete.append(name)

                # Perform deletions after iteration
                for name in to_delete:
                    deletions += 1
                    _log(
                        f"[reaper] delete agent={name} "
                        f"(tombstoned_for_s>{DELETE_AFTER_S})"
                    )
                    # Canonical helper (should emit its own event too)
                    delete_agent(name)

                # Periodic heartbeat log for narrative discipline (and demo sanity)
                if (
                    stale_transitions == 0
                    and tombstones == 0
                    and deletions == 0
                    and (cycle % max(REAPER_LOG_EVERY_N, 1) == 0)
                ):
                    _log(
                        f"[reaper] idle cycle={cycle} agents_total={total} "
                        f"alive={alive_count} stale={stale_count} tombstoned={tomb_count}"
                    )
                else:
                    # If we did any action, always log a summary line.
                    if stale_transitions or tombstones or deletions:
                        _log(
                            f"[reaper] cycle={cycle} agents_total={total} "
                            f"stale_transitions={stale_transitions} tombstones={tombstones} deletions={deletions}"
                        )

            # Sleep at end so the first pass happens immediately after startup.
            time.sleep(REAPER_INTERVAL_S)

    t = threading.Thread(target=loop, name="agent-reaper", daemon=True)
    t.start()

    setattr(start_reaper, "_thread", t)
    return t
