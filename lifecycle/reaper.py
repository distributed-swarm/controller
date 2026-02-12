# api/v1/reaper.py
import os
import time
import threading
from dataclasses import dataclass
from typing import Dict, Any, Callable, Optional


REAPER_INTERVAL_S = float(os.getenv("REAPER_INTERVAL_S", "2"))
STALE_AFTER_S = float(os.getenv("STALE_AFTER_S", "15"))
TOMBSTONE_AFTER_S = float(os.getenv("TOMBSTONE_AFTER_S", "30"))
DELETE_AFTER_S = float(os.getenv("DELETE_AFTER_S", "60"))

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


def start_reaper(
    agents: Dict[str, Dict[str, Any]],
    jobs: Dict[str, Dict[str, Any]],
    publish_event: Callable[[str, Dict[str, Any]], None],
    lock: threading.Lock,
) -> threading.Thread:
    """
    agents: mapping agent_name -> agent_record
      agent_record SHOULD contain last_seen: float (unix seconds)  [controller-native]
      agent_record MAY contain last_heartbeat_ts: float (unix seconds) [compat]
      agent_record MAY contain _reap: AgentReapMeta (we add if missing)

    jobs: mapping job_id -> job_record with:
      leased_by: Optional[str]
      lease_expires_at: Optional[float]  (unix seconds)
      state/status fields may exist; we only force lease expiry

    publish_event(event_name, data)
    lock: same lock used by request handlers for agents/jobs stores
    """

    def loop() -> None:
        while True:
            time.sleep(REAPER_INTERVAL_S)
            now = _now()

            with lock:
                to_delete = []

                for name, a in list(agents.items()):
                    meta: Optional[AgentReapMeta] = a.get("_reap")
                    if meta is None:
                        meta = AgentReapMeta()
                        a["_reap"] = meta

                    # Prefer controller-native field name, but accept older/newer variants
                    last = a.get("last_seen")
                    if last is None:
                        last = a.get("last_heartbeat_ts")
                    if last is None:
                        # If an agent record has no heartbeat timestamp at all, treat as dead.
                        last = 0.0

                    # ALIVE -> STALE
                    if meta.state == AGENT_ALIVE and (now - float(last)) > STALE_AFTER_S:
                        meta.state = AGENT_STALE
                        meta.stale_since_ts = now

                        publish_event(
                            "agent.stale",
                            {
                                "agent": name,
                                "last_seen": last,
                                "now": now,
                                "stale_after_s": STALE_AFTER_S,
                            },
                        )

                        # Force-expire any leases held by this agent (requeue now)
                        for job_id, job in jobs.items():
                            if job.get("leased_by") == name:
                                job["lease_expires_at"] = 0.0
                                publish_event(
                                    "lease.forced_expire",
                                    {
                                        "job_id": job_id,
                                        "agent": name,
                                        "reason": "agent_stale",
                                    },
                                )

                    # STALE -> TOMBSTONED
                    if meta.state == AGENT_STALE and meta.stale_since_ts is not None:
                        if (now - meta.stale_since_ts) > TOMBSTONE_AFTER_S:
                            meta.state = AGENT_TOMBSTONED
                            meta.tombstoned_ts = now
                            publish_event(
                                "agent.tombstoned",
                                {
                                    "agent": name,
                                    "stale_since_ts": meta.stale_since_ts,
                                    "now": now,
                                    "tombstone_after_s": TOMBSTONE_AFTER_S,
                                },
                            )

                    # TOMBSTONED -> DELETE
                    if meta.state == AGENT_TOMBSTONED and meta.tombstoned_ts is not None:
                        if (now - meta.tombstoned_ts) > DELETE_AFTER_S:
                            to_delete.append(name)

                for name in to_delete:
                    agents.pop(name, None)
                    publish_event(
                        "agent.deleted",
                        {
                            "agent": name,
                            "now": now,
                            "delete_after_s": DELETE_AFTER_S,
                        },
                    )

    t = threading.Thread(target=loop, name="agent-reaper", daemon=True)
    t.start()
    return t
