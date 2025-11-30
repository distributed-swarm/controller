# health_policy.py
import time
from typing import Iterable, Tuple

# This is the central config for all health decisions.
HEALTH_POLICY = {
    "heartbeat_interval_sec": 15,
    "suspect_heartbeat_age_sec": 40,
    "dead_heartbeat_age_sec": 120,

    "modes": {
        "aggressive": {
            "timeout_rate_suspect": 0.05,       # 5%
            "timeout_rate_quarantine": 0.15,    # 15%
            "consec_timeouts_quarantine": 5,
        },
        "conservative": {
            "timeout_rate_suspect": 0.10,       # 10%
            "timeout_rate_quarantine": 0.25,    # 25%
            "consec_timeouts_quarantine": 8,
        },
    },

    "min_tasks_for_rate": 30,

    "ban": {
        "max_consec_timeouts": 20,
        "max_timeout_rate": 0.40,
        "window_tasks": 200,
    },

    "recovery": {
        "clean_tasks": 50,
        "max_timeout_rate": 0.02,
        "needed_heartbeats": 4,
    },
}


def choose_policy_mode(agents: Iterable) -> str:
    """
    Decide whether we are in aggressive or conservative mode
    based on cluster capacity.
    """
    total_workers = 0
    healthy_workers = 0
    healthy_agents = 0

    for a in agents:
        total_workers += getattr(a.worker_profile, "max_total_workers", 0)
        if a.state == "healthy":
            healthy_workers += getattr(a.worker_profile, "max_total_workers", 0)
            healthy_agents += 1

    if total_workers == 0:
        return "conservative"

    healthy_ratio = healthy_workers / total_workers
    if healthy_ratio >= 0.7 and healthy_agents >= 5:
        return "aggressive"
    return "conservative"


def evaluate_agent_health(agent, mode: str, policy: dict = HEALTH_POLICY) -> Tuple[str, str]:
    """
    Decide the new health state for an agent.

    Returns:
        (new_state, reason_string)
    """
    now = time.time()
    state = agent.state
    reason = "no_change"

    # You will need these attributes on your agent object:
    # - last_heartbeat: float (timestamp)
    # - timeout_stats: object with total_tasks, timeouts, consec_timeouts,
    #                  clean_tasks, clean_timeout_rate
    # - recent_good_heartbeats: int
    # - worker_profile.max_total_workers (for choose_policy_mode)

    # 1) Heartbeat-based checks
    age = now - getattr(agent, "last_heartbeat", 0)
    dead_age = policy["dead_heartbeat_age_sec"]
    suspect_age = policy["suspect_heartbeat_age_sec"]

    if age > dead_age:
        return "dead", f"heartbeat_age>{dead_age}"
    if age > suspect_age and state == "healthy":
        return "suspect", f"heartbeat_age>{suspect_age}"

    mode_cfg = policy["modes"][mode]

    # 2) Timeout-based checks
    stats = agent.timeout_stats
    n_tasks = getattr(stats, "total_tasks", 0)
    timeouts = getattr(stats, "timeouts", 0)
    consec_timeouts = getattr(stats, "consec_timeouts", 0)
    timeout_rate = (timeouts / n_tasks) if n_tasks > 0 else 0.0

    # Ban rules (if enough data)
    ban_cfg = policy["ban"]
    if consec_timeouts >= ban_cfg["max_consec_timeouts"]:
        return "banned", f"consec_timeouts>={ban_cfg['max_consec_timeouts']}"
    if (
        n_tasks >= ban_cfg["window_tasks"]
        and timeout_rate >= ban_cfg["max_timeout_rate"]
    ):
        return "banned", f"timeout_rate>={ban_cfg['max_timeout_rate']}"

    # Quarantine / suspect (only if enough tasks for rate-based rules)
    min_tasks = policy["min_tasks_for_rate"]
    if n_tasks >= min_tasks:
        if timeout_rate >= mode_cfg["timeout_rate_quarantine"]:
            return "quarantined", f"timeout_rate>={mode_cfg['timeout_rate_quarantine']}"

        if timeout_rate >= mode_cfg["timeout_rate_suspect"] and state == "healthy":
            return "suspect", f"timeout_rate>={mode_cfg['timeout_rate_suspect']}"

    # Consecutive timeout rule (independent of rate)
    if consec_timeouts >= mode_cfg["consec_timeouts_quarantine"]:
        return "quarantined", f"consec_timeouts>={mode_cfg['consec_timeouts_quarantine']}"

    # 3) Recovery (from quarantined → healthy)
    if state == "quarantined":
        rec = policy["recovery"]
        clean_tasks = getattr(stats, "clean_tasks", 0)
        clean_timeout_rate = getattr(stats, "clean_timeout_rate", 0.0)
        good_hb = getattr(agent, "recent_good_heartbeats", 0)

        if (
            clean_tasks >= rec["clean_tasks"]
            and clean_timeout_rate <= rec["max_timeout_rate"]
            and good_hb >= rec["needed_heartbeats"]
        ):
            return "healthy", "recovered"

    return state, reason
