# ~/projects/controller/brainstem_tasks.py

import time
import asyncio
import threading
from enum import Enum
from typing import Dict, Any, Optional

# -----------------------------
# Task state & config
# -----------------------------

class TaskState(str, Enum):
    PENDING = "pending"
    LEASED = "leased"
    COMPLETED = "completed"


# Per-op default lease timeouts (seconds)
LEASE_TIMEOUT_DEFAULTS: Dict[str, int] = {
    "map_classify": 30,
    # add more ops here as needed
}


class Task:
    def __init__(
        self,
        task_id: str,
        op: str,
        payload: Dict[str, Any],
        lease_timeout_sec: Optional[int] = None,
    ) -> None:
        self.id = task_id
        self.op = op
        self.payload = payload

        self.state: TaskState = TaskState.PENDING
        self.leased_agent: Optional[str] = None
        self.leased_at: Optional[float] = None

        if lease_timeout_sec is None:
            lease_timeout_sec = LEASE_TIMEOUT_DEFAULTS.get(op, 30)
        self.lease_timeout_sec: int = lease_timeout_sec

        self.result: Optional[Dict[str, Any]] = None

    def to_lease_payload(self) -> Dict[str, Any]:
        """What gets sent to an agent on /lease."""
        return {
            "id": self.id,
            "op": self.op,
            "payload": self.payload,
            "lease_timeout_sec": self.lease_timeout_sec,
        }


# -----------------------------
# Global store
# -----------------------------

TASKS: Dict[str, Task] = {}
TASK_LOCK = threading.Lock()

# For simple stats; you can ignore these if you already track elsewhere
LEASE_RECLAIMED_COUNT = 0


# -----------------------------
# Enqueue / create tasks
# -----------------------------

def enqueue_task(task_id: str, op: str, payload: Dict[str, Any]) -> None:
    """Create a new task and store it as pending."""
    task = Task(task_id=task_id, op=op, payload=payload)
    with TASK_LOCK:
        TASKS[task_id] = task


# -----------------------------
# Leasing logic (Option A)
# -----------------------------

def lease_task_for_agent(agent_name: str, requested_op: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Find the next pending task (optionally filtered by op), mark it leased, and
    return the payload for the agent. Returns None if no work is available.
    """
    now = time.time()
    with TASK_LOCK:
        for task in TASKS.values():
            if task.state != TaskState.PENDING:
                continue
            if requested_op and task.op != requested_op:
                continue

            task.state = TaskState.LEASED
            task.leased_agent = agent_name
            task.leased_at = now

            return task.to_lease_payload()

    return None


# -----------------------------
# Completion logic (idempotent)
# -----------------------------

def complete_task_result(
    task_id: str,
    agent_name: Optional[str],
    op: Optional[str],
    result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Idempotent completion logic:

    - If task is pending or leased: accept result, mark completed.
    - If completed: ignore, report "duplicate".
    - If unknown ID: ignore, report "unknown".
    """
    with TASK_LOCK:
        task = TASKS.get(task_id)
        if task is None:
            # Unknown task; don't encourage retries
            return {"status": "ignored", "reason": "unknown_task_id"}

        # Optional sanity check: op mismatch
        if op and task.op != op:
            # You can add logging here if you want
            pass

        if task.state in (TaskState.PENDING, TaskState.LEASED):
            # Here is where you could add per-op schema validation of `result`
            task.result = result
            task.state = TaskState.COMPLETED
            return {"status": "ok"}

        if task.state == TaskState.COMPLETED:
            # Idempotent: late or duplicate completion, ignore impact
            return {"status": "ignored", "reason": "duplicate_completion"}

    # Fallback (shouldn't hit)
    return {"status": "ignored", "reason": "unexpected_state"}


# -----------------------------
# Reaper loop (lease timeouts)
# -----------------------------

async def task_reaper_loop(interval_sec: float = 1.0) -> None:
    """
    Periodically scans for leased tasks that have exceeded their lease_timeout_sec
    and returns them to pending state.
    """
    global LEASE_RECLAIMED_COUNT

    while True:
        now = time.time()
        reclaimed = 0

        with TASK_LOCK:
            for task in TASKS.values():
                if task.state != TaskState.LEASED:
                    continue
                if task.leased_at is None:
                    continue

                deadline = task.leased_at + task.lease_timeout_sec
                if now > deadline:
                    task.state = TaskState.PENDING
                    task.leased_at = None
                    task.leased_agent = None
                    reclaimed += 1

            LEASE_RECLAIMED_COUNT += reclaimed

        # You can replace this with proper logging if desired
        # if reclaimed:
        #     print(f"[reaper] reclaimed {reclaimed} tasks (total {LEASE_RECLAIMED_COUNT})")

        await asyncio.sleep(interval_sec)


# -----------------------------
# Stats helper
# -----------------------------

def get_task_stats() -> Dict[str, Any]:
    """Basic view into current task states. You can merge this into /stats."""
    with TASK_LOCK:
        total = len(TASKS)
        pending = sum(1 for t in TASKS.values() if t.state == TaskState.PENDING)
        leased = sum(1 for t in TASKS.values() if t.state == TaskState.LEASED)
        completed = sum(1 for t in TASKS.values() if t.state == TaskState.COMPLETED)

        return {
            "tasks_total": total,
            "tasks_pending": pending,
            "tasks_leased": leased,
            "tasks_completed": completed,
            "tasks_reclaimed": LEASE_RECLAIMED_COUNT,
        }
