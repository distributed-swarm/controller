# controller/api/v1/events.py
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

router = APIRouter()

# --- Simple in-process pub/sub for SSE subscribers ---
_SUBSCRIBERS: List[asyncio.Queue] = []
_SUBSCRIBERS_LOCK = asyncio.Lock()

# Main event loop reference (used to publish from sync/threadpool contexts)
_MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

# How often to send keepalive comments (seconds)
_KEEPALIVE_INTERVAL_S = 15.0


def _sse_format(event_type: str, data: Dict[str, Any]) -> str:
    """
    SSE format: event + data + blank line.
    'data:' must be single-line. JSON dumps is fine.
    """
    payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    return f"event: {event_type}\ndata: {payload}\n\n"


async def _add_subscriber(q: asyncio.Queue) -> None:
    async with _SUBSCRIBERS_LOCK:
        _SUBSCRIBERS.append(q)


async def _remove_subscriber(q: asyncio.Queue) -> None:
    async with _SUBSCRIBERS_LOCK:
        try:
            _SUBSCRIBERS.remove(q)
        except ValueError:
            pass


async def _broadcast(msg: str) -> None:
    """
    Fan-out message to all subscribers.
    If a queue is full/unresponsive, drop the message for that subscriber.
    """
    # Copy subscribers under lock, then fan-out without holding the lock.
    async with _SUBSCRIBERS_LOCK:
        subscribers = list(_SUBSCRIBERS)

    for q in subscribers:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            pass


def publish_event(event_type: str, data: Dict[str, Any]) -> None:
    """
    Publish an SSE event to all connected clients.

    Safe to call from:
    - async endpoints (has running loop)
    - sync endpoints (threadpool; no running loop)

    Best-effort: if no loop exists yet (no /events clients connected),
    it no-ops rather than crashing.
    """
    global _MAIN_LOOP

    msg = _sse_format(event_type, data)

    # Fast exit: nothing to do if nobody is listening.
    # (Also avoids doing loop scheduling work when unused.)
    try:
        # Avoid locking here; worst case we schedule broadcast and it finds nobody.
        if not _SUBSCRIBERS:
            return
    except Exception:
        # If something weird happens, keep best-effort behavior.
        return

    # If we're already on an event loop (async context), schedule directly.
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_broadcast(msg))
        return
    except RuntimeError:
        pass  # Not in an event loop (likely threadpool)

    # Threadpool / sync context path: schedule on the main loop if we have it.
    if _MAIN_LOOP is None:
        return

    try:
        _MAIN_LOOP.call_soon_threadsafe(asyncio.create_task, _broadcast(msg))
    except Exception:
        # Don't ever break API calls because SSE had a bad day.
        return


@router.get("/events")
async def sse_events(request: Request) -> StreamingResponse:
    """
    SSE stream emitting events (e.g., job.created, job.leased, job.completed)
    plus keepalive pings.
    """
    global _MAIN_LOOP
    try:
        _MAIN_LOOP = asyncio.get_running_loop()
    except Exception:
        # In practice, this should always succeed in ASGI.
        pass

    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    await _add_subscriber(q)

    async def event_generator():
        # Initial hello so the client knows the stream is alive
        yield _sse_format("hello", {"ts": time.time()})

        last_keepalive = time.time()

        try:
            while True:
                if await request.is_disconnected():
                    break

                timeout = max(0.1, _KEEPALIVE_INTERVAL_S - (time.time() - last_keepalive))
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=timeout)
                    yield msg
                except asyncio.TimeoutError:
                    # Keepalive comment (SSE ignores lines starting with :)
                    yield f": keepalive {int(time.time())}\n\n"
                    last_keepalive = time.time()
        finally:
            await _remove_subscriber(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # helps reverse proxies not buffer SSE
        },
    )
