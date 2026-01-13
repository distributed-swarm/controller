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

# How often to send keepalive comments (seconds)
_KEEPALIVE_INTERVAL_S = 15.0


def _sse_format(event_type: str, data: Dict[str, Any]) -> str:
    # SSE format: event + data + blank line
    # data must be a single line; JSON dumps is fine
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
    If a queue is full/unresponsive, we drop the message for that subscriber.
    """
    async with _SUBSCRIBERS_LOCK:
        for q in list(_SUBSCRIBERS):
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                # Drop rather than blocking the controller
                pass


def publish_event(event_type: str, data: Dict[str, Any]) -> None:
    """
    Publish an SSE event to all connected clients.

    Safe to call from sync FastAPI endpoints:
    - If we're not in an event loop, it schedules on the current loop when available.
    - If no loop is running (rare in ASGI), we simply no-op rather than crashing.
    """
    msg = _sse_format(event_type, data)

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_broadcast(msg))
    except RuntimeError:
        # No running loop (e.g., called from a sync context without loop)
        # We avoid crashing; the event just won't be broadcast.
        # When we wire the app with UvicornWorker, the loop will exist.
        return


@router.get("/events")
async def sse_events(request: Request) -> StreamingResponse:
    """
    SSE stream emitting:
      - job.updated
      - agent.updated
      - queue.updated
    plus keepalive pings.
    """
    q: asyncio.Queue = asyncio.Queue(maxsize=100)

    await _add_subscriber(q)

    async def event_generator():
        # Initial hello so the client knows the stream is alive
        yield _sse_format("hello", {"ts": time.time()})

        last_keepalive = time.time()

        try:
            while True:
                # Client disconnected?
                if await request.is_disconnected():
                    break

                # Wait for next message, but wake up periodically for keepalive
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
            # Helps with reverse proxies buffering (nginx)
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
