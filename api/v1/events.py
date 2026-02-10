# controller/api/v1/events.py
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

router = APIRouter()


@dataclass
class _Subscriber:
    q: asyncio.Queue
    namespace: str
    include_global: bool = False


# --- In-process pub/sub for SSE subscribers (namespace-filtered) ---
_SUBSCRIBERS: List[_Subscriber] = []
_SUBSCRIBERS_LOCK = asyncio.Lock()

# Main event loop reference (used to publish from sync/threadpool contexts)
_MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

# How often to send keepalive comments (seconds)
_KEEPALIVE_INTERVAL_S = 15.0


def _sse_format(event_type: str, data: Dict[str, Any]) -> str:
    payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    return f"event: {event_type}\ndata: {payload}\n\n"


async def _add_subscriber(sub: _Subscriber) -> None:
    async with _SUBSCRIBERS_LOCK:
        _SUBSCRIBERS.append(sub)


async def _remove_subscriber(sub: _Subscriber) -> None:
    async with _SUBSCRIBERS_LOCK:
        try:
            _SUBSCRIBERS.remove(sub)
        except ValueError:
            pass


async def _broadcast(event_type: str, data: Dict[str, Any]) -> None:
    """
    Namespace-filtered fan-out.

    Rule:
      - If event has data["namespace"] == "<ns>", deliver only to subscribers of that ns.
      - If event has no namespace or namespace is None => "global event"
        deliver only to subscribers with include_global=True.
    """
    ev_ns = data.get("namespace")
    msg = _sse_format(event_type, data)

    async with _SUBSCRIBERS_LOCK:
        subs = list(_SUBSCRIBERS)

    for sub in subs:
        if ev_ns is None:
            if not sub.include_global:
                continue
        else:
            if str(ev_ns) != sub.namespace:
                continue
        try:
            sub.q.put_nowait(msg)
        except asyncio.QueueFull:
            pass


def publish_event(event_type: str, data: Dict[str, Any]) -> None:
    """
    Best-effort publish; safe from async or sync contexts.
    """
    global _MAIN_LOOP

    # Fast exit if nobody is listening.
    try:
        if not _SUBSCRIBERS:
            return
    except Exception:
        return

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_broadcast(event_type, data))
        return
    except RuntimeError:
        pass

    if _MAIN_LOOP is None:
        return

    try:
        _MAIN_LOOP.call_soon_threadsafe(asyncio.create_task, _broadcast(event_type, data))
    except Exception:
        return


@router.get("/events")
async def sse_events(request: Request, namespace: str, include_global: bool = False) -> StreamingResponse:
    """
    SSE stream (namespace-filtered).
    Requires namespace to prevent cross-tenant leakage.
    """
    ns = (namespace or "").strip()
    if not ns:
        raise HTTPException(status_code=400, detail="namespace is required")

    global _MAIN_LOOP
    try:
        _MAIN_LOOP = asyncio.get_running_loop()
    except Exception:
        pass

    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    sub = _Subscriber(q=q, namespace=ns, include_global=bool(include_global))
    await _add_subscriber(sub)

    async def event_generator():
        # Initial hello so the client knows the stream is alive (namespaced)
        yield _sse_format("hello", {"ts": time.time(), "namespace": ns})

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
                    yield f": keepalive {int(time.time())}\n\n"
                    last_keepalive = time.time()
        finally:
            await _remove_subscriber(sub)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
