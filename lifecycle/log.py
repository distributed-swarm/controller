# controller/lifecycle/log.py
from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, Optional

def _json_default(o: Any):
    # Never let logging crash your controller.
    try:
        return str(o)
    except Exception:
        return "<unserializable>"

def emit(event: str, *, component: str = "controller", **fields: Any) -> None:
    """
    Structured lifecycle event logger.
    One line of JSON per event. Never throws.
    """
    payload: Dict[str, Any] = {
        "type": event,
        "ts": time.time(),                 # epoch seconds (easy for machines)
        "ts_ms": int(time.time() * 1000),  # convenient for humans/graphs
        "component": component,
        "event_id": uuid.uuid4().hex,      # correlation-friendly
        **fields,
    }

    try:
        print(json.dumps(payload, default=_json_default, separators=(",", ":")), flush=True)
    except Exception:
        # Logging must never take the system down.
        try:
            print(
                json.dumps(
                    {
                        "type": "LOG_EMIT_FAILED",
                        "ts": time.time(),
                        "component": component,
                        "failed_event": event,
                    },
                    separators=(",", ":"),
                ),
                flush=True,
            )
        except Exception:
            pass
