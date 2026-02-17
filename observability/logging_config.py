import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

SERVICE = os.getenv("SERVICE_NAME", "controller")
ENV = os.getenv("ENV", "dev")

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base: Dict[str, Any] = {
            "ts": _utc_iso(),
            "level": record.levelname,
            "service": SERVICE,
            "env": ENV,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # Merge "extra" dict fields (we store them on the record via `extra=...`)
        for k, v in record.__dict__.items():
            if k.startswith("_"):
                continue
            if k in (
                "name","msg","args","levelname","levelno","pathname","filename","module",
                "exc_info","exc_text","stack_info","lineno","funcName","created","msecs",
                "relativeCreated","thread","threadName","processName","process"
            ):
                continue
            # Avoid overriding core keys unless you really mean to
            if k not in base:
                base[k] = v

        if record.exc_info:
            base["exception"] = self.formatException(record.exc_info)

        return json.dumps(base, ensure_ascii=False)

def configure_logging(level: Optional[str] = None) -> None:
    lvl = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    root = logging.getLogger()
    root.setLevel(lvl)

    # Clear default handlers (uvicorn can double-log otherwise)
    root.handlers.clear()

    h = logging.StreamHandler(sys.stdout)
    h.setLevel(lvl)
    h.setFormatter(JsonFormatter())
    root.addHandler(h)

    # Quiet noisy libs unless you want them
    logging.getLogger("uvicorn.access").setLevel(os.getenv("UVICORN_ACCESS_LEVEL", "WARNING"))
    logging.getLogger("uvicorn.error").setLevel(lvl)
