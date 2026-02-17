import logging
from typing import Any, Dict

log = logging.getLogger("lifecycle")

def lifecycle(event: str, **fields: Any) -> None:
    payload: Dict[str, Any] = {"event": event}
    payload.update(fields)
    log.info(event, extra=payload)
