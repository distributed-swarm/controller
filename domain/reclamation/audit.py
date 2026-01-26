from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict

@dataclass(frozen=True)
class AuditEvent:
    type: str
    payload: Dict[str, Any]

