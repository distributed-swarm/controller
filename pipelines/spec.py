from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class IntakeRequest:
    """
    External request shape for 'intake' (aka ingest).
    Keep it simple at first: only text/plain.
    """
    content_type: str = "text/plain"
    text: str = ""

    # Digestion knobs (defaults are conservative and safe)
    chunk_chars: int = 4000
    overlap_chars: int = 200

    # Pipeline toggles (start with summarize only)
    do_summarize: bool = True
    do_classify: bool = False
    labels: List[str] = field(default_factory=list)


@dataclass
class PipelineStatus:
    """
    Controller-side tracking of a pipeline run.
    """
    run_id: str
    status: str  # queued | running | completed | failed
    created_ts: float
    updated_ts: float
    detail: Optional[str] = None
    final_result: Optional[Dict[str, Any]] = None
