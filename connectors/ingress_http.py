from __future__ import annotations

from typing import Any, Dict

from pipelines.spec import IntakeRequest


def parse_intake_body(body: Dict[str, Any]) -> IntakeRequest:
    """
    Parse and sanitize the external intake request.

    Keep it strict + simple for v1:
      - only text/plain supported
      - basic bounds on chunk/overlap to avoid abuse
    """
    content_type = (body.get("content_type") or "text/plain").strip()
    text = body.get("text") or ""
    if not isinstance(text, str):
        text = str(text)

    chunk_chars = int(body.get("chunk_chars", 4000) or 4000)
    overlap_chars = int(body.get("overlap_chars", 200) or 200)

    # Bounds (anti-footgun + anti-abuse)
    chunk_chars = max(500, min(chunk_chars, 20000))
    overlap_chars = max(0, min(overlap_chars, int(chunk_chars * 0.5)))

    do_summarize = bool(body.get("do_summarize", True))
    do_classify = bool(body.get("do_classify", False))
    labels = list(body.get("labels") or [])

    return IntakeRequest(
        content_type=content_type,
        text=text,
        chunk_chars=chunk_chars,
        overlap_chars=overlap_chars,
        do_summarize=do_summarize,
        do_classify=do_classify,
        labels=labels,
    )
