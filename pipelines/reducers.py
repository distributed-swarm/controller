from __future__ import annotations

from typing import Any, Dict, List


def aggregate_report(chunk_outputs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Minimal reducer for the first digestion slice.

    Input: list of per-chunk outputs (dicts). Output: one final report dict.

    We keep this flexible because op output shapes can vary:
      - summarize may return {"summary": "..."} or {"text": "..."}
      - classify may return {"label": "..."} or {"class": "..."} etc.
    """
    summaries: List[str] = []
    label_counts: Dict[str, int] = {}
    errors: List[Dict[str, Any]] = []

    for out in chunk_outputs:
        if not isinstance(out, dict):
            continue

        # summary text (handle common shapes)
        if isinstance(out.get("summary"), str) and out["summary"].strip():
            summaries.append(out["summary"].strip())
        elif isinstance(out.get("text"), str) and out["text"].strip():
            summaries.append(out["text"].strip())

        # classification label (flexible)
        label = out.get("label") or out.get("class") or out.get("category")
        if isinstance(label, str) and label.strip():
            label_counts[label] = label_counts.get(label, 0) + 1

        # errors
        if out.get("error"):
            errors.append({"error": out.get("error"), "job_id": out.get("job_id")})

    return {
        "summary": "\n\n".join(summaries).strip(),
        "class_counts": label_counts,
        "chunks": len(chunk_outputs),
        "errors": errors,
    }
