from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional

from pipelines.reducers import aggregate_report


def split_text(text: str, chunk_chars: int, overlap_chars: int) -> List[str]:
    text = text or ""
    if chunk_chars <= 0:
        return [text]

    overlap_chars = max(0, overlap_chars)
    chunks: List[str] = []

    i = 0
    n = len(text)
    while i < n:
        j = min(n, i + chunk_chars)
        chunk = text[i:j]
        if chunk.strip():
            chunks.append(chunk)
        if j >= n:
            break
        i = max(0, j - overlap_chars)

    return chunks


def wait_jobs_complete(
    job_ids: List[str],
    get_job_fn: Callable[[str], Optional[Dict[str, Any]]],
    *,
    timeout_s: float = 900.0,
    poll_s: float = 0.1,
) -> List[Dict[str, Any]]:
    """
    Block until all job_ids are completed/failed, then return results in the same order.
    Uses controller JOBS dict via get_job_fn.
    """
    deadline = time.time() + timeout_s
    remaining = set(job_ids)

    while remaining:
        if time.time() > deadline:
            raise TimeoutError(f"Timeout waiting on {len(remaining)} jobs")

        done_now: List[str] = []
        for jid in list(remaining):
            job = get_job_fn(jid)
            if not job:
                done_now.append(jid)
                continue
            st = job.get("status")
            if st in ("completed", "failed"):
                done_now.append(jid)

        for jid in done_now:
            remaining.discard(jid)

        if remaining:
            time.sleep(poll_s)

    outputs: List[Dict[str, Any]] = []
    for jid in job_ids:
        job = get_job_fn(jid) or {}
        if job.get("status") == "failed":
            outputs.append({"error": job.get("error"), "job_id": jid})
        else:
            res = job.get("result")
            outputs.append(res if isinstance(res, dict) else {"result": res, "job_id": jid})

    return outputs


def run_text_pipeline(
    *,
    text: str,
    chunk_chars: int,
    overlap_chars: int,
    do_summarize: bool,
    do_classify: bool,
    labels: List[str],
    enqueue_job_fn: Callable[[str, Dict[str, Any]], str],
    get_job_fn: Callable[[str], Optional[Dict[str, Any]]],
    timeout_s: float = 900.0,
) -> Dict[str, Any]:
    """
    Minimal digestion slice:
      split -> map summarize -> (optional) map classify -> reduce aggregate_report
    """
    chunks = split_text(text, chunk_chars, overlap_chars)

    # Stage 1: summarize each chunk (or pass-through)
    if do_summarize:
        sum_job_ids: List[str] = []
        for idx, chunk in enumerate(chunks):
            payload = {"text": chunk, "chunk_index": idx, "chunks_total": len(chunks)}
            sum_job_ids.append(enqueue_job_fn("summarize", payload))
        chunk_outputs = wait_jobs_complete(sum_job_ids, get_job_fn, timeout_s=timeout_s)
    else:
        chunk_outputs = [{"text": c, "chunk_index": i} for i, c in enumerate(chunks)]

    # Stage 2: classify (optional)
    if do_classify:
        cls_job_ids: List[str] = []
        for idx, out in enumerate(chunk_outputs):
            base_txt = ""
            if isinstance(out, dict):
                if isinstance(out.get("summary"), str):
                    base_txt = out["summary"]
                elif isinstance(out.get("text"), str):
                    base_txt = out["text"]
            payload = {"text": base_txt, "labels": labels, "chunk_index": idx}
            cls_job_ids.append(enqueue_job_fn("map_classify", payload))

        cls_outputs = wait_jobs_complete(cls_job_ids, get_job_fn, timeout_s=timeout_s)

        merged: List[Dict[str, Any]] = []
        for a, b in zip(chunk_outputs, cls_outputs):
            m: Dict[str, Any] = {}
            if isinstance(a, dict):
                m.update(a)
            if isinstance(b, dict):
                m.update(b)
            merged.append(m)
        chunk_outputs = merged

    final = aggregate_report(chunk_outputs)
    final["chunks_preview"] = chunks[:3]  # small debug aid
    return final
