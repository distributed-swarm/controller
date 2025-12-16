from __future__ import annotations

import json
import urllib.request
from typing import Any, Dict


def post_callback(url: str, payload: Dict[str, Any], timeout_s: float = 10.0) -> None:
    """
    Minimal HTTP egress connector.

    v1 behavior:
      - fire-and-forget POST
      - JSON payload
      - no retries (yet)
    """
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        # Intentionally ignore body; errors will raise
        resp.read()
