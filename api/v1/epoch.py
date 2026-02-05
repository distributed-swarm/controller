from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


class StaleEpochError(Exception):
    pass


class StaleLeaseError(Exception):
    pass


@dataclass(frozen=True)
class EpochCheck:
    ok: bool
    reason: str | None = None


def require_fresh_epoch(expected: int, got: int | None) -> None:
    # ruthlessness: None is NOT allowed on v1 results.
    if got is None:
        raise StaleEpochError("MISSING_EPOCH")
    if got != expected:
        raise StaleEpochError("STALE_EPOCH")


def require_fresh_lease(expected_lease_id: str | None, got_lease_id: str | None) -> None:
    """
    Optional helper: lease-id fencing. Use this in results processing if you want
    to reject mismatched leases explicitly (STALE_LEASE).
    """
    if expected_lease_id is None:
        # If controller thinks there is no current lease, any got_lease_id is stale.
        if got_lease_id is not None:
            raise StaleLeaseError("STALE_LEASE")
        return

    if got_lease_id is None or str(got_lease_id) != str(expected_lease_id):
        raise StaleLeaseError("STALE_LEASE")


def coerce_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default
