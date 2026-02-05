from dataclasses import dataclass

class StaleEpochError(Exception):
    pass

@dataclass(frozen=True)
class EpochCheck:
    ok: bool
    reason: str | None = None

def require_fresh_epoch(expected: int, got: int | None) -> None:
    # got may be None for older callers; decide policy:
    # ruthlessness: None is NOT allowed on v1 results.
    if got is None:
        raise StaleEpochError("MISSING_EPOCH")
    if got != expected:
        raise StaleEpochError("STALE_EPOCH")
