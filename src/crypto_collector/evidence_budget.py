"""Resource limits for optional journals, not admission of market data."""
from __future__ import annotations

MIB = 1024**2
GIB = 1024**3
DEFAULT_MAX_BYTES = 64 * MIB
MAX_BYTES = 512 * MIB
DEFAULT_MIN_FREE_BYTES = 100 * GIB
DISK_CHECK_BYTES = MIB


def configured_budget(max_mib=64, min_free_gib=100) -> tuple[int, int]:
    # Reject bool, fractional values and numeric strings in ops JSON. Argparse
    # converts CLI strings explicitly; no silent truncation of config mistakes.
    if type(max_mib) is not int or not 1 <= max_mib <= 512:
        raise ValueError("session_evidence_max_mib must be an integer in 1..512")
    if type(min_free_gib) is not int or not 100 <= min_free_gib <= 4096:
        raise ValueError("session_evidence_min_free_gib must be an integer in 100..4096")
    return max_mib * MIB, min_free_gib * GIB


def required_free_bytes(max_bytes: int, written_bytes: int, min_free_bytes: int) -> int:
    return min_free_bytes + max(0, max_bytes - written_bytes)
