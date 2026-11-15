# OI receipt-order correction: validation and deployment boundary

Prepared November 10; packaged November 12; forward-verified November 15, 2026.
Approved STANDARDS v20.

Deployment addendum, November 12: owner approved the contract and forward
deployment; PR110 merged as `0885199`, live checkout aligned and fixed SYSTEM
restart verified at 15:47:12 UTC (runner 103660, 166 jobs). The November 12 PC
crash interrupted the original forward verifier; no final receipt was produced.
The SYSTEM task recovered after reboot. A new bounded check on November 15 at
21:12 UTC passed: one latest promoted closed run for each of BTC/ETH/SOL,
1,394/1,389/1,400 rows respectively, matches replay counts, promotion counts and
exact venue/receipt clock multisets in curated Parquet. Clean source bytes were
unchanged. This verifies those runs, not continuous coverage. Both previously
blocked non-OI workers have fresh running heartbeats. Private receipt:
`04 - Logs/oi-v20-deployment-20261112/forward_verification_20261115T211236Z.json`.
Existing historical quarantines remain excluded; no historical repair occurred.

## Problem and behavior

Three fixed BTC/ETH/SOL samples contain 3,469 observations in total. Their raw
venue timestamps regress 32/19/26 times between adjacent observations, by up to
1.989/2.121/2.223 seconds. Receipt clocks remain ordered and both clocks match the
normalized records. Thus whole-run rejection is caused by the scorer's exchange
ordering rule, not by normalization reordering those samples. This does not
establish why the venue returns regressing timestamps.

The approved OI mode validates per-product receipt ordering, valid aware clocks,
instrument identity, finite nonnegative quantity, absent price and a finite
nonnegative skew limit (default 60 seconds). Original venue regressions remain
counted diagnostics. Values, clocks, duplicates and source order are preserved.
Funding's exchange-order requirement remains; invalid skew configuration now
raises explicitly instead of weakening the bound. No polling or collector
dispatch setting changes.

## Verification

- Full local suite: **930 passed, 5 skipped**, with three expected configuration
  fallback warnings, in 84.24 seconds. `PYTHONPATH=src`, shared plant Python 3.12
  environment, isolated checkout and hermetic temporary archive roots.
- Full configured Ruff checks (`src tests scripts`) and `git diff --check` pass.
- Independent static review approved the diff with no actionable findings. It
  checked receipt/venue high-water handling, malformed clocks, funding separation,
  promotion compatibility and quarantine precedence; it did not access live data.
- Three fixed source runs replayed with `write_summary=False`: all 3,469 records
  pass the proposed structural rule, with venue high-water regression diagnostics
  retained (62/39/54, distinct from adjacent-regression counts). SHA256 of each
  clean file and existing summary remained unchanged. No promotion was invoked.
- Synthetic tests cover reversed receipt clocks; missing, malformed and naive
  clocks; stale/future skew; malformed values and identities; mixed metric types;
  duplicate timestamps; zero OI; empty runs; interleaved historical products;
  and preservation of both clocks through actual Parquet promotion. An existing
  quarantine index still prevents promotion even after a passing re-score.

Private source receipts live in the Second Brain's
`04 - Logs/plant-audit-20261110/oi-source-diagnosis.json` and
`oi-proposed-dry-run.json`. No raw market payload is included in this PR.

## Decision and rollout

The repository reserves STANDARDS/contract changes to the owner. The owner
approved receipt order for OI replay admission while recording original
venue-clock regressions as diagnostics, under all other stated checks, on
November 12. No permission for historical data repair is implied.

PR110 was merged after contract approval, green CI and independent review. Live
code was aligned and restarted through the fixed SYSTEM-task process; the
forward check above separately verifies new curated rows. A merged PR alone is
not deployment or verification. This documentation update needs no restart.

Existing replay summaries are skipped unless overwrite is explicitly requested,
and indexed quarantines remain excluded. Missing-summary catch-up may still score
eligible runs through the existing job path; that is not a bulk archive repair.
Prepare any historical recovery as a separate bounded inventory and reviewed
repair action. Keep original timestamps and gaps. No protected research outcomes,
trading, archive deletion, source alteration or new acquisition is authorized.
