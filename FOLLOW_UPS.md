# Follow-ups

Residual hardening items deferred from the 2026-05-25 session. None are blockers
for current operation — the plant collects, the scheduled task survives reboot,
curated depth data is replay-validated. These are the things to address before
fully trusting this for unsupervised long-running research.

Ordered roughly by risk × ease.

## 1. Reconnect depth-worker in place instead of ending the segment

**Where:** `src/crypto_collector/cli.py` — `_open_binance_depth_connection` /
`collect_binance_depth_segment` (around lines 173-280, 740-772).

**What:** Today the depth path only retries the *opening* handshake. Any
mid-stream WebSocket disconnect (Binance keepalive ping timeout, NAT drop,
etc.) ends the segment, fetches a fresh REST snapshot, and starts a new run
directory. Wasteful, fragments the archive, and creates extra "anchor gap"
risk at every blip.

**Fix:** Wrap the depth read loop with the same reconnect-in-place logic as
the generic collector (commit `6754887`). Re-use `_align_binance_buffered_events`
to detect if the post-disconnect window still aligns with the prior snapshot —
only fetch a new snapshot if it doesn't.

**Risk:** Medium. Real outages happen, and right now they cost extra REST
calls plus messier replay output.

---

## 2. Add a quality-gated curation chain for trades

**Where:** `src/crypto_collector/promotion.py`, `replay.py`, plus a new
trades-side replay module.

**What:** Depth has `replay_depth_run` → `quarantine_bad_runs` →
`promote_replayable_runs` → curated Parquet. Trades have nothing — they
land in `normalized/trades/` Parquet directly. "Curated = clean" only
applies to depth today.

**Fix:** Trades don't have first/final update IDs, so the gap-check has to
be different. A reasonable trades quality bar: trade_id monotonicity,
positive price/size, exchange_time within clock-skew tolerance,
trade_id density vs. wall-clock (catastrophic drop = dropped stream).

**Risk:** Low for today, important if you start running quant research on
the trades data and need the same "I trust this" guarantee.

---

## 3. Real durability test under SIGKILL / power loss

**Where:** New `tests/test_durability.py` or a manual runbook.

**What:** Commit #3 added fsync to JSONL writes, on paper protecting against
torn-tail lines on crash. There is no test that actually kills the process
mid-write and verifies the file parses cleanly. A future refactor could
remove the flush+fsync without anyone noticing.

**Fix:** Spawn the mock pipeline in a subprocess, kill -9 it during writes,
read the output and assert every line is valid JSON. Same for the per-run
Parquet flush in promotion (commit #11).

**Risk:** Low today, regression-prevention against future edits.

---

## 4. Make `health` consume partial metrics

**Where:** `src/crypto_collector/ops.py` — `build_health_report` and the
`metrics` directory of each run.

**What:** Commit #14 emits `partial: true` summary rows during a run. Nothing
reads them. The `health` command only sees heartbeat + job_runs, not the
in-flight reject-rate. An operator can't tell mid-run that the gate is
quarantining 30% of events until the run ends.

**Fix:** In `build_health_report`, find the latest `summary.jsonl` per
active run, take the last line, expose `reject_counts` and the
clean/quarantined ratio. Add a finding when the ratio exceeds a threshold.

**Risk:** Low. Observability gap.

---

## 5. Wake-from-sleep for the scheduled task

**Where:** `scripts/install_startup_task.ps1` or a one-off task edit.

**What:** `CryptoMarketDataPlant` doesn't have `-WakeToRun`. If the PC
sleeps, collection pauses until manual wake. For "I'm not at home for a
week" use, you either set power options to never sleep or add this flag.

**Fix:** Add `-WakeToRun` to `New-ScheduledTaskSettingsSet` in the install
script and re-register the task. Note: `-WakeToRun` requires the time
trigger has a `StartBoundary`; for the Startup trigger this means the
task wakes only when its repetition fires, not the boot itself.

**Risk:** Depends on usage pattern. None if PC never sleeps.

---

## 6. Tighten ParquetDatasetSink batch_size from 1000 → 100

**Where:** `src/crypto_collector/storage.py:129`.

**What:** On a hard kill / power cut, up to 1000 buffered normalized rows
are lost for the in-flight run. Raw JSONL is still durable — you can
rebuild from there — but the normalized layer briefly disagrees with raw.

**Fix:** Drop the default to 100. Cost: more, smaller part-files in the
Parquet dataset (slightly slower scans, more inode usage).

**Risk:** Low. Only matters if you care about the normalized layer's
consistency with raw at second-level granularity.

---

## L3 — explicitly deferred

The `Crypto_L3 collection` project at `G:\01-active\trading\Crypto_L3 collection\`
has `CryptoL3Collector` + `CryptoL3MarketSupervisor` scheduled tasks, both
**disabled**. Its unique jobs (Deribit perps via `book_summary`, CoinDesk RSS,
maker-queue-measure, promote-deribit-options/perps) are not running.

Re-enabling needs:

- A separate `--ops-root` so it doesn't fight `CryptoMarketDataPlant` for the
  `OpsRunnerLock` on `D:\market_archive\ops`.
- Disable the three duplicate jobs in L3's config: `quarantine-market`,
  `promote-market`, `research-manifest` (the plant already runs them).
- Verify L3's deribit jobs don't conflict with `BinanceIV Collect Deribit`
  (different output paths today, so likely fine).

Out of scope for current session.
