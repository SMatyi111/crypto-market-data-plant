# Follow-ups

Residual hardening items deferred from the 2026-05-25 session. None are blockers
for current operation — the plant collects, the scheduled task survives reboot,
curated depth data is replay-validated. These are the things to address before
fully trusting this for unsupervised long-running research.

Ordered roughly by risk × ease.

---

## North-star goal — multi-pair, multi-venue, day-bounded, pull-ready

What "done" looks like, eventually:

- **Continuous collection**, not the current hour-bounded segment model. A run
  rotates at the day boundary, not at an arbitrary message count.
- **Daily curated files** that an analyst can pull by date alone (e.g.
  `curated/research/market_replayable/source=binance/instrument=BTC-USDT/event_date=2026-05-26/*.parquet`)
  with no per-run timestamp directories to think about.
- **Multiple instruments** per venue: BTCUSDT, ETHUSDT, SOLUSDT, … each in its
  own collection lane. Today the config has one symbol hard-coded per worker.
- **Multiple venues**: Binance + Coinbase + Bybit + Kraken (depth + trades each),
  plus the options stack already running (Binance options, Deribit). Each venue
  gets its own normalizer and replay validator.
- **Published "ready day" manifest** that downstream tools consume: for each
  (venue, instrument, event_date) tuple, is the day complete and replay-clean?
  The existing `research-manifest` job is the scaffolding; it needs to become
  the contract.
- **Standards documented** in the repo so future-me (and anyone else) knows
  what the guarantees are: schema per dataset, gap policy, what "replayable"
  actually means, retention SLA, how to consume.

### Gap from today

- One pair (BTCUSDT), one venue (Binance) for depth/trades.
- Hour-bounded run directories — analyst has to glob across them to assemble
  a day.
- Manifest exists at `D:\market_archive\curated\research\manifests\` but isn't
  the canonical "what's ready" artifact downstream tools key off of.
- No published standards doc.

### Rough order if/when this becomes the focus

1. **Per-instrument lanes** — turn `symbol` from a fixed CLI flag into a list
   of jobs in `ops.live.local.json`. One job per (venue, instrument). Each
   job gets its own worker, own ops-state, own quarantine. (Re-uses existing
   code; mostly a config exercise.)
2. **Day-bounded run rotation** — change `_run_segmented_worker` from
   "segment_count messages per segment" to "rotate at midnight UTC". Replay
   then runs over whole days, not arbitrary 5000-event slices.
3. **Add Coinbase + Bybit + Kraken adapters** — each needs a normalizer
   (like `BinanceDepthNormalizer`) and venue-specific subscription / snapshot
   handling. Generic collector already handles `subscription_style` so most
   of the framework is there.
4. **Curated layout by event_date** — already partitioned that way at the
   Parquet layer; just need the manifest to surface it as the contract.
5. **`STANDARDS.md`** at repo root: schema, gap policy, replayable definition,
   retention, consumer API. Tag manifest output with the standards version.

### Risk

This is the actual product. Until it exists, "research-ready data" means
"depth from Binance BTCUSDT, hour by hour, manually assembled." Worth
investing in once the immediate hardening list above is closed out.

---

## 1. Reconnect depth-worker in place instead of ending the segment — DONE

**Status:** Resolved. `collect_binance_depth_segment` now reconnects-in-place
on retryable WS errors / clean close, reusing the original snapshot anchor
and applying `_align_binance_buffered_events` against a rolling
`last_seen_final_update_id`. If the post-reconnect window has a gap, the
segment ends cleanly (preserving replay's single-snapshot-per-run invariant)
and the worker loop opens a fresh run+snapshot. Metrics now expose
`reconnect_count` and `alignment_break_count` per segment.

---

## 2. Add a quality-gated curation chain for trades — DONE

**Status:** Resolved. Added `replay_trades_run` to `replay.py` and wired it
into `collect_binance_trades_segment`, so every trades run now writes a
`metrics/replay_summary.json` with the same `{replayable, findings}` shape
the depth chain uses. `quarantine_bad_runs` and `promote_replayable_runs`
work unchanged — `ops.live.example.json` shows the matching trades-side
quarantine + promote jobs.

Quality bar: trade_id monotonicity (non-decreasing), no trade_id gaps
(Binance trade_id is a dense per-symbol counter), price and size finite
and positive, exchange_time within `--max-clock-skew-ms` (default 60s)
of received_at.

---

## 3. Real durability test under SIGKILL / power loss — DONE

**Status:** Resolved. `tests/test_durability.py` spawns the mock pipeline as
a subprocess, kills it with `Popen.kill()` (which is `TerminateProcess` on
Windows — equivalent to SIGKILL, no user-space cleanup runs), then asserts
that every line in the produced JSONL files parses cleanly and the file
ends with a newline. If a future refactor removes the per-write fsync, the
test fails — the killed subprocess would otherwise leave a partial last
line in messages.jsonl. Added `--delay-ms` to the mock CLI so the
subprocess actually has writes in flight when the kill lands.

(The per-run Parquet flush in promotion is also exercised by the existing
`test_promotion.py`, which validates the flush-before-index ordering. A
dedicated SIGKILL-mid-Parquet-flush test was deferred — the index is the
single durability gate there, and a kill before flush is equivalent to a
kill before index write, which the existing tests already cover.)

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
