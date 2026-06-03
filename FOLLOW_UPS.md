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

### Gap from today (updated 2026-05-30, after Phase 2)

Most of the north-star scaffolding now exists. Snapshot of what was left as of
2026-05-30, with status after the 2026-06-01 "Next steps" work below:

- ~~New venue lanes never run against a real socket / ship disabled~~ — **DONE**:
  all 6 validated live and enabled in `ops.live.local.json` (#1 below).
- `--rotate-at-midnight` exists but is **opt-in**; the live model is still
  count-bounded segments, so an analyst still globs runs to assemble a day. (Still
  open — see the roadmap note on day-bounded rotation as the default.)
- ~~Parquet partitioned by `source=<venue>` only, no `instrument=` column~~ —
  **DONE**: v1→v2 cutover added the `instrument=` partition (#2 below).

### Rough order if/when this becomes the focus

1. **Per-instrument lanes** — DONE. Added `--source-suffix` flag (depth +
   trades) so additional symbols land in their own
   `binance_depth_<suffix>/` / `binance_trades_<suffix>/` directory tree
   without touching the legacy single-symbol BTC layout. `ops.live.example.json`
   shows an ETH lane (`enabled: false`) as the recipe.
2. **Day-bounded run rotation** — DONE. Added `--rotate-at-midnight` flag
   (depth + trades). When set, `_run_segmented_worker` computes a UTC-midnight
   deadline for each segment and threads it through `args.deadline_utc`. The
   depth segment checks `_deadline_crossed()` after each processed event;
   `CollectorPipeline.run` does the same for trades. Segments stop cleanly
   on the deadline (the existing parquet flush + replay summary + metrics
   write all run), exposed via `deadline_reached` in the segment summary.
   Default off preserves the count-based behavior the live BTC collector uses.
3. **Add Coinbase + Bybit + Kraken adapters** — DONE. All three venues now have
   trades + depth normalizers and verticals (Coinbase `matches`/`level2_batch`,
   Kraken v2 `trade`/`book`, Bybit v5 `publicTrade`/`orderbook`). Trades:
   Coinbase + Kraken are `sequence` (gap-proof), Bybit is `none_native`. Depth:
   Coinbase + Kraken + Bybit are all `none_native` (in-stream snapshot, no dense
   sequence we trust). All ship `enabled: false` in `ops.live.example.json`.
   **Caveat:** validated only against scripted WebSockets — never a real socket
   (see "Next steps after Phase 2" #1 below).
4. **Curated layout by event_date** — DONE for the manifest contract. The
   `research-manifest` job now emits per-`(venue, instrument, dataset,
   event_date)` `lanes` with `gap_detection` + `readiness`, tagged with
   `standards_version`. **Not** done: an `instrument=` Parquet partition column
   (see "Next steps after Phase 2" #2).
5. **`STANDARDS.md`** at repo root — DONE. `STANDARDS_VERSION = 1`; covers
   schema, gap policy, replayable definition per feed class, retention, consumer
   API. Manifest output carries `standards_version`.

### Risk

This is the actual product. Until it exists, "research-ready data" means
"depth from Binance BTCUSDT, hour by hour, manually assembled." Worth
investing in once the immediate hardening list above is closed out.

---

## Next steps after Phase 2 (captured 2026-05-30)

Candidate next focuses, in rough value order. Each is **paused pending a
decision** — every high-value item touches the live deployment, the live data
layout, or an external venue, so none should be started silently.

1. **Validate the new venue adapters against real exchanges, then enable them
   live.** — DONE (2026-05-31). All 6 lanes ran bounded real-socket segments to a
   throwaway temp archive (`MARKET_DATA_ARCHIVE_ROOT`) and produced
   `replayable: true` with the expected contract: coinbase/kraken trades =
   `sequence`; bybit trades + all three depth lanes = `none_native` /
   `stream_snapshot`. One real bug found + fixed: **Coinbase depth `level2_batch`
   is dead** (public `level2`/`level2_batch` now require auth) — switched to
   `level2_50` (same frame shape) and raised the WS `max_size` for its ~1.4 MiB
   full-book snapshot (commit `d616e52`). All 6 lanes + full curation chains
   (quarantine+promote, depth→`market_replayable` / trades→`trades_replayable`) +
   cleanup retention are now enabled in `ops.live.local.json` (backed up to
   `ops.live.local.json.bak`; validated through `load_ops_config`). **Activation:**
   the ops-runner reads its config once at startup, so the new lanes go live on the
   next runner restart (reboot or manual restart of the `CryptoMarketDataPlant`
   task) — the currently-running Binance collector is untouched until then.

2. **`instrument=` partition column** — DONE (2026-06-01). Implemented as a Parquet
   `schema_version` `v1→v2` cutover: `ParquetDatasetSink` now partitions on
   `["schema_version", "source", "instrument", "event_date"]` for v2 (the default),
   deriving `instrument` from the row's canonical symbol (→ venue product → `unknown`),
   sanitized for the path (`BTC/USDT`→`BTC-USDT`); the resolved `InstrumentRef` moves
   to an `instrument_ref` column. Old `v1` data is untouched (a v1-tagged sink keeps
   the 3-level layout). Promotion re-writes rows through the same sink (auto-gets the
   partition); the manifest reads by key name (depth-agnostic) so it's unaffected.
   `STANDARDS_VERSION` 3→4. Verified live: a real run writes
   `schema_version=v2/source=coinbase/instrument=BTC-USD/event_date=…`. Storage v2
   tests added.

3. **App-level keepalive ping for Bybit** — DONE (2026-05-31). Added opt-in
   `CollectorConfig.ping_message` + `ping_interval_seconds`; `GenericWebsocketCollector`
   runs a per-connection keepalive task (spawned after the subscription handshake,
   torn down in `finally` on reconnect/limit/error). Both Bybit lanes opt in at
   `{"op":"ping"}` / 20 s; every other venue (incl. live Binance) leaves it off and
   relies on protocol-level ping/pong. Deterministic tests cover ping-sent (Bybit)
   and no-ping (default). STANDARDS §4.3 + §8 updated.

4. **Stronger gap-proofing for the `none_native` depth lanes.**
   - **Bybit `data.u` — DONE (2026-06-01, commit `b5ca110`).** Real-socket
     validation showed `data.u` increments by exactly 1 per message (60/60 frames),
     so Bybit depth was upgraded from `none_native` to a provable `sequence`
     guarantee via `replay_depth_stream_run(sequence_metadata_key="bybit_update_id")`
     — a `data.u` gap now blocks promotion. `STANDARDS_VERSION` bumped 1→2.
   - **Kraken CRC32 `checksum` — DONE (2026-06-01).** The exact CRC32 spec was
     solved empirically against a real captured snapshot (asks top-10 asc then bids
     top-10 desc, each `price`@price-prec + `qty`@qty-prec, decimal removed + leading
     zeros stripped) and verified to reproduce the snapshot **and** update checksums.
     It turned out the precision-from-float reconstruction works (values have ≤8
     decimals), so **no schema change was needed**: `replay_depth_stream_run` gained
     `checksum_metadata_key` + precisions, rebuilds the top-10 book from stored floats,
     and recomputes the CRC after every event. BTC/USD precision `(1, 8)` lives in
     `_KRAKEN_BOOK_PRECISION`; unknown pairs fall back to `none_native`. Kraken depth
     is now `gap_detection="checksum"` (provable integrity); `STANDARDS_VERSION` 2→3.
     A frozen golden-vector test guards the CRC algorithm against real venue data.
     Remaining: add other pairs' precision (or auto-fetch from REST `AssetPairs`).

5. **Data-arrival watchdog for the WS collector** — DONE (2026-06-01). Added opt-in
   `CollectorConfig.idle_timeout_seconds` (default `0.0` = OFF). When set, the
   `GenericWebsocketCollector.stream` receive loop bounds each wait for the next data
   frame (`asyncio.wait_for` around the iterator); if none arrives in time it ends the
   segment **cleanly** (the pipeline `finally` still writes `metrics/summary.jsonl` +
   the segment writes `replay_summary.json`, and the worker loop opens a fresh segment)
   instead of blocking forever — chosen over in-place reconnect because for the
   in-stream-snapshot depth lanes a fresh segment yields a clean single-snapshot run,
   not a quarantined multi-snapshot one. Each fire increments the collector's
   `idle_timeout_count`, threaded by the pipeline into `metrics/summary.jsonl` and
   surfaced by `build_health_report` as a non-blocking `idle_timeout:<worker>` finding
   for active workers (self-heals via the fresh segment). Wired through the ops config
   per-lane via `_job_args` → `_run_segmented_worker` (the Binance depth lane runs its
   own socket loop and ignores it; default-off leaves every live lane unchanged).
   Deterministic tests use a fake WS whose async iterator stalls: timeout fires + ends
   (no hang), frames-then-stall, disabled-blocks-forever contrast, clean-close still
   reconnects, and an end-to-end segment surfacing `idle_timeout_count` in
   `summary.jsonl`. STANDARDS §2.1 + §4 updated (no `STANDARDS_VERSION` bump — the data
   schema, partition layout, and "replayable" definition are unchanged; this adds an
   operational metric/finding only).

6. **Parallel collection runner** — DONE (2026-06-03). `OpsRunner` now dispatches the
   8 collector job types (`*-worker`) through a `ThreadPoolExecutor` sized by
   `--collector-concurrency` (CLI flag, default `1` = legacy serial; live scheduled task
   passes `4` via `run_ops_runner.ps1 -CollectorConcurrency 4`). Each collector reaps on
   completion → writes `job_runs.jsonl`, updates counters, increments `run_count`, and
   reschedules from `finished_at + interval_seconds`. A job is never launched twice while
   its previous run is still in flight (active-set guard). Maintenance jobs
   (quarantine/promote/manifest/cleanup/health) stay serialized: they run one at a time in
   the scheduler thread, may overlap active collectors, but never overlap each other.
   Heartbeat gained `current_jobs` (full active set: `name`/`job_type`/`started_at`) while
   `current_job` is preserved as the oldest active job (or `null`) for backward compat; a
   single background refresher thread keeps `last_seen`/`current_jobs` fresh.
   `build_health_report` now treats any job present in `current_jobs` as in progress, so
   long-running collectors are not flagged stale (legacy `current_job` still honored).
   Tests cover: concurrent collectors (barrier-proven), no double-launch of the same job,
   maintenance serialized when multiple due, maintenance running while a collector is
   active, heartbeat `current_jobs` + preserved `current_job`, health not flagging a
   running collector, and the `--collector-concurrency` default. No `STANDARDS_VERSION`
   bump — the on-disk data schema, partition layout, and "replayable" definition are
   unchanged; this is a scheduler/telemetry change only. The live Binance lanes are
   unaffected (default-1 behavior is identical; concurrency only changes dispatch order).

7. **Multi-anchor stream-depth replay + backfill** — DONE (2026-06-03). Root cause of
   Coinbase/Bybit/Kraken depth showing **0 curated rows** while raw collection was clean:
   `replay_depth_stream_run` required exactly one snapshot anchor at position 0 (the
   Binance REST model), but stream-snapshot venues re-snapshot mid-run by design (~4
   anchors per 5,000-event segment), so every run was rejected. Fixed by (a) re-anchoring
   at each in-stream `snapshot` and validating sequence/checksum **within each sub-book**
   (gate is now "run starts with a snapshot" + integrity-per-sub-book; retired
   `multiple_snapshot_anchors`/`snapshot_not_first_event`, added
   `run_does_not_start_with_snapshot`), and (b) a `book_depth` param that trims Kraken's
   book to the subscribed top-N after each mutation (Kraken silently evicts the worst
   level past depth without a delete, so an unbounded book diverges the CRC — empirically
   ~90% of frames mismatched). Verified against today's live captured runs: bybit/kraken/
   coinbase all `replayable=True`, 0 findings (kraken 5,000 frames, 0 CRC mismatch). Added
   the `backfill-stream-depth` CLI (dry-run default; `--apply` regenerates each backlog
   run's `replay_summary.json` and promotes the replayable ones) — dry-run over the live
   backlog reports 11 coinbase + 10 bybit + 10 kraken depth runs all now replayable.
   `STANDARDS_VERSION` 4→5; §4 updated. **Operational rollout still pending the maintainer:**
   restart the ops-runner (so the live collector loads this code — briefly interrupts the
   Binance lane) and run `backfill-stream-depth --apply` for the existing backlog.

Also still parked: the **L3 collection project** re-enable (see bottom of this
file) and making **day-bounded rotation the default** run model (currently
opt-in via `--rotate-at-midnight`).

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

## 4. Make `health` consume partial metrics — DONE

**Status:** Resolved. `build_health_report` now reads the last row of
`<current_run_path>/metrics/summary.jsonl` for each standalone worker and
surfaces it on the worker row as `partial_metrics` + `quarantine_ratio`.
When the ratio exceeds `--quarantine-ratio-threshold` (default 0.20) AND
the worker is still active, the report adds a
`high_quarantine_ratio:<worker>` finding so operators see in-flight gate
problems instead of having to wait for shutdown. Stopped workers don't
trigger the finding (historical high-reject runs aren't an in-flight issue).

---

## 5. Wake-from-sleep for the scheduled task — DONE

**Status:** Resolved. `-WakeToRun` added to `New-ScheduledTaskSettingsSet`
in `scripts/install_startup_task.ps1`. With only the current
`-AtStartup` / `-AtLogOn` trigger this flag is a no-op (those triggers
fire when the OS comes up, not on a sleeping system) — but it costs
nothing and is ready for the day a time-based or repetition trigger is
added via `Set-ScheduledTask`. Re-register the task by re-running the
installer as Administrator.

---

## 6. Tighten ParquetDatasetSink batch_size from 1000 → 100 — DONE

**Status:** Resolved. Default lowered to 100 in
`src/crypto_collector/storage.py`. Lost-on-kill window for the normalized
layer is now ~100 events of disagreement with raw JSONL (which is still
durable via per-write fsync) at the cost of more, smaller part-files.

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
