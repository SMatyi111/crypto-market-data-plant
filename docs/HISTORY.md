# History — resolved work narrative

Root causes, design decisions, and verification notes for completed work, newest
first. Moved out of the former `FOLLOW_UPS.md` on 2026-06-11 — the forward-looking
plan now lives in [`../ROADMAP.md`](../ROADMAP.md). The terse per-change record is
git log + the merged PR descriptions; this file keeps the *why*.

---

## 2026-06-12 — baseline src/ audit completed (7 remaining subsystems; ~35 findings fixed)

Finished the one-time baseline audit PR #15 started: one reviewer agent per
subsystem (ws-core, cli-collection, cli-ops-wiring, normalize, ops-runner,
support, mexc-misc — the slim design, no verifier fleet), every finding verified
by reading the code in the main session, fixes + regression tests in one PR. The
deferred PR #17 review folded into the ops-runner pass; verdict: its four fixes
are sound, with three gaps closed here (garbage-lock self-heal, stall-finding
escalation, the surviving long-maintenance stall class). The five headline
findings:

1. **websockets ≥ 13 rotted the reconnect allowlist.** The installed library (16.0)
   raises `InvalidStatus` for a non-101 handshake, not the legacy
   `InvalidStatusCode` the allowlist knew — so a routine 429/503 during venue
   maintenance crashed the worker on the FIRST attempt instead of backing off.
   Any `InvalidHandshake` subclass is now retryable (MRO-name check).
2. **A torn/0-byte lock file crash-looped its lane forever.** `_read_json_file`
   raised `JSONDecodeError` straight out of `acquire()`, bypassing the stale-lock
   self-heal PR #17 built — and on `ops-runner.lock` it was a plant-wide boot
   failure. Lock/heartbeat JSON reads are now never-raise; stale locks are broken
   via atomic rename (closing an unlink/create TOCTOU that could double-acquire a
   lane = double promoter).
3. **The research manifest silently omitted every perp lane and the whole
   `funding` dataset** — `parse_lane` couldn't parse `<venue>_perp_<dataset>` and
   `DATASET_CONFIG` predated v6's funding dataset, so the contract's canonical
   readiness view covered 14 of 21 lanes. Also fixed there: non-atomic
   `research_manifest_latest.json` writes (torn consumer reads), the
   `gap_detection` latch (Kraken's checksum lane published as `sequence`;
   no-evidence lanes defaulted to provable — now worst-class-wins with an
   explicit `unknown`), and one torn index line crash-looping the manifest job.
4. **Kraken subscribe-time trade snapshots became duplicate prints in curated.**
   Kraken replays the last ~50 trades in a `type:"snapshot"` frame on EVERY
   subscribe; the normalizer ignored the frame type, the gate's per-run sequence
   cursor starts empty, and promotion dedups by run only — so every segment
   boundary curated up to ~50 duplicated prints into the provably-gapless trades
   dataset (Coinbase's `last_match` is the 1-per-subscribe variant). Normalizers
   now tag `subscribe_replay`; the gate quarantines it (raw keeps everything).
   The gate also mirrors the promotion bar for prints (missing/zero price or size
   quarantines the event, not the whole segment at scoring).
5. **Binance REST aggTrades duplicated a full segment into curated on every
   unclean worker death.** The resume cursor only advances on clean segment end;
   a killed segment left durable rows beyond the cursor, the hourly catch-up
   scorer certified the re-fetch, and run-keyed promotion curated both. The
   resume floor is now raised to the durable on-disk high-water (age-bounded by
   the same resume-gap rule).

Runner-architecture fix motivated by the audit: **maintenance jobs moved off the
scheduler thread onto a dedicated single-slot executor** (still strictly
serialized with itself, reaped by the scheduler loop). Inline maintenance was the
remaining scheduler-stall class PR #17's incident exposed — archive-offload's
first real pass (due ~2026-06-22, tens of GB) would have frozen all dispatch and
tripped a false `scheduler_stalled`. The stall finding itself now escalates
health to `error`, heartbeat/run-log write failures (AV contention, full disk)
no longer kill the runner, and health tail-reads `job_runs.jsonl`.

The enumeration/lambda-drop trap claimed four more victims, now all threaded
centrally and regression-tested: `normalized_parquet` (inert on every lane but
one), `snapshot_anchor_timeout_seconds` (binance-depth tuning was a no-op),
`jsonl_fsync` on the five depth job types, and the REST segment's fsync cadence
knobs. Other notable fixes: one undecodable WS frame no longer kills a lane
(skip + count + consecutive-failure cap); the binance-depth deadline rotation no
longer records a spurious reconnect + alignment break every 30-min segment
(constant 1-per-segment noise in book-sync-health since the lane went live);
offload's resume path now writes the index row it owed (and recovers
interrupted deletes instead of wedging in `cold_target_mismatch`); a configured
`MARKET_DATA_ARCHIVE_ROOT` on a not-yet-mounted drive no longer silently
re-routes writes to the default disk; `mock` no longer writes synthetic rows
into the live normalized dataset (the durability test had been doing exactly
that on this box); duplicate job names are refused at config load and in both
`.ps1` preflights; `redeploy_runner.ps1` kills root-first (no surviving
mid-kill workers), clears stale worker locks after its no-plant-python gate
(saves each lane the up-to-600s self-heal wait), and its post-relaunch check
now proves the NEW runner is alive instead of re-reading the dead runner's
heartbeat. STANDARDS §2.1 was corrected to describe the deployed batched-fsync
durability posture (process-kill-proof; power loss can cost ≤1 batch + a torn
tail), §5 documents the new gate reasons, §6 the perp/funding lanes and
`unknown`/worst-class `gap_detection` semantics — and because those widen
contract-visible surfaces (manifest vocabulary, lane venues, gate reasons),
**`STANDARDS_VERSION` bumped 6 → 7** in both files with a v7 changelog.

The pre-handover `/code-review` pass (4 finder angles, candidates verified by
reading the code in the main session per the fan-out budget) caught five real
misses in the audit fixes themselves, all fixed before the PR: (1) the blanket
subscribe-replay quarantine destroyed the mid-segment reconnect heal — within a
run, the old monotonic gate let the snapshot's NEW prints fill the reconnect
gap, and quarantining them punched a provable id gap that failed the whole run;
the gate now passes a tagged print only when the run's sequence cursor proves
it new. (2) `normalized_parquet` central threading delivered the flag to depth
segments that never read it — both depth segment bodies now honor it. (3) The
resume-floor scan wasn't symbol-scoped, could burn its window on the current
segment's own empty dir, and full-scanned a multi-MB file per rotation — now
symbol-filtered, exclusion-aware, tail-reading. (4) `OpsRunnerLock` lacked the
fresh-lock `created_at` grace its worker sibling has (boot task + manual
redeploy could double-acquire the runner), `_break_stale_lock`'s rename could
livelock on leftover `.stale-*` residue (now `replace`), unreadable-but-young
locks get a mid-write grace, and acquire() fails loudly after 30s instead of
spinning invisibly. (5) The offload pre-delete re-verify ran AFTER the index
write, leaving a lying index row on abort — reordered, with the orphaned cold
copy removed. Also from the pass: `health` survives the invalid-config error it
should be diagnosing; subscribe-replay rejects are excluded from the
high-quarantine-ratio alarm (Kraken's ~50-print snapshot tripped it on quiet
segments); `--stop-on-error` surfaces the error heartbeat before the drain; the
job-runs tail window sized against real growth (64 MiB); shared
`write_text_atomic` in storage.py (the snapshot anchor is now fsynced — a power
cut could previously promote a zero-length anchor); one tolerance policy for
both promotion-index readers. Queued for live verification (ROADMAP item 11):
whether OKX/Bybit also replay prints at subscribe.

Known data-quality residue (owner decision queued in ROADMAP): curated kraken
trades carry historical subscribe-replay duplicates from before this fix
(dedupe by `(product, trade_id)` on read, or re-promote); coinbase carries one
`last_match` per segment boundary; binance perp aggTrades may carry crash-window
duplicates. Suite 377 → 410; ruff clean; both `.ps1` parse-checked ASCII.

## 2026-06-11 — scheduler-stall incident (15-hour outage masked by a fresh heartbeat)

After an overnight reboot (03:02 UTC) the plant dispatched everything once, then
collected **nothing for 15 hours** while `heartbeat.json` stayed fresh. Three
distinct causes, all fixed in the incident PR:

1. **`score-stream-depth` re-scored every depth run in scope, every hour, in the
   scheduler thread.** `run_backfill_stream_depth` had no already-scored skip, so
   each hourly score-only pass re-CRC-replayed the whole window (~508 runs × ~12 s
   ≈ 100 min of blocked dispatch, growing daily since the self-heal jobs landed).
   Fix: skip runs that already have a `replay_summary.json` unless `--overwrite`
   (a fresh pass dropped from ~100 min to 122 s).
2. **`kalshi-collect` hung inside an HTTP call at boot** — `urlopen`'s timeout does
   not cover DNS/proxy resolution — and it ran *in the scheduler thread*, blocking
   all dispatch and maintenance indefinitely. Fix: both kalshi REST job types moved
   into `COLLECTOR_JOB_TYPES` (pool + subprocess + 7200 s timeout); concurrency
   default 21 → 23; the `.ps1` preflights count `kalshi-*` lanes too.
3. **`StandaloneWorkerLock` trusted pid existence alone.** After hard kills/reboots,
   recycled pids made stale locks read "already active" (one pointed at svchost,
   another at the *new* runner's own worker for a different lane), crash-looping
   three lanes. Fix: pid-alive must be corroborated by a fresh sibling worker
   heartbeat (≤ 10 min), else the lock is stale and broken.

Compounding factors, also fixed: the heartbeat refresher thread kept `last_seen`
fresh while the scheduler was dead (fix: `last_scheduler_tick` advanced only by the
scheduler loop + a `scheduler_stalled` health finding); and `redeploy_runner.ps1`
died on `taskkill` stderr (PS 5.1 `NativeCommandError` despite `2>$null`) before
its own relaunch step — twice — leaving collection down with a stale lock (fix:
PS-native recursive `Stop-Process -ErrorAction SilentlyContinue` kill-tree).

Interim mitigations applied live during the incident (to revert on deploy of the
fix): kalshi jobs `enabled:false`, `score-stream-depth` limit 1000 → 6.

## 2026-06-09 → 06-11 — BTC instrument expansion sprint (PRs #3–#13)

Took the plant from 10 BTC-spot lanes to the full 22-lane matrix in three days.
Condensed; each PR description carries the detail.

- **Phases 1–2 (PR #3):** BTC/USDC spot lanes (Binance) + Bybit USDT linear perp via a
  `--market spot|linear` flag. Coinbase BTC-USDC was planned but the product is
  **delisted** — lanes removed (PR #6).
- **Phase 3 trades (PR #4):** Binance USDT-M perp aggTrades via `--market futures`.
  Live rollout exposed that the **futures websocket is jurisdiction-blocked from this
  box** (acks SUBSCRIBE, streams zero frames; spot WS + `fapi` REST fine), so the WS
  lane was retired in favor of…
- **REST polling collector (PRs #7, #8):** `binance-futures-rest-worker --stream
  trades|depth|funding`. Trades poll `/fapi/v1/aggTrades` with a persisted resume
  cursor (gap-proof, survives segment rotation after the PR #8 cursor fix); depth
  polls full-book snapshots (`none_native`); funding polls `premiumIndex` into its own
  `funding` dataset. One boot 429 (three cold-starting REST workers burst the per-IP
  limit) self-healed on restart.
- **Concurrency-cap starvation, twice (PRs #5, #10/fb75ad9):** the runner's
  `-CollectorConcurrency` default lagged the lane count after each expansion
  (12 < 17, then 17 < 21), silently starving the newest lanes. Now 21. Lesson recorded:
  **bump the cap in `run_ops_runner.ps1` + `redeploy_runner.ps1` whenever lanes are added.**
- **Lambda arg-drop trap, twice (PRs #6, #9):** the per-venue `build_segment_args`
  lambdas silently dropped newly-added fields — first `market` (perp lanes collected
  as spot), then `jsonl_fsync` (config `false` ignored). Fix centralized the
  threading in `_run_segmented_worker` with regression tests.
- **Fsync backlog on hot lanes (PR #9):** the two highest-tick-rate lanes
  (`binance_trades`, `bybit_perp_trades`) grew a `received_at` backlog (~12 s/min)
  from per-event `jsonl_fsync` disk latency, tripping the 60 s clock-skew gate so
  segments stopped promoting. Fix: **batched fsync** in the JSONL sinks (flush every
  line so no torn tails; fsync every 64 events / 200 ms; final batch on close) with
  per-lane knobs. Trade lanes are batched-durable; Binance depth keeps per-event fsync.
- **Phase 5 — OKX spot + linear perp (PR #10):** new normalizers + a `chain_sequence`
  replay mode validating OKX's `prevSeqId`/`seqId` **linked chain by equality**
  (ids aren't contiguous; the link is) — making OKX the second provably-replayable
  depth venue after Binance. Raw-string `"ping"` keepalive + pong guard. Also fixed a
  latent offline-scorer venue-derivation drift on `*_perp_depth` lanes (affected Bybit
  perp too). Live probe confirmed OKX WS is reachable from this box.
- **Cold-tier archive offload (PR #11):** G: fills in ~25 days at ~30 GB/day raw, so
  the `archive-offload` ops job verify-moves aged raw runs `G:` → `D:\market_archive_cold`,
  gated on the run appearing in its lane's promotion or quarantine index, recording
  every move in `_offload_index.jsonl`. First candidates age in ~2026-06-22.
- **Normalized-root drift (PR #12):** normalized Parquet was still landing on the
  abandoned `D:` default for some lanes — per-lane `normalized_root` is now threaded
  explicitly, default flipped `D:` → `G:`. Verified landing on G: after restart.
- **Redeploy alive-check scope (PR #13):** `redeploy_runner.ps1` killed/checked *all*
  `python.exe` processes, taking out unrelated Pythons during the 2026-06-10 redeploy
  (~30-min outage). Now scoped to plant processes only.

Also in this window (no PR): MEXC lanes validated against live frames and enabled
(see the MEXC entry below); both configs settled at 83/84 jobs.

---

## 2026-06-09 — self-heal cut-off segments via score-only catch-up jobs (PR #1)

A segment cut off mid-finalize (clean events written to disk, but no inline
`metrics/replay_summary.json`) was invisible to curation: `promote_replayable_runs`
skips any run without a summary (`skipped_missing_replay`), so those rows never reached
the curated dataset. Fixed by making the trades + non-binance-depth scorers dispatchable
as **ops job types** (`backfill-trades-replay`, `backfill-stream-depth` — previously
CLI-only) and adding hourly per-lane `score-*` catch-up jobs that (re)write each run's
`replay_summary.json`. The depth scorer runs in a new `--score-only` mode that writes
summaries but does **not** promote, so the quarantine-aware `promote-replayable` jobs
stay the *single* promoter into the curated parquet — two concurrent promoters would
duplicate curated rows (the promotion index can't dedup a run it hasn't recorded yet).
8 tests incl. an end-to-end self-heal + no-duplicate-rows acceptance test. The same PR
added GitHub Actions CI (windows-latest, py3.11+3.12) — which exposed that local "284
passed" leaned on the workstation's >100 GB disk + live data; 5 non-hermetic
health/runner tests were made hermetic.

## 2026-06-09 — Binance emits its REST snapshot as a clean event (084f8c9)

coinbase/bybit/kraken/mexc receive their book snapshot **in-stream** (the WS sends a
`type:snapshot` frame, normalized to a clean event with `event_type="snapshot"`), so
each run's curated data is self-contained for replay. **Binance was the exception:** its
diff-depth WS sends no snapshot frame — the seed is fetched via REST and was written only
to the sidecar `…/snapshots/book_snapshot.json`, so binance clean/curated rows were pure
`depthUpdate` deltas with **no snapshot row**, and the curated `market_replayable`
dataset could not be replayed for binance without the raw sidecar.

**Resolved:** after the REST snapshot is captured, the collector synthesizes a
binance-format snapshot `RawMessage` (`e="snapshot"`, `U=u=lastUpdateId`, `b`/`a` =
snapshot levels), runs it through `BinanceDepthNormalizer` (correct `event_type="snapshot"`
clean row), and writes it to `clean_sink` + `parquet_sink` as the FIRST clean event
(bypassing the quality gate — the REST snapshot is authoritative). Collector + replay
tests cover the leading-snapshot clean event so it doesn't double-seed. Confirmed live
2026-06-09. **Residual** (tracked in ROADMAP): partitions collected *before* the fix
still lack a snapshot row and would need re-promotion for self-contained historical replay.

## 2026-06-09 — bare `health` follows the config's normalized root

Was: `_latest_partition_write` (ops.py) used env-based `default_normalized_root`, so a
bare `health` with no env set checked the abandoned `D:\market_archive\normalized` and
emitted a false `stale_partition:binance-*` (monitoring-only artifact in ad-hoc runs; the
runner with the correct env was unaffected). **Fixed** the same way as the ops-root fix
(`2d3a415`): `_normalized_root_from_jobs()` derives the live normalized root from the
discovered config, threaded through `build_health_report → _latest_partition_write`
via an optional `normalized_root`. When unset the env/default fallback is preserved.
Regression test: `test_health_follows_config_normalized_root_not_env_fallback`.

## 2026-06-08 — Continuous capture: time-based segment rotation + full concurrency

**Problem:** every collector lane was only recording ~one short segment per hour then
idling. Root cause: each lane is `max_segments=1` and the runner re-dispatches it at
`finish + interval_seconds` (interval was 3600 s), so the idle gap after each segment ≈
the interval. Measured coverage was brutal — coinbase trades ~13%, binance trades ~17%,
kraken/mexc ~15–40%, coinbase_depth ~8%, kraken_depth ~2%. Quality was fine (replayable,
0 quarantine); *continuity* was broken. Also only `collector_concurrency=4` of 10 lanes
could run at once.

**Fix (continuous capture):** new `max_segment_seconds` knob → segments rotate on a
fixed wall-clock cadence regardless of volume (set to **1800 s** for all lanes);
`interval_seconds` → **5** (re-dispatch ~immediately); `segment_count` → **100000**
(safety cap; the time bound fires first); `collector_concurrency` raised so all lanes
stream simultaneously. Net: each lane records continuously, rotating a finalized 30-min
segment with only a ~5–8 s reconnect gap (~0.3–0.4% per segment — eliminating it needs
separating connection lifecycle from file lifecycle; tracked in ROADMAP).

## 2026-06-08 — Live collection migrated D: → G: (NVMe)

Cut live collection from `D:\market_archive` to `G:\market_archive` (NVMe). **Why:** the
D: disk couldn't keep up with concurrent collection — high-volume trade lanes backlogged
past the 60 s freshness gate and quarantined valid, merely-late trades (coinbase ~0.55,
bybit ~1.0 quarantine ratios). Software fixes shipped first — Binance depth
snapshot-anchor (`206beb5`), trades buffered JSONL (`9e6e50b`), 900 s trades stale gate
(`a4bfd9d`), collector process isolation (`a7b9544`) — but the real bottleneck was disk
I/O, so the NVMe cut-over is what removed it. On G:, **all five trade lanes ran at 0%
quarantine**. `ops.live.local.json`, `run_ops_runner.ps1`, the runbook, and the
scheduled task all point at G:. `D:\market_archive` is kept read-only as history —
the retention/merge decision is tracked in ROADMAP.

## 2026-05-30 → 06-04 — venue expansion groundwork ("Next steps after Phase 2")

All six items landed:

1. **Real-socket validation of Coinbase/Kraken/Bybit lanes (2026-05-31).** All 6 lanes
   ran bounded real-socket segments to a throwaway archive and produced
   `replayable: true` with the expected contract. One real bug: **Coinbase depth
   `level2_batch` is dead** (public `level2`/`level2_batch` now require auth) —
   switched to `level2_50` and raised WS `max_size` for its ~1.4 MiB snapshot
   (`d616e52`).
2. **`instrument=` partition column (2026-06-01).** Parquet `schema_version` v1→v2
   cutover: v2 partitions on `["schema_version", "source", "instrument", "event_date"]`,
   deriving `instrument` from the canonical symbol (`BTC/USDT`→`BTC-USDT`); the resolved
   `InstrumentRef` moved to an `instrument_ref` column. v1 data untouched.
   `STANDARDS_VERSION` 3→4.
3. **App-level keepalive ping for Bybit (2026-05-31).** Opt-in
   `CollectorConfig.ping_message` + `ping_interval_seconds`; per-connection keepalive
   task. Bybit lanes opt in at `{"op":"ping"}` / 20 s; every other venue relies on
   protocol-level ping/pong.
4. **Stronger gap-proofing for `none_native` depth.** Bybit `data.u` proved dense
   (+1 per message, 60/60 live frames) → upgraded to provable `sequence` (`b5ca110`,
   `STANDARDS_VERSION` 1→2). Kraken CRC32 solved empirically (asks top-10 asc then bids
   top-10 desc, decimal stripped) and verified against live snapshot+update checksums →
   `gap_detection="checksum"` (`STANDARDS_VERSION` 2→3) with a frozen golden-vector test.
5. **Data-arrival watchdog (2026-06-01).** Opt-in `idle_timeout_seconds`: a stalled WS
   ends the segment **cleanly** (fresh segment = clean single-snapshot run) instead of
   blocking forever; surfaced as `idle_timeout_count` + a non-blocking health finding.
6. **Parallel collection runner (2026-06-03).** Collector job types dispatch through a
   `ThreadPoolExecutor` sized by `--collector-concurrency`; maintenance jobs stay
   serialized in the scheduler thread. Heartbeat gained `current_jobs`; health treats
   any active job as in-progress. Barrier-proven concurrency tests.

Plus two late additions:

7. **Multi-anchor stream-depth replay + backfill (2026-06-03).** Root cause of
   Coinbase/Bybit/Kraken depth showing **0 curated rows** while raw collection was
   clean: `replay_depth_stream_run` required exactly one snapshot anchor at position 0
   (the Binance REST model), but stream-snapshot venues re-snapshot mid-run by design.
   Fixed by re-anchoring at each in-stream snapshot and validating integrity **within
   each sub-book**, plus a `book_depth` param trimming Kraken's book to the subscribed
   top-N (Kraken silently evicts the worst level past depth — ~90% of frames CRC-diverged
   without the trim). Added `backfill-stream-depth` CLI. `STANDARDS_VERSION` 4→5.
8. **MEXC spot adapter — protobuf transport (2026-06-04, live 2026-06-09).** MEXC
   retired its JSON websocket 2025-08-04; public market data is now Protocol Buffers on
   `wss://wbs-api.mexc.com/ws` — the only binary-transport venue. Vendored `.proto` +
   committed generated bindings + an opt-in `CollectorConfig.message_decoder` seam
   (binary frames → payload dict; JSON ack/PING unchanged). Both lanes `none_native`
   (aggregated deals carry no per-trade id; limit-depth pushes independent full top-N
   books). Raw frames carry a `_mexc_decode` provenance block (schema/sha256/base64) so
   raw stays a rebuild source. Shipped disabled (schema built from published docs, not a
   live capture); validated against real frames and enabled live 2026-06-09 — both lanes
   promoting clean curated rows (~79.5k trades / ~86.8k depth at verification).

## 2026-05-25 — initial hardening (items closed over the following week)

1. **Reconnect depth-worker in place** — `collect_binance_depth_segment` reconnects on
   retryable WS errors reusing the original snapshot anchor against a rolling
   `last_seen_final_update_id`; a post-reconnect gap ends the segment cleanly. Metrics:
   `reconnect_count`, `alignment_break_count`.
2. **Quality-gated curation chain for trades** — `replay_trades_run` gives every trades
   run the same `{replayable, findings}` summary the depth chain uses; quarantine +
   promote work unchanged. Bar: trade_id monotone + gapless, price/size finite-positive,
   exchange_time within clock-skew gate.
3. **Real durability test under SIGKILL** — `tests/test_durability.py` kills the mock
   pipeline subprocess mid-write (`TerminateProcess`) and asserts every JSONL line
   parses and the file ends with a newline. Removing per-write fsync fails the test.
4. **`health` consumes partial metrics** — last row of the in-flight run's
   `metrics/summary.jsonl` surfaces as `partial_metrics` + `quarantine_ratio`, with a
   `high_quarantine_ratio:<worker>` finding for active workers past the threshold.
5. **Wake-from-sleep for the scheduled task** — `-WakeToRun` added (no-op for the
   boot/logon triggers, ready for time-based ones).
6. **ParquetDatasetSink batch_size 1000 → 100** — lost-on-kill window for the
   normalized layer now ~100 events (raw JSONL stays fsync-durable).

## Retired — `Crypto_L3 collection` (2026-06-09)

The predecessor project is **retired**: its `CryptoL3Collector` +
`CryptoL3MarketSupervisor` scheduled tasks were removed and the tree archived to
`G:\04-archive\Crypto_L3 collection`. Not a re-enable candidate — any of its unique
feeds still wanted get built as native lanes in this plant.
