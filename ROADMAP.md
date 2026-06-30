# Roadmap

**This file is the single source of truth for plans and open work.** It is
maintained by the project manager (Claude) and updated in every session that
changes scope or state. Companion docs:

- [`README.md`](README.md) — what the plant collects *today* (capability snapshot)
- [`STANDARDS.md`](STANDARDS.md) — the data contract (schemas, replayability, retention)
- [`docs/HISTORY.md`](docs/HISTORY.md) — resolved-work narrative (what was fixed, and why)

Last updated: **2026-06-30**.

> **2026-06-22..24 session note:** Ran the due 06-18 offload-index spot-check (PASS,
> see dated table). Discovered G: had fallen back to **3.9 GB free** because the
> 06-17 Kalshi-normalized preservation move never finished; completed the final
> `robocopy /MOVE` G:->D: (owner-approved, detached) -> **G: now 489 GB free,
> 0 FAILED** (see 06-19 row). Kalshi normalized is now fully off G:.
> **STILL DUE: a full ops audit** (health report, per-lane freshness/backlog, job
> success rate since last restart, quarantine ratios) — the 06-17 stamp is now
> >7 days old; this session only covered offload + disk headroom. Also open: the
> **16 `stuck_unaccounted_runs`** promotion gap surfaced by the offload dry-run.

---

## Current state (2026-06-17)

**21 enabled collector lanes** across Binance (spot USDT + USDC, USDT-M perp via
REST), Coinbase, Kraken, Bybit (spot + linear perp), MEXC, OKX (spot + linear
perp) — all BTC. **Kalshi crypto-binary collection is TURNED OFF as of
2026-06-17** (both Kalshi jobs `enabled:false`; it was the G:-full root cause —
see the audit note + Decision queue). Full quarantine → promote curation chain per
lane, hourly score catch-up self-heal, research manifest, cleanup retention, and
cold-tier archive offload. Live runner redeployed 2026-06-17 (pid 42916,
`ops.live.local.json`); all 21 lanes green post-restart. CI green on `main`.

---

## Dated operational checks

| Due | Check |
| --- | --- |
| ~~2026-06-18~~ DONE 06-22 | Offload-index spot-check **PASSED**: 4509 index rows == cold run-dirs 1:1 on every lane, 0 duplicates/malformed, 0 unindexed pile-up, 0 missing cold copies, 0 sampled file-count mismatches, 0 indexed runs still hot. Offload live (newest `moved_at` 2026-06-22T09:53Z). Dry-run also flags **16 `stuck_unaccounted_runs`** (raw from 06-09..06-11 never promoted: 8 `binance_perp_funding` + 8 trade/depth) — designed safety surface, but a real promotion gap to investigate. |
| ~~2026-06-19~~ DONE 06-24 | The 06-17 `robocopy /MINAGE:3` move never finished (~88% of partitions still on G:), leaving G: at **3.9 GB free**. First retry (06-22) was killed by the Bash tool's 10-min timeout after freeing ~57 GB. Relaunched **detached via `Start-Process`** (pid 48444) so it survives session/tool teardown -> **COMPLETED 2026-06-24 16:19, FAILED: 0** (45.29 M files / 555 GB moved G:->`D:\market_archive_cold`). **G: now 489 GB free.** D: holds 113,407 normalized partitions (full set). 1 partition / 2 parquet files remain on G: -- robocopy *skipped* them (already byte-present on D: from the 06-17 partial), so redundant not stranded; immaterial (489 GB free). Lesson: long-running moves must be detached, never run inside a Bash call (10-min cap). |

**Last ops audit:** 2026-06-30 — **plant GREEN.** Health `status=warn` with a
single finding `binance_trades_no_replayable_30m`, which I verified is a FALSE
ALARM (metric bug; fix PR in flight): the check measures the latest replayable
run's age from its segment START against a flat 1800 s, but binance_trades runs
1800 s segments, so the newest replayable run is always >=1800 s old the moment
it closes -> the finding fires continuously on a healthy lane (9/9 recent runs
replayable, clean_ratio 1.0, 0 trade-id gaps, 722k promoted rows, promote-trades
0 errors). Everything else green: runner pid 42916 alive (heartbeat ~3-13 s),
**81/81 jobs `status=success`** (0 stale, 0 partition_stale; promote/quarantine/
score jobs all 0 errors), **21/21 standalone workers fresh** (no ghosts), no
`high_quarantine_ratio` finding, **G: 488.7 GB free (25.6%)**, offload-index
spot-check PASS (4509 rows == cold dirs 1:1, 0 unindexed). Open low-severity
item: **16 `stuck_unaccounted_runs`** (raw from 06-09..06-11 that never promoted:
8 binance_perp_funding + 8 trade/depth) -- DIAGNOSED 2026-06-30 as incident residue
(aged past the 168 h scoring+quarantine window, now permanent orphans), current
pipeline healthy -> owner cleanup/backstop call in the Decision queue.

**Resolved 2026-06-17 incident (kept for reference):** G:-FULL INCIDENT (0 bytes
free). All collectors crash-looped on `OSError [Errno 28] No space left on device` (lock
creation) from ~11:15 to ~12:17 UTC; the runner (pid 29176) stayed alive but
wedged (could not write heartbeat/locks/job log) — ~1 h of data loss across every
lane. **Root cause: `G:\market_archive\normalized\binary_options` (Kalshi) was
624 GB / 53.6 M files / 112,692 per-strike `instrument=` partitions and is
COMPLETELY UNMANAGED** — `archive-offload` only handles raw run-dirs, and
`cleanup` only removes *zero-byte* parquet under `normalized` (and is dry-run).
The 2026-06-15 capacity model tracked RAW only and never accounted for
normalized, which is the real grower: ~60-68 GB/day (≈4x the 16 GB/day Kalshi
*raw* — normalization is *inflating* via one tiny parquet per strike-market per
run, ~6 M files/day; see Decision queue P0). Plant footprint: market_archive
~834 GB (normalized 624, raw 201, curated 8.8, quarantine ~0). G: is a SHARED
1.9 TB volume — non-plant ~635 GB (01-active 73.5, 03-archive 267.7, 04-archive
267.7, Binance_IV_V1 26.7). **Emergency relief applied this session (both
reversible — moves, not deletes; data also reconstructable from Kalshi raw on
D:):** (1) manual `archive-offload --min-age-days 7.5` → freed 18 GB (872 runs to
D:), plant resumed 12:17 UTC; (2) `robocopy /MOVE /MINAGE:3` of normalized
G:->`D:\market_archive_cold\normalized\binary_options` (keeps last 3 days hot,
~204 GB; moves ~408 GB / 35.7 M files older than 3 days). **No durable recurring
normalized retention exists yet — owner decision queued below; the one-time move
buys ~1 week of headroom.** Earlier raw-side work still holds: 3-day Kalshi raw
offload (PR #23) + indexed lanes at 10-day offload age (start draining ~06-18) +
`archive-offload-cold` ordered ahead of `cleanup-dry-run`. D: cold ~2% used
(~7.18 TB free). **RESOLUTION (owner decision, 2026-06-17): KALSHI COLLECTION
TURNED OFF.** Both Kalshi jobs (`kalshi-crypto-discovery`,
`kalshi-crypto-quotes`) set `enabled:false` in `ops.live.local.json` and the
runner redeployed (`redeploy_runner.ps1`, new pid 42916, all 21 worker lanes
green, 0 Kalshi procs). This stops ~76 GB/day (≈78% of plant write volume) at the
source and makes the normalizer-fix + normalized-offload PRs unnecessary for now.
Existing ~611 GB Kalshi normalized is being PRESERVED to
`D:\market_archive_cold\normalized\binary_options` (owner chose preserve over
delete) — the `robocopy /MINAGE:3` move handles 3+ day partitions; a final sweep
of the last 3 days is the only remaining step now that writes have stopped. With
Kalshi off, plant write volume drops to ~22 GB/day raw (bounded by the existing
10-day offload) + a trickle of `normalized/{market,trades}` (~12 GB, slow-growing,
still unmanaged — minor). Ritual: if this stamp is more than ~3 days old at
session start, audit the live plant first — see `CLAUDE.md` "Quality gates".

---

## Open work items (rough value order)

1. **D:\market_archive legacy history — decide retention or merge.** The pre-2026-06-08
   D: archive is kept read-only as history. Decide: backfill/merge its runs into the
   G: curated dataset (score with `backfill-trades-replay` / `backfill-stream-depth
   --score-only`, then let the promote jobs pick them up) or declare it cold history
   and leave it. Blocks nothing, but the disjoint pre-cutover data limits historical
   research coverage.
2. **Phase 6 candidate — inverse (coin-margined) BTCUSD perps.** Natural next
   instrument-expansion step after the linear-perp triangle. Note: Binance USDT-M
   *websocket* is jurisdiction-blocked from this box (REST works — see Constraints),
   so plan venue choice accordingly (Bybit/OKX inverse WS, or Binance dapi REST
   mirroring the fapi REST lanes).
3. **OKX funding channel.** Deferred from Phase 5. Would mirror the
   `binance-futures-rest-funding` lane (`funding-rate` channel or REST poll) so both
   perp venues carry funding context.
4. **MEXC depth → provable `sequence` upgrade.** The pushed `version` is already
   captured as `metadata.mexc_version`; if live frames prove it dense per symbol,
   upgrade the lane the way Bybit depth was upgraded (`data.u` +1). Until then depth
   stays `none_native`.
5. **Re-promote pre-fix Binance depth history (optional).** Binance depth partitions
   collected before commit `084f8c9` (2026-06-09) lack the leading synthesized
   `snapshot` row, so self-contained replay of those dates needs re-promotion from
   raw. Only matters if historical self-contained replay is wanted.
6. **Kraken checksum precision table for non-BTC/USD pairs.** `_KRAKEN_BOOK_PRECISION`
   covers BTC/USD only; other pairs fall back to `none_native`. Moot until a non-BTC
   Kraken pair is actually collected; could auto-fetch from REST `AssetPairs`.
7. **Day-bounded rotation as the default run model.** `--rotate-at-midnight` exists
   and works; the live model is 30-min wall-clock segments (`max_segment_seconds=1800`).
   Parked — analysts pull by `event_date` partition, so per-run boundaries rarely matter.
8. **fapi REST 429 handling — honor Retry-After / pace cold-start bursts.** Audit
   finding (real, deferred as a design change): a 429 currently crashes the segment
   (self-heals by restart, seen once at boot 2026-06-09) and a seeded resume fires
   up to 5 unpaced catch-up pages; repeated 429s risk escalation to a fapi 418 IP
   ban. Add Retry-After-aware backoff in `_get_json` / pacing between catch-up pages.
9. **Zero-gap segment rotation.** The ~5–8s WS reconnect between segments costs
   ~0.3–0.4% per segment. Eliminating it means separating connection lifecycle from
   file lifecycle in the collector core — a real refactor, parked unless that loss
   starts to matter.
10. **Ops-root JSONL log rotation/retention.** `job_runs.jsonl`,
    `heartbeat_history.jsonl`, and `worker_events.jsonl` grow unbounded (~3–5k
    rows/day). The 2026-06-12 audit made health tail-read the run log (cost
    contained), but the files themselves still need a rotation or retention policy
    — fold into `run_cleanup`.
11. **Verify OKX/Bybit trades subscribe-replay behavior over live frames.** The
    audit fixed subscribe-time print replays for Kraken (`snapshot` frame) and
    Coinbase (`last_match`); review suggested OKX may push the latest historical
    trade on subscribe and Bybit's first `publicTrade` push may carry recent
    trades. Both lanes are `none_native` with run-keyed promotion, so untagged
    replays would accumulate small duplicate counts at every reconnect. Capture a
    few live (re)subscribes for each, check whether the first data frame
    re-delivers pre-subscription prints, and if so tag them `subscribe_replay`
    like Kraken/Coinbase.
12. **Local-only modelling raw lanes are unconfigured in `archive-offload`.**
    A few raw lanes that exist only in the gitignored local config surface as
    benign `unconfigured_lane` warnings every offload pass and have no retention
    bound (tiny today, but unbounded). Fix: add per-lane `gate: age_only` entries
    in the local-only `ops.live.local.json` (lane identities/specifics stay local
    per the public-safe contract). Left unactioned this session: tiny, not the
    G:-full cause, and touching local-only modelling-data lifecycle wants owner
    awareness.

## Decision queue (owner)

Decisions waiting on the owner; agents must not act on these without an explicit OK
(see `CLAUDE.md` Governance):


- **D:\market_archive legacy history** — retention vs. merge (open item 1 above).
  Owner deferred 2026-06-11: stays read-only until research needs pre-cutover dates.
- **2026-06-13 modelling data-collection handoff (strategy-sensitive — ALL
  specifics in the gitignored local request doc).** A read-only, re-fetchable
  historical backfill feeding a frozen modelling study. Triaged: prior coverage
  was short, so a backfill was warranted; manager built + validated the pipeline
  (autonomous zone: re-fetchable public data, no money, no live lane, no auth).
  Owner nod was wanted only on the full pull's scale. Source, fields, volumes,
  and every other specific stay in the local doc — not here.
- **Historical curated duplicates (2026-06-12 audit residue).** Until the audit
  fixes deploy+age in, curated data carries known duplicates: kraken trades (up to
  ~50 subscribe-replay prints per segment boundary since the lane went live),
  coinbase trades (one `last_match` per boundary), and possibly binance perp
  aggTrades (crash-window re-fetches). Options: (a) document + dedupe by
  `(product, trade_id)` at read time in research consumers, or (b) re-promote the
  affected lanes from raw on the fixed code (touches curated data — owner call).
  New capture is clean once the fix PR deploys.
- **16 `stuck_unaccounted_runs` from the 2026-06-09..11 incident window (diagnosed
  2026-06-30).** Interrupted/partial or never-scored raw runs (8 binance_perp_funding
  + 8 trade/depth across lanes) that aged past the 168 h scoring+quarantine window
  before being accounted, so they are now permanent orphans: promote needs
  `replayable`, quarantine needs a fresh `replay_summary.json` within 168 h, and
  neither job will ever touch them again -> offload refuses to move them (by design)
  and reports them as `stuck_unaccounted_runs` forever. NOT an active bug -- the
  cohort is tightly clustered in the incident window and the current pipeline scores
  every run within the window (verified: recent lanes have only the live in-progress
  segment unscored). They are tiny (partial/corrupt captures, a few MB total). Latent
  gap: nothing back-stops runs that age out unscored, so a future multi-day incident
  will mint new permanent orphans. Options: (a) one-time cleanup -- quarantine the 16
  with diagnostics bundles (preserves them, clears the warn-noise, lets offload move
  them) or delete (they're interrupted garbage); (b) build a durable backstop job
  that quarantines aged unaccounted runs (code change, but changes data accounting ->
  owner sign-off); (c) leave as-is and accept the permanent offload warn-noise. All
  three touch data retention / accounting = owner-gated.

Decided 2026-06-17 (recorded, closed):
- **Kalshi collection TURNED OFF (the G:-full root cause).** The `normalized`
  blind spot (Kalshi binary_options = 624 GB / 53.6 M files / 112,692 per-strike
  partitions, growing ~60 GB/day = ~4x its raw, unmanaged by both offload and
  cleanup) filled the shared 1.9 TB G: to 0 bytes and wedged the runner for ~1 h.
  Owner chose to **disable Kalshi** rather than build a normalized-offload +
  fix the per-strike partitioning (both PRs now unnecessary): both Kalshi jobs
  `enabled:false` in `ops.live.local.json`, runner redeployed (pid 42916). Stops
  ~78% of plant write volume. **Existing 611 GB normalized is being preserved to
  `D:\market_archive_cold` (not deleted)** — `robocopy /MINAGE:3` move in flight;
  final last-3-days sweep pending now that writes stopped. REVERSIBLE: re-enable
  the lanes to resume (ideally only after fixing the partitioning so the data is
  usable). Kalshi *raw* stays the re-normalizable source on D:. NOTE: did NOT edit
  the committed `ops.live.example.json` (template keeps Kalshi as a documented
  capability); the live state lives in the gitignored local config + this entry.
Decided 2026-06-13 (recorded, closed):
- **Kalshi raw retention at continuous volume: option (b) — per-lane
  `min_age_days: 3` override on the Kalshi lane** inside the single
  archive-offload job (job default stays 14 for the indexed lanes; Kalshi is
  `age_only` because its curation is inline, so nothing downstream needs the
  raw hot). Cuts G: steady-state raw-in-flight from ~530 GB to ~355 GB
  (~190 GB headroom vs ~548 GB free). D: inflow is unchanged (~38 GB/day,
  ~6-month horizon) — the delete-or-compress question returns when D: passes
  ~50%. Code + config; **deploys at the next runner restart**. First pass
  drains a ~1,500-run burst-era backlog at the 200-runs/hour limit (~8 h,
  verify-staged). Review note: the first cut used a second offload job, which
  the /code-review pass killed — every offload job warns `unconfigured_lane`
  for raw dirs it doesn't own, so overlapping jobs are permanent warn-noise; a
  repo-hygiene test now pins "each lane appears in at most one offload job".
  A daily scheduled check watches the rotation until proven.
- **2026-06-13 modelling-side collection request — all four items closed**
  (strategy-sensitive venue — details in the gitignored local request doc):
  (i) a perishable local-only capture lane was approved as a **local-only
  artifact**, deployed 2026-06-12 ~23:42 UTC, and converted to a SYSTEM task
  2026-06-13 ~00:53 UTC (boot-resilient; verified 3.2 s max capture gap across
  the conversion). (ii) A strategy-sensitive historical backfill (read-only,
  re-fetchable) completed to the **D: cold tier** with a breadcrumb in the G:
  tree; a recurring task was declined 2026-06-13 then **REVERSED 2026-06-14 —
  owner now wants the live collector**, re-registered as a dedicated
  forward-collector scheduled task (now SYSTEM / boot-resilient). All source,
  field, and volume detail stays in the gitignored local doc — not here.
  (iii) was already satisfied by live capture; (iv) moot after the continuous
  Kalshi switch.

Decided 2026-06-11 (recorded, closed):
- Incident-fix PR #17 merged + deployed same day; kalshi re-enabled as pool jobs,
  `score-stream-depth` limit restored to 50, runner verified stable.
- Housekeeping deletions executed (bak configs, screenshots, `.tmp_research/`,
  all merged remote branches — origin now carries `main` only).
- Active alerting for blocking health findings: **declined** — the session-start
  audit ritual's ~3-day detection latency is accepted.
- Baseline-audit completion (open item 0): **approved** for the next session,
  slim design; the deferred PR #17 review pass folds into its ops-runner pass.

## Environmental constraints (verified, not bugs)

- **Binance USDT-M futures websocket is blocked from this location** — `fstream` acks
  SUBSCRIBE but streams zero frames (even `markPrice@1s`); spot WS and `fapi` REST are
  fine. Hence the REST-polling perp lanes. Re-test before assuming it changed.
- **Coinbase BTC-USDC is delisted** — do not re-add those lanes.
- **Non-elevated sessions** can't read the SYSTEM task's arguments, other users'
  process command lines, or create `Global\` mutexes. The live boot task
  (`CryptoMarketDataPlant`, SYSTEM, `PT0S`) is invisible to non-elevated `schtasks`.
- Plant `.ps1` scripts must stay **ASCII-only** (UTF-8-no-BOM + PowerShell 5.1
  misdecodes em-dashes into string-terminating curly quotes). Parse-check after edits.

## Portfolio coverage (cross-repo)

BTC derivatives/market-data coverage is split across repos by design:

| Slice | Owner |
| --- | --- |
| Spot order books + trades (6 venues), linear perps (Bybit, OKX, Binance-via-REST), Binance funding | **this plant** |
| Kalshi crypto binary-option quotes | **this plant** |
| Options chains + IV surface (Binance `eapi` BTC+ETH, ~2-min cadence; Deribit source) | `G:\Binance_IV_V1` (separate live repo) |
| CME futures | out of scope (paid data) |

## Retired (not candidates)

- **Kalshi near-expiry burst sampling (modelling-side request, 2026-06-11)** —
  REJECTED by the owner 2026-06-11: the live lane already samples every ~9 s
  per market (close to the requested 5-10 s), and settlement outcomes can be
  downloaded from the API directly, so the backtest does not depend on captured
  quotes at close. Response recorded in the local request doc.
- **`Crypto_L3 collection`** — retired 2026-06-09, tree archived to
  `G:\04-archive\Crypto_L3 collection`, scheduled tasks removed. Any feed it had
  that's still wanted gets built as a native lane here instead.
- **Deribit perps** — dropped from the instrument-expansion plan (options-side
  Deribit data is covered by `Binance_IV_V1`).

---

## Manager protocol

- Plans, decisions, and dated checks live **here**, not in chat history or agent
  memory. Memory holds pointers; this file holds the plan.
- Every session that changes scope, completes an item, or makes a decision updates
  this file in the same change.
- At session start: check the dated table above and flag anything due.
- Completed work moves to [`docs/HISTORY.md`](docs/HISTORY.md) with its root-cause
  narrative; this file stays short and forward-looking.
