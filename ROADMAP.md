# Roadmap

**This file is the single source of truth for plans and open work.** It is
maintained by the project manager (Claude) and updated in every session that
changes scope or state. Companion docs:

- [`README.md`](README.md) — what the plant collects *today* (capability snapshot)
- [`STANDARDS.md`](STANDARDS.md) — the data contract (schemas, replayability, retention)
- [`docs/HISTORY.md`](docs/HISTORY.md) — resolved-work narrative (what was fixed, and why)

Last updated: **2026-07-04**.

---

## Current state (2026-07-04)

**21 enabled collector lanes** across Binance (spot USDT + USDC, USDT-M perp via
REST), Coinbase, Kraken, Bybit (spot + linear perp), MEXC, OKX (spot + linear
perp) — all BTC. **Kalshi crypto-binary collection is TURNED OFF as of
2026-06-17** (both Kalshi jobs `enabled:false`; it was the G:-full root cause —
see `docs/HISTORY.md` 2026-06-17 + Decision queue). Full quarantine → promote
curation chain per lane, hourly score catch-up self-heal, research manifest,
cleanup retention, and cold-tier archive offload. Live runner restarted at boot
2026-07-02 ~17:20 UTC (machine reboot; SYSTEM task, `ops.live.local.json`) on
then-current `main`, so the PR #24 health fix is deployed; PRs #26/#28 (audit
docs + offload observability) merged 07-04 and **await the next runner restart**
to take effect in the runner. All 21 lanes green. CI green on `main`.

---

## Dated operational checks

| Due | Check |
| --- | --- |
| ~~2026-06-18~~ DONE 06-22 | Offload-index spot-check **PASSED**: 4509 index rows == cold run-dirs 1:1 on every lane, 0 duplicates/malformed, 0 unindexed pile-up, 0 missing cold copies, 0 sampled file-count mismatches, 0 indexed runs still hot. Offload live (newest `moved_at` 2026-06-22T09:53Z). Dry-run also flags **16 `stuck_unaccounted_runs`** (raw from 06-09..06-11 never promoted: 8 `binance_perp_funding` + 8 trade/depth) — designed safety surface, but a real promotion gap to investigate. *(Re-measured 2026-07-04: the true cohort is **14,211** — the 06-16..06-23 crash-loop debris had not yet crossed the 10-day offload fence when this check ran. See the 07-04 audit stamp + Decision queue.)* |
| ~~2026-06-19~~ DONE 06-24 | The 06-17 `robocopy /MINAGE:3` move never finished (~88% of partitions still on G:), leaving G: at **3.9 GB free**. First retry (06-22) was killed by the Bash tool's 10-min timeout after freeing ~57 GB. Relaunched **detached via `Start-Process`** (pid 48444) so it survives session/tool teardown -> **COMPLETED 2026-06-24 16:19, FAILED: 0** (45.29 M files / 555 GB moved G:->`D:\market_archive_cold`). **G: now 489 GB free.** D: holds 113,407 normalized partitions (full set). 1 partition / 2 parquet files remain on G: -- robocopy *skipped* them (already byte-present on D: from the 06-17 partial), so redundant not stranded; immaterial (489 GB free). Lesson: long-running moves must be detached, never run inside a Bash call (10-min cap). |

**Last ops audit:** 2026-07-04 — **plant GREEN operationally; one material
accounting correction.** Health `status=ok`, **0 findings** (heartbeat ~3 s).
The box **rebooted 2026-07-02 ~17:20 UTC**; the SYSTEM boot task restarted the
runner on current `main`, so **PR #24 (segment-aware binance_trades freshness)
is now LIVE** and the 06-30 false-alarm finding is gone. Jobs since restart:
**21,800/21,853 success (99.76%)**; all 53 errors self-healed (a boot-time
worker-lock race on okx-perp-depth, 50 errors in 60 s, + 3 transient REST
blips). 21/21 workers fresh (<=27 s, no ghosts); all 21 lanes raw-fresh <=4 s;
curated `latest_event_date=2026-07-04` on every lane; quarantine ~11 MB total
(ppm-level ratios). Disk: **G: 432.5 GB free** (-56 GB since 06-30; drivers:
`normalized/{market,trades}` now **66.2 GB**, ~3 GB/day, still unmanaged — see
open item 2 — plus non-plant shared-volume growth), D: cold 6.07 TB free.
Offload mechanics verified (5/5 spot-checked runs on D: and gone from G:;
22,784 index rows; moves same-day). **CORRECTION: `stuck_unaccounted_runs` is
actually 14,211 (~95 GB hot on G:), not 16.** Verified twice (agent + a
main-session re-run of the dry-run with the exact live job args). The cohort is
06-09..06-23, dominated by **crash-loop run-dir debris** from the 06-17 ENOSPC
crash-loop and the 06-21..22 robocopy-contention window (binance_depth alone:
157 stuck dirs dated 06-17, 172 on 06-21, **1,266 on 06-22** vs ~48 normal
segments/day — one dir per restart), all aged past the 168 h scoring/quarantine
window -> permanent orphans by design. The "16" was the 06-22 measurement,
taken **before this cohort crossed the 10-day offload fence**; 06-30 carried it
forward without re-measuring. Curated impact bounded: promoted segments/day
held 34-49 through the window (intraday holes of order hours on
06-17/06-21/06-22). **Closed population — 0 new orphans post-06-24.**
Detection gap: the hourly offload job reports `status=warn` +
`stuck_unaccounted_runs:14211` but the runner fails jobs only on
`failed_count` and `health` never reads offload reports, so this hid behind
"all jobs success" — **fixed same day, PR #28** (see open item 3; deploys at
the next runner restart; until the cohort cleanup, audit with
`health --stuck-unaccounted-baseline 14211`). Decision-queue item updated with
the true scale. Cosmetic: one stale 0-byte `bybit-depth-worker-perp.json.*.tmp`
heartbeat artifact from the 06-17 redeploy (inert).

**Ritual:** if this stamp is more than ~3 days old at session start, audit the
live plant first — see `CLAUDE.md` "Quality gates".

(Previous audit 2026-06-30: green, 81/81 jobs, false-alarm
`binance_trades_no_replayable_30m` diagnosed -> fixed in PR #24, now deployed.
The resolved 2026-06-17 G:-full incident — Kalshi normalized blind spot, ~1 h
data loss, Kalshi turned off — now lives in `docs/HISTORY.md` 2026-06-17;
its live remnants are the Kalshi-off state above, the stuck-cohort +
normalized-retention items below, and Kalshi raw preserved on D:.)

---

## Open work items (rough value order)

1. **D:\market_archive legacy history — decide retention or merge.** The pre-2026-06-08
   D: archive is kept read-only as history. Decide: backfill/merge its runs into the
   G: curated dataset (score with `backfill-trades-replay` / `backfill-stream-depth
   --score-only`, then let the promote jobs pick them up) or declare it cold history
   and leave it. Blocks nothing, but the disjoint pre-cutover data limits historical
   research coverage.
2. **`normalized/{market,trades}` retention (no longer minor).** 66.2 GB as of
   2026-07-04, growing ~3 GB/day, and still unmanaged: `archive-offload` is
   raw-only and `cleanup` only removes zero-byte parquet. This was the primary
   plant-side driver of the -56 GB G: burn 06-30..07-04. Same blind-spot shape
   as the Kalshi normalized tree that caused the 06-17 G:-full incident, just
   ~20x slower. Needs an offload/retention policy (code change; data-lifecycle
   -> owner sign-off on the policy, implementation is autonomous).
3. ~~Surface `stuck_unaccounted_count` in monitoring~~ **DONE — PR #28**
   (offload report persisted + growth-gated `health` finding; root-cause
   narrative in `docs/HISTORY.md` 2026-07-04). Merged ≠ deployed: activates at
   the next runner restart; audit with `--stuck-unaccounted-baseline 14211`
   until the cohort cleanup lands, then reset the baseline to 0.
4. **Phase 6 candidate — inverse (coin-margined) BTCUSD perps.** Natural next
   instrument-expansion step after the linear-perp triangle. Note: Binance USDT-M
   *websocket* is jurisdiction-blocked from this box (REST works — see Constraints),
   so plan venue choice accordingly (Bybit/OKX inverse WS, or Binance dapi REST
   mirroring the fapi REST lanes).
5. **OKX funding channel.** Deferred from Phase 5. Would mirror the
   `binance-futures-rest-funding` lane (`funding-rate` channel or REST poll) so both
   perp venues carry funding context.
6. **MEXC depth → provable `sequence` upgrade.** The pushed `version` is already
   captured as `metadata.mexc_version`; if live frames prove it dense per symbol,
   upgrade the lane the way Bybit depth was upgraded (`data.u` +1). Until then depth
   stays `none_native`.
7. **Re-promote pre-fix Binance depth history (optional).** Binance depth partitions
   collected before commit `084f8c9` (2026-06-09) lack the leading synthesized
   `snapshot` row, so self-contained replay of those dates needs re-promotion from
   raw. Only matters if historical self-contained replay is wanted.
8. **Kraken checksum precision table for non-BTC/USD pairs.** `_KRAKEN_BOOK_PRECISION`
   covers BTC/USD only; other pairs fall back to `none_native`. Moot until a non-BTC
   Kraken pair is actually collected; could auto-fetch from REST `AssetPairs`.
9. **Day-bounded rotation as the default run model.** `--rotate-at-midnight` exists
   and works; the live model is 30-min wall-clock segments (`max_segment_seconds=1800`).
   Parked — analysts pull by `event_date` partition, so per-run boundaries rarely matter.
10. **fapi REST 429 handling — honor Retry-After / pace cold-start bursts.** Audit
   finding (real, deferred as a design change): a 429 currently crashes the segment
   (self-heals by restart, seen once at boot 2026-06-09) and a seeded resume fires
   up to 5 unpaced catch-up pages; repeated 429s risk escalation to a fapi 418 IP
   ban. Add Retry-After-aware backoff in `_get_json` / pacing between catch-up pages.
11. **Zero-gap segment rotation.** The ~5–8s WS reconnect between segments costs
   ~0.3–0.4% per segment. Eliminating it means separating connection lifecycle from
   file lifecycle in the collector core — a real refactor, parked unless that loss
   starts to matter.
12. **Ops-root JSONL log rotation/retention.** `job_runs.jsonl`,
    `heartbeat_history.jsonl`, and `worker_events.jsonl` grow unbounded (~3–5k
    rows/day). The 2026-06-12 audit made health tail-read the run log (cost
    contained), but the files themselves still need a rotation or retention policy
    — fold into `run_cleanup`.
13. **Verify OKX/Bybit trades subscribe-replay behavior over live frames.** The
    audit fixed subscribe-time print replays for Kraken (`snapshot` frame) and
    Coinbase (`last_match`); review suggested OKX may push the latest historical
    trade on subscribe and Bybit's first `publicTrade` push may carry recent
    trades. Both lanes are `none_native` with run-keyed promotion, so untagged
    replays would accumulate small duplicate counts at every reconnect. Capture a
    few live (re)subscribes for each, check whether the first data frame
    re-delivers pre-subscription prints, and if so tag them `subscribe_replay`
    like Kraken/Coinbase.
14. **Local-only modelling raw lanes are unconfigured in `archive-offload`.**
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
- **14,211 `stuck_unaccounted_runs` (~95 GB hot on G:) — re-measured 2026-07-04;
  the previously documented "16" was a stale 06-22 measurement.** Never-scored raw
  run-dirs that aged past the 168 h scoring+quarantine window and are now permanent
  orphans: promote needs `replayable`, quarantine needs a fresh
  `replay_summary.json` within 168 h, and neither job will ever touch them again ->
  offload refuses to move them (by design) and reports them as
  `stuck_unaccounted_runs` forever. The cohort spans 06-09..06-23 and is dominated
  by **crash-loop debris** from the 06-17 ENOSPC incident and the 06-21..22
  robocopy-contention window (one run-dir per collector restart; binance_depth
  alone has 1,266 dirs dated 06-22). Heaviest lanes: binance_perp_trades ~32 GB,
  binance_perp_depth ~28 GB, binance_depth/binance_depth_usdc 1,602 dirs each.
  NOT an active bug: the population is closed (0 new orphans post-06-24; verified
  per-day) and curated coverage through the window held 34-49 promoted
  segments/day (intraday holes of order hours on 06-17/06-21/06-22 only). Latent
  gap: nothing back-stops runs that age out unscored, so a future multi-day
  incident will mint new orphans (observability half DONE — PR #28: persisted
  offload report + growth-gated health finding, active at next runner restart;
  run health with `--stuck-unaccounted-baseline 14211` until this cohort is
  cleaned up, then reset to 0). Options:
  (a) one-time cleanup -- quarantine the cohort with diagnostics bundles
  (preserves them, clears the warn-noise, lets offload move ~95 GB to D:) or
  delete (mostly interrupted partial captures); (b) build a durable backstop job
  that quarantines aged unaccounted runs (code change, but changes data
  accounting -> owner sign-off); (c) leave as-is and accept the warn-noise +
  ~95 GB permanently stranded hot. All three touch data retention / accounting =
  owner-gated. Note (a)-delete would discard partial raw that could in principle
  be backfill-scored to patch the 06-17/06-21/06-22 intraday curated holes --
  if those hours matter for research, choose quarantine-preserve over delete.

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
