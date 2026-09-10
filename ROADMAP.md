# Roadmap

**This file is the single source of truth for plans and open work.** It is
maintained by the project manager (Claude) and updated in every session that
changes scope or state. Companion docs:

- [`README.md`](README.md) — what the plant collects *today* (capability snapshot)
- [`STANDARDS.md`](STANDARDS.md) — the data contract (schemas, replayability, retention)
- [`docs/HISTORY.md`](docs/HISTORY.md) — resolved-work narrative (what was fixed, and why)

Last updated: **2026-09-07**.

> **Operating mode — safe shaping (owner directive, 2026-07-04).** No extended
> building on Claude's initiative: no new venues, lanes, or instruments, no big
> refactors, no new subsystems. Active work is plant health and audits,
> observability, retention and hygiene, small low-risk fixes, and clean
> documentation. Expansion items below are tagged **PARKED** and need an
> explicit owner ask to start.

---


## Finding — Binance fstream delivers no data from this host (2026-08-25)

fstream.binance.com accepts websocket connections and acks SUBSCRIBE, then
delivers no data frames at all: `btcusdt@aggTrade` + `!forceOrder@arr` on one
socket for 90 s produced zero frames, and ~24 h on the liquidation stream alone
produced zero. **A subscribe-ack is not evidence a Binance stream works from
here.** This is the same jurisdiction block that motivated the REST perp lane,
now measured at the data layer. Consequence: with REST `allForceOrders`
discontinued, Binance liquidations are currently uncollectable from this host;
the `binance-liquidations-worker` lane is correct code but ships disabled.
Bybit and OKX liquidation lanes are unaffected (both live-verified).

## Finding — Binance OI history backfilled from Vision daily zips (2026-08-27)

The open-interest lane's premise (PR #47: Binance API serves ~30 days of OI
history, older days are permanent loss) has a lossless complement:
`data.binance.vision/data/futures/um/daily/metrics/<SYMBOL>/` publishes daily
zips of 5-minute OI + long/short-ratio snapshots (BTCUSDT from 2020-09-01;
ETHUSDT/SOLUSDT from 2021-12-01). Owner-approved backfill landed 2026-08-27 at
`G:\03-reference-data\binance_futures_metrics\` — **outside the plant tree**,
~130 MB: raw zips (size-verified, archive of record) plus per-symbol zstd
parquet, gapless through 2026-08-26. Re-running its `backfill_metrics.py`
extends coverage (Vision trails ~1 day); the live lane remains the only
sub-daily-latency OI source. Source quirk recorded there: 2020-09→2021-05 zips
duplicate every row byte-identically; parquet layer drops exact dupes only.
This closes the "backfill OI metrics (BTC/ETH/SOL 2020→now) or defer?"
question orphaned when its RC session died on 2026-08-26.

## Open item — offload drain rate is the SSD bottleneck (noted 2026-08-24)

**Owner directive: G: (ADATA SSD) stays the live write buffer.** Collection
lanes need fast writes, so the fix for a filling SSD is to drain it faster, not
to move live roots onto bulk disk. This item is about drain rate only.

**Measured 2026-08-24:** G: at 97%, 76 GB free, having dropped ~18 GB in a day.
`archive-offload-cold` reports `eligible_count: 200` against `limit: 200` -
i.e. it is hitting its per-invocation cap every run, so the true backlog is
unknown and the drain is throttled rather than keeping up.

Levers, cheapest first:

1. **`min_age_days: 10` -> 3-5.** The dominant lever. Raw runs sit on the SSD
   for ten days before becoming eligible, but promotion to curated happens
   within minutes; the ten days only buys a replay-verification window. Halving
   it roughly halves steady-state SSD occupancy.
2. **`limit: 200` -> higher** (or shorten `interval_seconds` from 3600). While
   eligible == limit the queue is capped, so raising this is what actually
   converts eligibility into moved bytes. Raise and re-read `moved_count` /
   `moved_bytes` in the report rather than assuming.
3. **`cleanup` `raw_days: 14`** trails offload; it can come down once (1) lands.

Verify with `moved_bytes` in `ops/offload_report_latest.json`, not by eyeballing
free space - other things write to G: too.

**Separately (not an offload matter):** `G:-reference-data\hyperliquid_node`
is ~147 GB of STATIC research corpus on the SSD. It is not a write buffer, is
outside the plant tree, and is invisible to `archive-offload`. K: has ~4 TB free.
Moving it is the single biggest one-off reclaim available and does not conflict
with the SSD-as-buffer directive.

**Blocking risk:** at the current rate G: fills within days, and a full SSD
stops the plant writing - including the two new liquidation lanes.


## Current state (2026-08-17)

**23 enabled collector lanes** across Binance (spot USDT + USDC, USDT-M perp via
REST), Coinbase, Kraken, Bybit (spot + linear perp), MEXC, OKX (spot + linear
perp), Hyperliquid (frozen-wallet BTC/ETH/SOL perp fills), plus the RSS text lane.
All other market lanes remain BTC. **Kalshi
crypto-binary collection is TURNED OFF as of
2026-06-17** (both Kalshi jobs `enabled:false`; it was the G:-full root cause —
see `docs/HISTORY.md` 2026-06-17 + Decision queue). Full quarantine → promote
curation chain per lane, hourly score catch-up self-heal, research manifest,
cleanup retention, and cold-tier archive offload. The main live runner remains a
SYSTEM task using its startup copy of `ops.live.local.json`. **Hyperliquid: the
08-10 → 08-17 gap is fully recovered and curated** (2026-08-17, owner-directed):
after PR #42 merged, a bounded user-level catch-up run (60 s segments, 5 s poll
interval, live roots) refetched 10,663 backlog fills with zero poll errors —
exercising the new capped-response paging live — and dropped to the ~6-fill
live trickle, i.e. fully caught up to 13:33Z. The 08-10 torn run's 3,290
already-collected rows plus both 08-09 bridge runs were rescued by re-scoring,
and all 7 lane runs promoted: **16,874 rows now in
`curated/research/trades_replayable/source=hyperliquid`** (BTC/ETH/SOL), 0
failures. Scoring needed a lane-correct verdict — the generic stream scorer's
GLOBAL timestamp monotonicity fails any multi-wallet catch-up run (wallets sit
at different window depths), which post-restart would have sent the lane into a
quarantine → refetch loop. `replay_wallet_flow_run` (per-wallet ordering,
resume-window skew default, STANDARDS **v9**) is on
`codex/wallet-flow-scorer-v9` awaiting review/merge; the live score job now
selects it via `wallet_flow: true`. **Continuous collection still resumes only
at the elevated restart or reboot** (the SYSTEM runner runs its pre-08-09
startup config; on-disk config + concurrency bump are ready — and the restart
is no longer data-urgent, the gap being closed as of today). The 2026-08-01
preserve-first aged-run backstop is **merged** (PR #41, `b312657`) but awaits
the same restart.

---

## Dated operational checks

| Due | Check |
| --- | --- |
| ~~2026-06-18~~ DONE 06-22 | Offload-index spot-check **PASSED**: 4509 index rows == cold run-dirs 1:1 on every lane, 0 duplicates/malformed, 0 unindexed pile-up, 0 missing cold copies, 0 sampled file-count mismatches, 0 indexed runs still hot. Offload live (newest `moved_at` 2026-06-22T09:53Z). Dry-run also flags **16 `stuck_unaccounted_runs`** (raw from 06-09..06-11 never promoted: 8 `binance_perp_funding` + 8 trade/depth) — designed safety surface, but a real promotion gap to investigate. *(Re-measured 2026-07-04: the true cohort is **14,211** — the 06-16..06-23 crash-loop debris had not yet crossed the 10-day offload fence when this check ran. See the 07-04 audit stamp + Decision queue.)* |
| ~~2026-07-15~~ DONE 07-16 | **The 07-05 orphan wave crossed the offload fence as predicted**: `stuck_unaccounted=17,519` (forecast ~17.5k), `failed=0`. Health's sole finding is the expected `offload_stuck_above_baseline:17519`; the queued cleanup/backstop decision remains open. |
| ~~2026-06-19~~ DONE 06-24 | The 06-17 `robocopy /MINAGE:3` move never finished (~88% of partitions still on G:), leaving G: at **3.9 GB free**. First retry (06-22) was killed by the Bash tool's 10-min timeout after freeing ~57 GB. Relaunched **detached via `Start-Process`** (pid 48444) so it survives session/tool teardown -> **COMPLETED 2026-06-24 16:19, FAILED: 0** (45.29 M files / 555 GB moved G:->`D:\market_archive_cold`). **G: now 489 GB free.** D: holds 113,407 normalized partitions (full set). 1 partition / 2 parquet files remain on G: -- robocopy *skipped* them (already byte-present on D: from the 06-17 partial), so redundant not stranded; immaterial (489 GB free). Lesson: long-running moves must be detached, never run inside a Bash call (10-min cap). |
| ~~2026-07-26~~ DONE 08-01 | **Text raw offload wired for the next restart.** `archive-offload-text` is enabled in `ops.live.local.json` with the indexed promotion/quarantine gate, preserve-first aged-run backstop, byte-verified cold move, and `write_report:false` so it cannot replace the market health report. It remains inert until the guarded elevated runner restart. |

**Last ops audit:** 2026-09-07 — **every lane capturing and fresh; health
`status=error` is entirely the two 09-02 defects still waiting on PR #54 + a
redeploy; one NEW dataset-wide finding: roughly a third of curated runs are
truncated because the hourly scorers score the live segment.** Runner pid 31808
up since 2026-09-02 16:03Z (100 jobs / 32 pooled, heartbeat 4 s). 45,490 job
results since the restart: 45,230 success / 260 error (99.43 %). Newest raw run
≤ 28 min on all 33 hot lanes; promotions within minutes on all 22 indexed
lanes; options lanes on cadence (470 Binance chain / 1,396 Deribit snapshots
since the restart); leaderboard daily 5/5. Quarantine intake 7 d: kraken 24 and
coinbase 14 `trade_id_gaps` (4–7 % of runs), binance ≤ 2, text_rss 142
`no_events` (by design). G: 172 GB free (−22 GB since 09-02), D: 908 GB; hot
raw 131 GB, ops root 7 GB. Offload 13:38Z pass: 66 runs / 2.07 GB, 0 failed,
eligible == moved (the 1000 cap no longer binds); `stuck_unaccounted=3`
(funding 08-30 + 09-01, perp-depth 08-30 restart partials; the 10-day backstop
will classify them). `main a35deac`: 556 tests pass, ruff clean. **Findings:**
(1) **NEW — truncated promotions on every scored lane, since at least June.**
The hourly `backfill-*` score jobs (`max_age_hours: 6`, no minimum age) land
every ~77 min on the single maintenance slot and score whichever 1800 s segment
is still being written; the 5-min promoter then indexes the partial rows, and
the collector's own close-of-segment summary is never re-read because the
promotion index already holds the run. Measured as promoted_rows vs raw
`clean/` rows: since 09-03, 58–71 of ~190 promoted runs per trades lane carry
< 98 % of their rows (e.g. `binance_perp_trades/20260903_134004`: 19,461 of
61,886); depth lanes 38–43 of ~124 since 09-05; `binance_trades` sampled by
promotion day: 06-20 8/44, 07-10 17/49, 07-25 12/47, 08-10 12/44, 08-25 5/66,
09-01 15/51; `hyperliquid_wallet_flow` over its whole life 225 of 950 runs
short = **7.5 % of fills absent from curated** (227,086 promoted of 245,387
clean). Funding (inline summary, no hourly scorer) and text (`min_age_hours: 1`)
are unaffected. Raw is intact on hot + cold, so this is repairable. PR #54 adds
`min_age_hours` (default 1 h) to `backfill-trades-replay` and text only —
`backfill-replay` (Binance depth ×2) and `backfill-stream-depth` (8 depth
lanes) still have no minimum age (commented on the PR). Repair of the
historical rows touches curated data → Decision queue. (2) **The shared-run-dir
collision is still corrupting raw daily.** Since the restart
`binance_perp_open_interest/` has 220 mixed-symbol runs of 374, 41 torn
`clean/events.jsonl` lines and 20 worker crashes (JSON decode of a
half-written line by another lane's replayer, plus `PermissionError` races on
the shared `replay_summary.json`) — all three lanes die in the same second
(09-02 20:36:07, 09-04 21:36:48). `bybit_perp_liquidations/`: 56 mixed of 70,
11 torn lines. The fix (`source_suffix`) is on disk in `ops.live.local.json`
(undeployed) and in PR #54. (3) **Liquidation lanes:** 230 of the 260 errors
are the 7200 s kills (every run of all four lanes); 69/70 Bybit and 55/60 OKX
runs have no replay summary; data is on disk. The 25 h rule is code in PR #54
and the local config carries no explicit pin, so a redeploy on `main` alone
leaves this unfixed. (4) **Config drift / ordering hazard.**
`ops.live.local.json` (edited 09-02 22:35Z, 115 jobs / 112 enabled) is ahead
of the running config (100 jobs) and of `main`: its 12 OI curation jobs and
`min_age_hours` args need PR #54's code (`main` silently drops `min_age_hours`
for trades scorers; health flags the 12 jobs `stale_job`). `source_suffix` is
supported on `main`. **Merge #54 first, then redeploy** — the reverse order
deploys the OI curation chain with live-segment scoring and no liquidation
timeout fix. (5) Transient, self-healed: 4 fapi transport errors (SSL EOF /
WinError 10054), 6 Deribit 503s 09-05 09:02–09:35Z. (6) Hygiene: archived the
two orphaned heartbeat files (`binance-liquidations-worker`,
`bybit-liquidations-worker`) via `ops-prune-stale-workers --apply` →
`ops/archived_standalone_workers/20260907_144114` (clears the four
`unmanaged_*` findings). **The test suite writes to the live archive:**
`tests/test_ops.py` runs the real runner with `mock` jobs and no
`output_root`, so every `pytest` run adds run dirs under `raw/market/mock/`
(56 now, two from this audit's own test run) — the source of the standing
`unconfigured_lane:mock` warning (open item 16). `heartbeat_history.jsonl` is
**5.8 GB** (27 KB per row, ~80 MB/day; open item 12).
`scripts/redeploy_runner.ps1` starts the runner without redirecting
stdout/stderr, so `runner.log` has not been written since the 08-25 boot-task
start and a runner-process crash would leave no trace (open item 17). V1 tasks
`BinanceIV Collect History` / `Collect Deribit` still run (cutover step 4,
owner).

**Deployed 2026-09-08 00:29Z** (owner, elevated `redeploy_runner.ps1` on
`main 50f44d7` = PRs #54/#55/#56 merged 09-07 17:03Z), **verified 09:30Z (9 h):**
runner pid 35660, 112 jobs / 31 pooled, health `status=warn` with the single
finding `offload_stuck_above_baseline:4`. 3,861 job results, 3,859 success /
2 error (both fapi transport exits, self-healed). **Every 09-02 and 09-07
defect that the redeploy could fix is fixed live:** (a) the four liquidation
lanes have run 9.02 h continuously — no `timed out after 7200s` since the
restart; (b) the OI lanes write to `binance_perp_open_interest_{btcusdt,ethusdt,solusdt}/`
(18 runs each, single product, 0 torn lines, 17/17 successes, 0 errors); the
legacy shared dir and `bybit_perp_liquidations/` stopped receiving runs at
00:23Z / 00:10Z; the Bybit lanes write to `bybit_perp_liquidations_<symbol>/`;
(c) the OI curated dataset exists: `curated/research/open_interest` holds 408
promotions / 12,841 rows (357 legacy-dir runs + 17 per symbol dir), 321
legacy runs quarantined (the mixed-symbol / torn ones, by design), the 12 OI
curation jobs run on cadence; (d) **trades-lane truncation has stopped**: 0 of
73 runs promoted after 01:30Z on `binance_trades`, `bybit_perp_trades`,
`okx_trades`, `hyperliquid_wallet_flow` are short. **Depth lanes still truncate
as predicted** (7/15 `binance_depth`, 6/16 `coinbase_depth`, 6/16 `okx_depth`
since 01:30Z) — the two depth scorers have no minimum age; autonomous fix
queued (open item 18). Owner also **disabled the V1 options tasks** — cutover
step (4) done, plant lanes are the sole options collectors. Residue: the 4
stuck runs are 3 restart partials (08-30 ×2, 09-01) plus the torn OI run
`20260902_203608`; the three new `bybit_perp_liquidations_<symbol>` dirs (and
`okx_perp_liquidations`) have no offload row → Decision queue; `runner.log`
still not written (item 17). D: free fell 908 → 830 GB in a day from
`D:\open-weights` (owner activity, not the plant).

**Previous ops audit:** 2026-09-02 — **runner healthy, options-IV cutover live; the
three lanes added 2026-08-25 have been defective since deploy day.** SYSTEM
runner redeployed by the owner 2026-09-01 ~12:32Z (PR #51 merged 12:30Z), so
merged == deployed for the first time since 08-09; heartbeat fresh, 100 enabled
jobs, 29–31 pooled. Every trade/depth/funding/wallet-flow/text lane and every
promote/quarantine/score/offload job: 0 errors since the restart, newest raw
run ≤ 30 min on all hot lanes. Options-IV lanes on cadence with 0 errors (96
Binance chain + 287 Deribit snapshots in 24 h); V1 tasks still writing in
parallel, so runbook step (3) is satisfied — step (4) is the owner's. G: 194 GB
free (up from 76 GB on 08-24: the offload levers worked), D: 1.05 TB. Offload
12:00Z pass: 54 runs / 0.99 GB moved, 0 failed, 1 stuck. **Findings, all fixed
on `fix/lane-worker-names-oi-replay` (config changes need the redeploy):**
(1) **Lock-collision crash loop since 2026-08-25 10:44Z** — the three
open-interest lanes reused `worker_name: binance-futures-rest-funding` and the
three Bybit liquidation lanes all used `bybit-liquidations-worker`; the
standalone-worker lock is keyed by that name, so one lane per group ran and the
rest failed every 5 s with "standalone worker already active" — 39,665 of
48,598 job results in the last 24 h, 308k since 08-25, and it survived the
09-01 redeploy because every job *name* was unique. Fix: unique worker names in
both configs, loader now rejects shared names, hygiene test pins it. (2)
**Open-interest runs were never replayable** — all 245 scored runs carry
`invalid_mark_price` because the funding replayer required a positive `price`
and OI carries `price: null` by contract (value in `size`). Fix: replayer
scores `open_interest` rows on `size` (finite, ≥ 0; `invalid_open_interest`
finding, `price` must stay None), **STANDARDS v10**, `backfill-trades-replay
--funding` added so the mis-scored runs can be re-issued; tests pin it.
Curation chain shipped as STANDARDS v11 (PR #54; see `docs/HISTORY.md` 2026-09-02). (3) **`hyperliquid-leaderboard-snapshot`
never ran in the runner** — its live-config `output_root` had a single
backslash before `raw` (= a carriage return), so mkdir failed with WinError 123
on both attempts. Fix: path corrected in the live config, loader rejects
control characters, hygiene test pins it. The interim per-user daily task has
17/17 snapshots since 08-17, so there is no capture gap. Smaller: Bybit/OKX
liquidation workers are killed by the runner's 7200 s subprocess timeout every
two hours (126/133 Bybit runs have no replay summary — data on disk, never
scored); the disabled `binance_perp_liquidations` lane left 4,460 near-empty hot
run dirs (now `age_only` offload); `bybit_perp_liquidations`,
`okx_perp_liquidations`, `hyperliquid_leaderboard`, `hyperliquid_retro_fills`
and the limitless lanes remain `unconfigured_lane` for offload.

**Deployed 2026-09-02 16:03Z** (owner, elevated `redeploy_runner.ps1` on `107a235`):
runner up with 100 jobs / 31 pooled; all six previously colliding lanes hold their
own lock (`binance-futures-rest-open-interest-{btc,eth,sol}`,
`bybit-liquidations-worker-{btc,eth,sol}`) with fresh heartbeats, 0 errors since
the restart (the last `standalone worker already active` row is 16:02:44Z,
pre-restart). `hyperliquid-leaderboard-snapshot` succeeded on its first runner
run (44,554 rows, `parse_ok`), so the interim `HyperliquidLeaderboardDaily` user
task was disabled (not deleted) the same hour. Options-IV lanes resumed on
cadence. Same-day owner decisions (OI curated dataset v11, liquidation lane
timeout rule) shipped in PR #54 — see `docs/HISTORY.md` 2026-09-02; V1 task
disable stays queued ("not yet").

**Verified 2026-09-02 20:06Z (4 h after the restart):** `binance-{btc,eth,sol}-open-interest`
and `binance-futures-rest-funding` 8 successes each, 0 errors (1800 s segments);
1,529 success / 8 error job results since 16:03Z (pre-restart 24 h: 9,062 /
38,563), 0 `standalone worker already active` rows, six new per-lane locks
present. All 8 errors are the known 7200 s subprocess kills of
`bybit-{btc,eth,sol}-liquidations` + `okx-swap-liquidations` (18:03Z, 20:03Z): the
runner read its config at 16:03Z, before `subprocess_timeout_seconds: 90000`
landed on disk (PR #54, 16:19Z), so that fix is not deployed yet.
`hyperliquid-leaderboard-snapshot` next run 2026-09-03 16:03Z;
`HyperliquidLeaderboardDaily` confirmed Disabled. **New finding — same-second
run-dir collision:** run dirs are `<source>/<YYYYMMDD_HHMMSS>` with no symbol
component (`prepare_run_paths`), and the three OI lanes (shared
`binance_perp_open_interest/`, synchronized 1800 s segments) and the three Bybit
liquidation lanes (shared `bybit_perp_liquidations/`) start their segments within
the same second, so they append into one run dir: 13 run dirs for 21 OI segments,
9 of the 12 closed OI runs hold 2-3 symbols interleaved and score
`non_monotonic_event_time` (non-replayable); only the 3 single-symbol runs are
replayable; one torn `clean/events.jsonl` line from the concurrent appends. Both
post-restart Bybit runs are mixed (31 of 135 historical Bybit runs already were).
Not fixed here — see Decision queue. Note: the shared checkout on the box has
been on `feat/open-interest-curation-v11` (PR #54, unmerged) since 16:19Z, so
`run-job` child processes import that code while the runner itself is 107a235.

**Verified 2026-09-02 20:06Z (4 h after the restart):** `binance-{btc,eth,sol}-open-interest`
and `binance-futures-rest-funding` 8 successes each, 0 errors (1800 s segments);
1,529 success / 8 error job results since 16:03Z (pre-restart 24 h: 9,062 /
38,563), 0 `standalone worker already active` rows, six new per-lane locks
present. All 8 errors are the known 7200 s subprocess kills of
`bybit-{btc,eth,sol}-liquidations` + `okx-swap-liquidations` (18:03Z, 20:03Z): the
runner read its config at 16:03Z, before `subprocess_timeout_seconds: 90000`
landed on disk (PR #54, 16:19Z), so that fix is not deployed yet.
`hyperliquid-leaderboard-snapshot` next run 2026-09-03 16:03Z;
`HyperliquidLeaderboardDaily` confirmed Disabled. **New finding — same-second
run-dir collision:** run dirs are `<source>/<YYYYMMDD_HHMMSS>` with no symbol
component (`prepare_run_paths`), and the three OI lanes (shared
`binance_perp_open_interest/`, synchronized 1800 s segments) and the three Bybit
liquidation lanes (shared `bybit_perp_liquidations/`) start their segments within
the same second, so they append into one run dir: 13 run dirs for 21 OI segments,
9 of the 12 closed OI runs hold 2-3 symbols interleaved and score
`non_monotonic_event_time` (non-replayable); only the 3 single-symbol runs are
replayable; one torn `clean/events.jsonl` line from the concurrent appends. Both
post-restart Bybit runs are mixed (31 of 135 historical Bybit runs already were).
Not fixed here — see Decision queue. Note: the shared checkout on the box has
been on `feat/open-interest-curation-v11` (PR #54, unmerged) since 16:19Z, so
`run-job` child processes import that code while the runner itself is 107a235.

**Previous ops audit:** 2026-08-17 — **market/text capture healthy; Hyperliquid
stalled awaiting the elevated restart.** Manual markers only (the health command
was not re-attempted after the 08-09 timeouts). SYSTEM-runner heartbeat fresh
(status `running`, 23 pooled slots / 22 distinct jobs); job results since 08-09:
38,233 success / 43 error (99.89%) — every error a transient collector network
timeout or venue disconnect with clean restart, most on the Binance REST perp
and depth lanes. All 22 SYSTEM-runner lanes fresh (newest raw run ≤ 29 min).
G: 379 GB free, D: 1.8 TB free. Latest offload pass (2026-08-17T12:34Z,
`mode:apply`): 69 runs / 2.3 GB moved byte-verified to cold, 0 failed, 0
stuck-unaccounted, backstop idle (0 candidates). Quarantine intake last 7 days
is low (2–13 runs/lane) except the Binance perp REST lanes (46–63/lane),
consistent with their known REST-timeout profile. Findings: (1) **the
Hyperliquid bridge is stopped** — newest run 2026-08-10, ~7.5 days dark; see
Current state (prospective-only gap, resumes at the elevated restart). (2) The
offload report `status:warn` is solely `unconfigured_lane` findings; two of the
seven, `limitless_books` and `limitless_series_registry`, are **actively
writing right now** (newest runs 1–4 min old) from outside the ops runner with
no offload/retention coverage on G:, while three sibling limitless lanes are
~66 days stale — owner call needed on retention/offload config for these (see
Decision queue context in the limitless docs). (3) Hygiene: the unconfigured
`mock` lane holds 98 hot run dirs; `coinbase_*_usdc` and `kalshi_crypto_quotes`
lane dirs are empty leftovers.

**Post-audit review of PR #42 (2026-08-17, same session):** the pre-merge review
found three lane defects that would have fired at the pending elevated restart;
all fixed on the PR branch + local config: (a) a cap-sized `userFillsByTime`
response permanently stalled that wallet — the poller raised and never advanced
the high-water, so the window re-capped identically forever; it now pages
forward to the last complete timestamp and marks the poll incomplete. (b) The
lane's 300 s delay/clock-skew gates would have quarantined the entire
post-outage catch-up at capture — and since quarantined rows never reach clean,
the durable scan couldn't suppress them and every segment refetched and
re-quarantined the same growing window (the bfr lesson at 60 s scale). Gates
are now resume-window-sized (90 days) in the code defaults and in
`ops.live.local.json` (backup: `ops.live.local.json.bak-20260817-hyperliquid-gates`).
(c) The `score-hyperliquid-wallet-flow` window (`max_age_hours: 6`) could never
score a run torn by an outage longer than 6 h, stranding its rows out of curated
while the durable scan suppresses their refetch; score/promote/quarantine
windows widened to 336 h in the local config. **Timing note for the owner:** the
08-10 torn run `20260810_000001` (3,290 clean rows, no replay summary) is healed
automatically by the widened score job only if the elevated restart happens
before ~2026-08-24; after that, rescue it manually with
`backfill-trades-replay --stream` + a promote pass using a wide `--max-age-hours`.

**Previous ops audit:** 2026-08-09 — **existing capture markers fresh; Hyperliquid
prospective collection live.** The official health command exceeded both bounded
30 s and 60 s attempts, so no clean overall verdict is claimed from it. Manual
markers showed a fresh SYSTEM-runner heartbeat, 23 current jobs, success as the
latest counter-tracked status, about 376.6 GB free on G: and 1.95 TB free on D:;
the latest offload report had zero failed moves and zero stuck-unaccounted runs.
The owner then approved the frozen 10-wallet Hyperliquid lane. A scratch probe
proved capped-response handling and cross-run dedup; the native lane passed the
full suite. Guarded redeploy stopped safely at the non-elevated SYSTEM boundary.
A hidden user-level bridge is therefore collecting from the final prospective
boundary `2026-08-09T00:45:48Z`; its heartbeat and all ten wallet polls are fresh,
with zero poll errors. The live config and concurrency change will be adopted by
the SYSTEM runner at the next elevated deploy/reboot.

**Earlier ops audit:** 2026-08-01 — **capture healthy; aged-run accounting repaired
without raw deletion.** All 22 collectors were fresh, heartbeat ~14 s, and G:
had 384.45 GB free. The 17,809 aged unaccounted run directories were classified
`aged_unaccounted` with bounded diagnostics and quarantine index rows: 17,809
successes, 0 failures. Raw remained in place until the existing byte-verified
cold move; the latest official offload report shows `stuck_unaccounted=0`,
`failed=0`, and the audit baseline is reset to 0. The audit also exposed a real
single-slot scheduling fairness bug: three RSS maintenance jobs near the end of
the live config were stale while the RSS collector stayed fresh. The branch fix
selects the oldest due maintenance deadline and allows one normal 51-53 minute
manifest pass before declaring a queued job stale. Until the elevated restart,
health remains WARN only for those three stale RSS maintenance jobs; collection
itself remains live.

**Earlier ops audit:** 2026-07-16 — **plant GREEN before text deployment; RSS
initial verification GREEN.** Pre-deploy health's sole finding was the expected
`offload_stuck_above_baseline:17519`, exactly the forecast 07-05 crash cohort;
heartbeat 2.3 s, all 81 enabled scheduled jobs' latest rows successful, all 21
collector workers fresh, quarantine ratios 0 where reported, **G: 436.7 GB
free**, offload same-hour with 42 moves / 0 failures. PR #35 merged as
`496075b`; owner ran the guarded elevated redeploy at 2026-07-16 11:36 UTC and
the new runner confirmed healthy as `pid=27212`. RSS collector + scorer +
quarantine + promoter are enabled; the worker is fresh and the first segment
captured **121 clean rows across all five feeds** (25 CoinDesk, 30
Cointelegraph, 20 The Block, 36 Decrypt, 10 Bitcoin Magazine), with zero
duplicate keys, missing timestamps, or future timestamps. The initial
maintenance jobs are queued behind the startup research-manifest pass and will
clear their first-run health warnings as that slot turns over. Reddit remains
disabled pending the approved OAuth credential file.

**Same-day RSS acceptance checkpoint (16:35 UTC, ~5 h live) — adversarial
live audit, NO code defect found.** Full chain verified on live data:
10/10 closed segments scored `replayable:true` and promoted **exactly once**
(promotion index: 10 distinct runs, 0 re-promotions; curated 295 rows == index
sum; per-run parquet counts == `promoted_rows`; layout
`v2/source=rss/instrument=<feed>/event_date=<ingestion date>` with all envelope
+ provenance columns). The 11:30 pre-redeploy worker was hard-killed mid-segment
and **self-healed exactly as designed**: `backfill-text-replay` scored it at
13:07 (the 1 h `min_age` floor correctly deferred the 12:02 pass) and the next
promote pass promoted its 121 rows at 13:16 — no stranded orphan (the funding
lesson holds for text). The kill also exercised the at-least-once cursor
contract live: the first post-restart segment re-emitted the 121-key window
(curated carries exactly those 121 keys ×2 and **zero other duplicates**), and
the following segment emitted 1 row — the persisted `_cursors` seen-map dedups
across segments. `ingestion_ts` monotone in every run; `event_date` ==
ingestion date on all 295 rows; 0 quarantined events; 0 poll errors across
~675 conditional GETs; quarantine index empty (no quiet segment yet — that
path stays unverified until a zero-item window occurs); rotation exactly
30 min + ~5-6 s redispatch; steady state 1-6 rows/segment (probe-consistent).
Live churn validated the source-clock design: Decrypt re-served ~20 old items
in one poll (one claimed publish ts ~199 days old) — captured as new
sightings, flagged non-gating `stale_source_ts`, `ingestion_ts` stayed the
axis. Warnings (not defects): the missing live `archive-offload-text` job (see
the 2026-07-26 dated check) and a cosmetic one — text segment summaries always
report `deadline_reached=false` because the collector's own deadline ends the
stream before the pipeline's check; rotation itself is proven by the cadence.

**Earlier ops audit:** 2026-07-12 — **plant GREEN today; two self-healed network
incidents since 07-04; an orphan wave crosses the offload fence ~07-15.**
Health `status=warn` with exactly one finding,
`offload_stuck_above_baseline:14215` (heartbeat ~1.4 s) — **PR #28's growth
gate working as designed**: the stuck cohort grew **+4** (all
`binance_perp_funding` restart partials dated 06-27..07-02, ~500 KB total). So
the 07-04 "closed population" claim is wrong in the small: **the funding lane
mints one permanent orphan per worker restart** — its replay summary is written
inline only at clean segment close and it has no hourly catch-up scorer (trades
and depth do), which is also why funding is the largest historical stuck lane
(2,465). Box **rebooted 2026-07-11 ~15:50 UTC** -> runner now on `main`
`b3cf669`, so **PRs #26/#28/#31 all deployed**. Jobs since boot: **8,236/8,262
success (99.69%)**; all 26 errors were fapi transport failures (timeouts/SSL)
in the first ~35 min after boot + one 21:27 blip, all self-healed; **0 job
errors on 07-12**. Incidents: **(1) 07-05 ~11:00-17:00 UTC all-venue network
outage** — all 21 lanes churned (WS lanes ~42-46 worker restarts each; the 3
fapi REST lanes crash-looped at ~10 s cycles -> ~600 partial run-dirs each;
binance spot depth x2 ~404 each); promotions still held 42-45/lane that day, so
curated holes are intraday, order ~1-2 h. **(2) 07-11 07:00-15:50 UTC
fapi-reachability degradation** (REST lanes only: 177/66/218 errors; WS lanes
unaffected), ended by the reboot; funding coverage 32/48 that day; gapless
aggTrades self-backfilled (240 promotions) and the slow-cycle partials promoted
(perp depth 203) — so funding is the only lane with a material 07-11 hole
(~8 h thin), while every lane carries the small 07-05 intraday holes. All 21 lanes now fresh (every promotion index shows promotions within
minutes of this audit); quarantine ~0; **G: 468.8 GB free** (+36 vs 07-04);
offload live (index 23,347 rows, +563; newest move same-hour; 0 failures).
`normalized/{market,trades}` now **83.2 GB / 5.5 M files** (+17 GB in 8 d,
~2.1 GB/day) — open item 2 remains the main plant-side G: burn. **Inbound:
~3,417 unaccounted run-dirs dated 07-03+** (dominated by the 07-05 crash
cohort — fast-loop partials the 168 h catch-up scorers never scored, i.e.
unscorable debris) start crossing the 10-day offload fence 2026-07-13, bulk
~07-15 -> `stuck_unaccounted` steps from 14,215 to **~17.5k** and the health
warn grows daily until the queued cleanup/backstop decision lands (see the
updated decision-queue entry).

**Ritual:** if this stamp is more than ~3 days old at session start, audit the
live plant first — see `CLAUDE.md` "Quality gates".

(Previous audit 2026-07-04: green, 99.76% jobs, curated fresh on every lane;
its material finding was the stuck-cohort re-measure **14,211, not 16** — a
stale 06-22 count carried forward — plus the detection gap that health never
read offload reports, fixed same day as PR #28. Full narrative in
`docs/HISTORY.md` 2026-07-04. The resolved 2026-06-17 G:-full incident —
Kalshi normalized blind spot, ~1 h data loss, Kalshi turned off — lives in
`docs/HISTORY.md` 2026-06-17; its live remnants are the Kalshi-off state
above, the stuck-cohort + normalized-retention items below, and Kalshi raw
preserved on D:.)

---

## Open work items (rough value order)

**PARKED** items are extended building — not picked up without an explicit
owner ask (safe-shaping directive above).

0. **Adaptive wallet-cohort program (owner-directed 2026-08-17 — ACTIVE).**
   Owner directive: don't track only a fixed cohort; select wallets by an
   adaptive rule, backfill deep history for backtesting, and if the screen shows
   signal, move the selected cohort to a fast (WebSocket) feed. Methodology
   guard: adaptive selection + retrospective backfill is survivorship-biased by
   construction — the backtest is a *screen* only; the frozen 10-wallet lane
   (§4.7) stays as the clean prospective evidence, and honest point-in-time
   selection becomes possible only from leaderboard snapshots captured from
   2026-08-17 onward. Feasibility probes (2026-08-17): leaderboard = 41,903
   wallets with day/week/month/all-time PnL/ROI/volume (current state only);
   fill history depth is inversely proportional to activity — top-1000-by-month-
   volume sample: median 68 d lookback, only ~17 % reach 365 d.
   - **Phase 1a (built 2026-08-17, awaiting merge + elevated restart):**
     `hyperliquid-leaderboard-snapshot` raw-only reference lane (STANDARDS §4.8),
     daily job in the live config; interim per-user daily scheduled task
     `HyperliquidLeaderboardDaily` covers capture until the restart.
   - **Phase 1b (approved scale: top 1,000 by month volume):** one-shot
     retrospective fills backfill, each wallet as deep as the public API allows
     (~2–3 GB expected), written to `raw/market/hyperliquid_retro_fills/` as a
     clearly-tagged retrospective dataset via a local `artifacts/` script —
     NEVER merged into the prospective curated tree.
   - **Phase 2 (owner gate):** walk-forward screen — rule computed only from
     fills ≤ each rebalance date; results discounted for universe survivorship.
     Backtests live OUTSIDE this repo (publication contract: no model
     experiments); the plant only ships the datasets.
   - **Phase 3 (owner gate, PARKED until Phase 2 passes):** WebSocket
     `userFills` subscription lane for the adaptive cohort (sub-second delay vs
     ~37 s median REST polling).

1. **D:\market_archive legacy history — decide retention or merge.** The pre-2026-06-08
   D: archive is kept read-only as history. Decide: backfill/merge its runs into the
   G: curated dataset (score with `backfill-trades-replay` / `backfill-stream-depth
   --score-only`, then let the promote jobs pick them up) or declare it cold history
   and leave it. Blocks nothing, but the disjoint pre-cutover data limits historical
   research coverage.
2. ~~**`normalized/{market,trades}` retention (no longer minor).**~~ **DONE
   2026-09-10/11: layer retired (v13, `normalized_parquet: false` everywhere,
   live 15:06) and the 229.7 GB / 15.43 M-file tree verify-moved to
   `D:\market_archive_cold\normalized\`, FAILED 0; G: 131 -> 391 GB free. Details
   in the Decision queue entry.** Original item: 66.2 GB as of
   2026-07-04, growing ~3 GB/day, and still unmanaged: `archive-offload` is
   raw-only and `cleanup` only removes zero-byte parquet. This was the primary
   plant-side driver of the -56 GB G: burn 06-30..07-04. Same blind-spot shape
   as the Kalshi normalized tree that caused the 06-17 G:-full incident, just
   ~20x slower. Needs an offload/retention policy (code change; data-lifecycle
   -> owner sign-off on the policy, implementation is autonomous).
   *2026-09-10: measured 229 GB / 15.4 M files (market 197.6 GB, trades 31.7
   GB), ~2.4 GB/day, G: 136 GB free; no in-plant data consumer (two maintenance
   jobs only walk it for stats: manifest, cleanup). Proposal in the
   Decision queue: stop writing (config, 20 lanes) + verify-move the tree to
   the cold tier + retire section 2.2 in STANDARDS. All three done by 2026-09-11
   00:12.*
3. ~~Surface `stuck_unaccounted_count` in monitoring~~ **DONE — PR #28**
   (offload report persisted + growth-gated `health` finding; root-cause
   narrative in `docs/HISTORY.md` 2026-07-04). **Deployed at the 07-11 boot and
   verified live 07-12**: it caught the +4 cohort growth within a day. The
   07-05 wave crossed the offload fence as forecast (~17.5k). **Cleanup and the
   durable preserve-first backstop landed locally 2026-08-01:** all 17,809 runs
   were classified with 0 failures and the live offload report is at 0 stuck;
   audit with `--stuck-unaccounted-baseline 0` now.
4. **PARKED — Phase 6 candidate: inverse (coin-margined) BTCUSD perps.** Natural next
   instrument-expansion step after the linear-perp triangle. Note: Binance USDT-M
   *websocket* is jurisdiction-blocked from this box (REST works — see Constraints),
   so plan venue choice accordingly (Bybit/OKX inverse WS, or Binance dapi REST
   mirroring the fapi REST lanes).
5. **PARKED — OKX funding channel.** Deferred from Phase 5. Would mirror the
   `binance-futures-rest-funding` lane (`funding-rate` channel or REST poll) so both
   perp venues carry funding context.
6. **PARKED — MEXC depth → provable `sequence` upgrade.** The pushed `version` is already
   captured as `metadata.mexc_version`; if live frames prove it dense per symbol,
   upgrade the lane the way Bybit depth was upgraded (`data.u` +1). Until then depth
   stays `none_native`.
7. **PARKED (touches curated data — owner-gated anyway) — re-promote pre-fix
   Binance depth history.** Binance depth partitions
   collected before commit `084f8c9` (2026-06-09) lack the leading synthesized
   `snapshot` row, so self-contained replay of those dates needs re-promotion from
   raw. Only matters if historical self-contained replay is wanted.
8. **PARKED (moot until a non-BTC Kraken pair exists) — Kraken checksum precision
   table for non-BTC/USD pairs.** `_KRAKEN_BOOK_PRECISION`
   covers BTC/USD only; other pairs fall back to `none_native`. Moot until a non-BTC
   Kraken pair is actually collected; could auto-fetch from REST `AssetPairs`.
9. **PARKED — day-bounded rotation as the default run model.** `--rotate-at-midnight` exists
   and works; the live model is 30-min wall-clock segments (`max_segment_seconds=1800`).
   Parked — analysts pull by `event_date` partition, so per-run boundaries rarely matter.
10. ~~fapi REST 429 handling — honor Retry-After / pace cold-start bursts~~
    **DONE — PR #31.** The default fetch path now honors `Retry-After` on 429
    (bounded: 3 attempts, 2 s default / 60 s cap; a 418 IP-ban raises
    immediately, never retried) and seeded aggTrades catch-up polls pace 0.25 s
    between pages (first page of every poll stays immediate — steady state
    unchanged). **Deployed at the 07-11 boot.**
11. **PARKED (real refactor) — zero-gap segment rotation.** The ~5–8s WS reconnect between segments costs
   ~0.3–0.4% per segment. Eliminating it means separating connection lifecycle from
   file lifecycle in the collector core — a real refactor, parked unless that loss
   starts to matter.
12. **Ops-root JSONL log rotation/retention.** `job_runs.jsonl`,
    `heartbeat_history.jsonl`, and `worker_events.jsonl` grow unbounded (~3–5k
    rows/day). The 2026-06-12 audit made health tail-read the run log (cost
    contained), but the files themselves still need a rotation or retention policy
    — fold into `run_cleanup`. *Measured 2026-09-07: `heartbeat_history.jsonl`
    is 5.8 GB (each row embeds the full 100-job counter map, ~27 KB, ~2,900
    rows/day = ~80 MB/day on the SSD), `job_runs.jsonl` 554 MB, `worker_events`
    150 MB — the ops root is 7 GB. No longer minor; autonomous fix.*
    *2026-09-08: `heartbeat_history.jsonl` rotation on
    `fix/heartbeat-history-rotation` — the history now uses the existing
    `RotatingJsonlSink` (numbered parts `heartbeat_history.<n>.jsonl`, roll at
    256 MB) extended with `max_files=8` retention and a non-fatal
    `on_rotate_error="warn"` mode (a roll blocked by a reader on Windows is
    deferred 10 min with one warning line, never a traceback per heartbeat).
    ~2.3 GB / ~4 weeks bound, no config change; collectors keep the old
    defaults (no pruning, fail-loud). **Retention = deletion, so the policy is
    the owner's: merging PR #61 is the approval.** Merged != deployed (runner
    code). NOTE for the first post-deploy heartbeat: the live 5.8 GB file becomes
    part 1 whole and is pruned only after 8 further rolls (~4 weeks); disposing
    of it earlier is a separate Decision-queue item. `job_runs.jsonl` (554 MB) is deliberately NOT rotated yet: health
    tail-reads it and the audits use it for since-restart success rates; a
    rotation there needs the health reader to follow the rotated files.*
16. **Test suite writes into the live archive (found 2026-09-07).** The
    `run_ops_runner` tests in `tests/test_ops.py` dispatch `mock` jobs without
    an `output_root`, so `run_mock` falls back to `default_output_root()` =
    `G:\market_archive\raw\market\mock` — 56 run dirs so far, +2 per `pytest`
    run, and the cause of the permanent `unconfigured_lane:mock` offload
    warning. Fix: an autouse `conftest.py` fixture pointing every
    `MARKET_DATA_*_ROOT` at `tmp_path` (hermetic by construction), then delete
    the `mock` lane dir (owner nod — it is test debris, not data). Autonomous.
    *2026-09-08: fixture landed on `fix/test-hermetic-roots` (`tests/conftest.py`
    points `MARKET_DATA_ARCHIVE_ROOT` at a per-test temp dir and clears inherited
    per-root overrides; `test_suite_default_roots_are_hermetic` pins it).
    Merged as PR #59; the debris (72 run dirs by then) was deleted with the
    owner's nod the same day — item CLOSED.*
17. **`redeploy_runner.ps1` discards runner stdout/stderr (found 2026-09-07).**
    Its `Start-Process` has no `-RedirectStandardOutput/-RedirectStandardError`,
    unlike `run_ops_runner.ps1` (`*>> runner.log`), so `runner.log` last grew at
    the 08-25 boot start and a runner-process crash after a manual redeploy
    leaves no trace; the script's own "check runner.log" warning is misleading.
    Autonomous fix (ASCII-only `.ps1`, parse-check). *2026-09-08: fixed on
    `fix/redeploy-runner-log` — the relaunch goes through a hidden PowerShell
    child that appends stdout+stderr to `runner.log` (same `*>>` posture as the
    boot script) and writes a dated relaunch marker first; hygiene test pins
    the redirect in both scripts. Takes effect at the NEXT manual redeploy (the
    current runner was launched by the old script and stays unlogged).* Review
    residue, not fixed here: PS 5.1 `*>>` writes UTF-16LE while both scripts'
    `Out-File -Encoding utf8` markers are UTF-8, so `runner.log` from the BOOT
    path is already a mixed-encoding file (Get-Content renders the python output
    spaced). The redeploy path now writes all its lines under one redirect;
    aligning `run_ops_runner.ps1` the same way is a one-line follow-up. The
    reviewer's altitude suggestion — have the redeploy child run
    `run_ops_runner.ps1` itself instead of a hand-copied launch line — is
    recorded as an option; it needs a `-SkipMutex` switch for non-elevated use.
18. **Minimum-age floor for the two depth scorers (found 2026-09-07, still
    live after the 09-08 redeploy).** `backfill-replay` (`score-binance-depth`,
    `score-binance-depth-usdc`) and `backfill-stream-depth` (`score-stream-depth`,
    8 lanes) score the live 1800 s segment every ~77 min; the promoter then
    indexes the partial run for good. ~40 % of depth runs promoted since the
    redeploy are short. Fix = the same `min_age_hours` (default 1 h) PR #54 gave
    the trades/text scorers, threaded through `backfill_replay_summaries` and
    `run_backfill_stream_depth` + arg-survival tests; config needs no change
    (default applies) but the runner must restart to pick up the code for the
    scheduler-thread jobs — collector subprocesses import the checkout, the
    maintenance jobs run in-process. Autonomous PR; deploy at the next redeploy.
    *2026-09-08: fix on `fix/depth-scorer-min-age` — `--min-age-hours` (default
    1 h, `0` disables) on both `backfill-replay` and `backfill-stream-depth`,
    runner dispatch defaults pinned, `skipped_too_recent` surfaced in the
    stream-depth report; 6 regression tests in `tests/test_scorer_min_age.py`.
    Merged != deployed: the depth lanes keep truncating until the runner
    restarts on this code.* **Follow-up idea (from the PR review, not built):**
    the floor guards the two hourly summary writers, but the root cause is that
    `promote_replayable_runs` promotes any run with a replayable summary and
    the run-keyed index never revisits — a manual `replay-depth` on a live run,
    or `--min-age-hours 0`, re-opens the hole. A segment-close marker that the
    promoter requires (or the scorer refuses runs without) would close every
    path at once. Contract-adjacent (touches what "promotable" means) → needs an
    owner nod before building.
13. ~~Verify OKX/Bybit trades subscribe-replay behavior over live frames~~
    **DONE — verified 2026-07-06, no code change needed.** Live probe (2
    independent runs, 8 connections: OKX spot + swap, Bybit spot + linear,
    BTC): **zero trade-ID re-delivery** across back-to-back resubscribes —
    neither venue replays prior prints on subscribe, unlike Kraken (last-50
    `snapshot`) and Coinbase (`last_match`). Bybit labels every first
    `publicTrade` push `type:"snapshot"` but its content is fresh (boundary
    prints <=21 ms old that the previous connection never received — they
    shrink the rotation gap, they don't duplicate). No `subscribe_replay`
    tagging needed; curated OKX/Bybit trades carry no reconnect duplicates
    from this mechanism. Method + numbers in `docs/HISTORY.md` 2026-07-06.
14. **Local-only modelling raw lanes are unconfigured in `archive-offload`.**
    A few raw lanes that exist only in the gitignored local config surface as
    benign `unconfigured_lane` warnings every offload pass and have no retention
    bound (tiny today, but unbounded). Fix: add per-lane `gate: age_only` entries
    in the local-only `ops.live.local.json` (lane identities/specifics stay local
    per the public-safe contract). Left unactioned this session: tiny, not the
    G:-full cause, and touching local-only modelling-data lifecycle wants owner
    awareness.

15. **ACTIVE — text-capture P1 lanes (owner-approved 2026-07-13; NOT parked).**
    Two native lane families: `text-reddit` (fixed sub list, OAuth
    client-credentials polling of `/new` posts+comments, ~100 QPM budget) and
    `text-rss` (5 crypto news feeds, 1-5 min conditional-GET polling). Raw
    text only at capture (no capture-time NLP/filtering); envelope per row:
    `source`, `source_id`, `source_ts` (platform-claimed), `ingestion_ts`
    (plant clock, authoritative), poll metadata, untouched raw payload; dedup
    `(source, source_id, content_hash)`, edits kept as new rows; standard
    quarantine -> promote, exactly one promoter per lane; archive placement
    `raw/text/{source}/...`; volume well under 100 MB/day. Sequence:
    **(a) P0 probe — RSS probe DONE** (72 h, completed ~2026-07-16: 10,740
    polls, 421 item rows = 384 new + 37 edits, zero duplicate new ids /
    missing source-ts / future source-ts, 2 transient network errors; one
    ~16 h stale Cointelegraph publish-ts outlier -> `ingestion_ts` is the
    authoritative clock, claimed `source_ts` preserved + diagnosed only;
    of the 37 edits 25 were semantic title changes and 12 raw-only feed
    churn -> the lane hashes SEMANTIC fields only, so raw churn emits no
    row); Reddit probe stays blocked on the owner-created OAuth app
    (client id+secret dropped at `G:\market_archive\ops\reddit_app.json`,
    outside the repo; no account password involved) — the lane ships
    probe-less on the conservative defaults (~10 QPM vs the ~100 QPM
    budget) since it cannot start without the credentials file anyway;
    **(b)** probe readout folded into (a); **(c) DONE — lane build PR #35
    merged 2026-07-16 as `496075b`**: `text-rss-worker` +
    `text-reddit-worker` job types, envelope normalizer + text quality
    gate, `replay_text_run` verdict (`no_events` quiet segments quarantine
    by design so offload accounting closes), `backfill-text-replay`
    catch-up scorer (also scores event-less crash orphans — the funding
    lesson), cross-segment dedup cursor, curated target
    `curated/research/text`, STANDARDS v8 (§4.6), CollectorConcurrency
    23 -> 25 in BOTH runner scripts, example-config job family
    (enabled:false), arg-survival regression tests + mocked-network suite;
    `/code-review` + `/security-review` run on the PR; **(d) RSS DONE —**
    collector + scorer + quarantine + promoter enabled and deployed by guarded
    restart 2026-07-16 11:36 UTC (`pid=27212`). Reddit remains pending and
    disabled until `reddit_app.json` exists; **(e) IN PROGRESS through
    2026-07-30** — acceptance = >=2 weeks continuous green capture,
    `ingestion_ts` monotone, stable dedup ratios; then it accrues silently.
    **First checkpoint 2026-07-16 ~16:35 UTC: GREEN, no code defect** — full
    raw -> summary -> promote chain verified live incl. exactly-once
    promotion, the crash-orphan catch-up path, and cross-segment cursor dedup
    (evidence in the 07-16 audit stamp above). Still unexercised: a quiet
    zero-item segment (`no_events` -> quarantine-by-design) and the
    2026-07-26 text offload wiring (dated check).
    **(f) P2 source feasibility DONE (docs-only, 2026-07-16)** —
    [`docs/text_source_p2_feasibility.md`](docs/text_source_p2_feasibility.md):
    primary-source decision matrix (auth / cost / rate limits / terms /
    retention-deletion-edit semantics / timestamps incl. `availability_ts` /
    volume / bounded P0 probes / go-no-go) for Farcaster, official project
    sources (GitHub releases, Discourse governance, Snapshot, project blogs),
    YouTube, and X. Evidence revised the hypothesized order: **official
    sources first** (keyless, $0, terms-clean — probe-ready), **Farcaster
    second** (no public keyless read endpoint exists; needs an owner unlock:
    free hosted-API account+key vs ~2 TB dedicated node), **YouTube parked**
    (third-party transcript text has no permitted path; the API's 30-day
    refresh-or-delete storage rule conflicts with indefinite accrual),
    **X standing NO-GO** (pay-per-use $0.005/post read since 2026-02-06, no
    free read tier, 24 h deletion/edit-propagation duty for stored content).
    No probe, lane, config, account, or key was created; probes are
    owner-gated — see Decision queue.

## Decision queue (owner)

Decisions waiting on the owner; agents must not act on these without an explicit OK
(see `CLAUDE.md` Governance):

- **DO NOT run `scripts/redeploy_runner.ps1` from `main` before the pid-identity
  guard PR is merged (incident 2026-09-10 13:48 local).** The elevated redeploy
  read `ops-runner.lock`, which named pid 1668, and ran `Stop-Process -Force` on it
  without checking what the pid was. Windows had reused 1668 for a critical
  `svchost.exe`; the kernel bugchecked `CRITICAL_PROCESS_DIED (0xEF)` (minidump:
  terminated svchost.exe 1668, terminator powershell.exe 25208 = the script,
  started 4 s earlier). The plant came back at boot (13:49:36, runner on `main`
  `c041c4e`, so STANDARDS v12 is now fully deployed) and no data was lost beyond
  the reboot gap. `job_runs.jsonl` shows the runner alive 35 s before the kill,
  so the lock's pid did not match the live runner - WHY the lock named a wrong pid
  is not established (the runner has guarded its own locks against recycled pids
  since 2026-06-11; the script never did). Fix on `fix/redeploy-stale-pid-guard`:
  nothing is killed unless it is a plant python (repo path or `crypto_collector`
  in the command line); children are re-verified; a non-plant pid is reported
  and left alone; an unreadable (other-principal) python or a CIM miss with a
  live pid refuses instead of guessing. Hygiene test pins the gating form;
  harness-verified non-elevated against self-spawned processes. Owner:
  merge, then redeploys are safe again. Nothing needs a redeploy right now.
- **`normalized/{market,trades}` retention - the G: headroom lever (measured
  2026-09-10, open item 2).** Full walk of `G:\market_archive\normalized`:
  `market` (depth) **197.6 GB / 11.33 M files**, `trades` **31.7 GB / 4.07 M
  files**, `funding` 1 file; **229 GB / 15.4 M files total**. It was 83.2 GB on
  07-12, so the tree grows **~2.4 GB and ~165 k files per day**. G: is at
  **136 GB free** (468 GB on 07-12; 30 GB of that went to the curated repair) -
  at this burn the normalized tree alone consumes the remaining headroom in
  under two months, before raw and curated growth. Facts that frame the
  options: (a) **nothing in the plant consumes this tree's data** (correction
  2026-09-10 17:40: the `research-manifest` job walks it for per-day file
  counts and `cleanup` scans it for zero-byte parquet - both tolerate an empty
  tree; both walks had a listing-then-stat race that the move exposed - the
  manifest errored every 15 min from 17:30 - fixed in the manifest-tolerance
  PR, which needs the NEXT runner restart to take effect, see the new redeploy
  item below) - `cli.py` only
  writes it (`_resolve_normalized_root`); curation runs raw -> replay ->
  curated, the manifest lists curated only, and both studies to date read
  curated; (b) it is unmanaged by design today - `archive-offload` is
  raw-only and `cleanup` removes only `zero_byte_parquet` (STANDARDS section 7:
  "retained indefinitely"); (c) **20 enabled lanes still write it** (9 depth,
  7 WebSocket trades, 4 liquidation lanes) while 9 already run with
  `normalized_parquet: false` (Binance spot trades x2, the 6 Binance REST
  lanes, wallet-flow) and text is opt-in - the plant has run half its lanes
  without the layer since June with no consumer noticing; (d) the June
  precedent: 555 GB / 45 M files of normalized partitions were verify-moved
  G: -> `D:\market_archive_cold` with a detached `robocopy /MOV` (06-24,
  0 failures); D: has 782 GB free today. **Proposal (recommended, in this
  order; none of it deletes data):** (1) *stop the bleed* - set
  `normalized_parquet: false` on the 20 lanes in `ops.live.local.json` (+ the
  example config) and redeploy; reversible per lane, zero effect on raw or
  curated; (2) *recover the 229 GB* - detached `robocopy /MOV` of
  `normalized/{market,trades}` to `D:\market_archive_cold\normalized\`, same
  recipe and verification as June, G: back to ~365 GB free; (3) *contract* -
  STANDARDS section 2.2 becomes "optional per lane, default off; hot-path
  layer retired 2026-09; history on the cold tier" and section 7 says so,
  `STANDARDS_VERSION` 13 (docs PR after the decision). Alternatives the owner
  may prefer instead of (2): keep the tree on G: (buys nothing), or delete it
  (no in-plant data consumer; unknown external readers - the owner's call, not
  proposed). Alternative to (1): a `normalized_days` retention job (code) -
  more machinery to keep a layer nothing reads. What Claude does on OK: the
  config edit + example-config PR + STANDARDS PR; the redeploy and the
  robocopy launch are the owner's (elevated, and a state change).
  *2026-09-10 15:00: owner approved step 1 - `normalized_parquet: false` set on
  all 21 collector lanes in `ops.live.local.json` (backup
  `.bak-20260910-normalized-off`) and in the example config; STANDARDS 2.2
  retired (v13) in the same PR. LIVE since the 15:06 redeploy - no normalized
  file written after 15:06:02 (verified 16:58).*
  *Step 2 DONE 2026-09-11 00:12 local (owner: "Do it", 17:06; launched detached
  from Claude's non-elevated session after a 155-file probe move proved the
  permissions): `market` 11,346,201 files / 197.9 GB in 5 h 06 min, `trades`
  4,085,440 files / 31.7 GB in 1 h 53 min, **FAILED 0 on both**, 0 files left on
  G: (robocopy /MOVE removed the source directories too; `G:\market_archive\
  normalized` now holds only the June `binary_options` leftover and `funding`).
  Logs `ops/robocopy-normalized-{market,trades}-20260910.log`, marker
  `ops/robocopy-normalized-20260910.done.json`. **G: 131 -> 391 GB free** (more
  than the 229 GB logical size: 15 M small files gave back their cluster slack);
  D: 491 GB free. Side effect: the in-process `research-manifest` job errored 7
  times on files vanishing under its walk (see the redeploy item below; fix on
  `main`). The recipe that ran, kept for the next such move:*
  ```powershell
  Start-Process robocopy -WindowStyle Hidden -ArgumentList 'G:\market_archive\normalized\market','D:\market_archive_cold\normalized\market','/MOVE','/E','/COPY:DAT','/DCOPY:T','/R:1','/W:1','/MT:8','/NP','/NFL','/NDL','/LOG:G:\market_archive\ops\robocopy-normalized-market-20260910.log'
  ```
  *then the same with `trades` and its own log. Verify `FAILED : 0` in each
  summary and that the source is gone.*
- **Next elevated redeploy (no urgency, bundle with the next real need):** the
  manifest/cleanup walk-tolerance fix runs IN-PROCESS in the runner, so until a
  restart the live `research-manifest` job errors once per 15 min while a move
  or offload deletes files under it (harmless: nothing else fails, the manifest
  just does not refresh during the 2026-09-10 normalized move). After the
  restart: `research-manifest` `error_count` stops growing.
- ~~**Next elevated redeploy — owner checklist**~~ **DONE 2026-09-10 15:06 local,
  verified 16:58:** `redeploy_runner.ps1` (guarded, `main` >= `5b6f440`) wrote its
  relaunch marker to `runner.log` at 15:06:08; the new runner (pid 10432, python,
  started 15:06:08, lock `created_at` 13:06:09Z) has a fresh heartbeat, all 31
  worker lanes running under it, 0 job errors. **Normalized writes stopped:** the
  newest file under `normalized/{market,trades}` is stamped 15:06:02 (the old
  runner's last flush), nothing since -> step 2 of the normalized item is
  unblocked. **OKX 1 h gate live:** the first post-redeploy day-run
  (`okx_perp_liquidations/20260910_130610`) quarantined 57 `stale_or_clock_skew`
  rows in its first 1 h 50 min, ALL later than 3600 s (min 3667 s, max 4443 s),
  none in 900..3600 s - so the venue's tail runs past 1 h too; those rows stay
  quarantined by design (raw keeps them). Kept below for the record:
  The 09-08 checklist is done (redeploys 09-08/09-09, the 09-10 boot).
  Pending config in `ops.live.local.json` (backup
  `ops.live.local.json.bak-20260910-normalized-off`): `normalized_parquet: false`
  on all 21 collector lanes and `max_delay_ms: 3600000` on `okx-swap-liquidations`
  (owner decisions 2026-09-10). Before running: `main` must be at or past
  `9c9204d` (#74, pid-identity guard) - check `Test-PlantProcess` exists in
  `scripts/redeploy_runner.ps1`. After `scripts/redeploy_runner.ps1` (elevated,
  at the PC): (1) the script prints `Lock names runner pid=... ` followed by a
  kill of a python, never of anything else, then `Redeploy OK`; (2) lock pid ==
  a live plant python, heartbeat advancing, 37 slots; (3) no new files under
  `G:\market_archive\normalized\{market,trades}` after the restart (newest
  mtime stays at the restart time) - then the robocopy move (Decision queue
  item above, step 2) can start; (4) the next OKX liquidation day-run's
  `quarantine/events.jsonl` no longer collects `stale_or_clock_skew` rows for
  15 min..1 h lags.
- **OKX liquidation lane: replay verdict is wrong for this feed (2026-09-09).**
  The first full OKX day-run (`okx_perp_liquidations/20260908_002925`, 11,564
  rows, 340 swap products) scores `replayable: false` with
  `non_monotonic_event_time` (1,744) and `excessive_clock_skew` (186 rows > 60 s,
  max 897 s), while the three per-symbol Bybit day-runs pass. Diagnosis from the
  raw rows (read-only): (a) the lane is scored by the *trades stream* replayer,
  whose GLOBAL event-time monotonicity is meaningless for a channel that
  interleaves 340 instruments — the same lesson as the multi-wallet wallet-flow
  scorer (v9) and the per-product OI ordering (v11); (b) even per product there
  are 691 backward steps across 71 products, and 186 rows arrive 60 s–15 min
  after their `exchange_time`, concentrated in illiquid alt swaps (SOPH 82,
  CP 21, CNPY 10) and NOT clustered in time — so this is OKX's own delayed /
  batched delivery of `liquidation-orders` details (one push carries details
  whose `ts` spread up to 246 s), not a reconnect replay or a capture defect.
  Receipt delay is otherwise tight (p50 1.2 s, p90 2.4 s). BTC-USDT-SWAP: 407
  rows, in order. **Proposal (STANDARDS change → owner):** score the
  `liquidations` channel with a liquidation-aware verdict — per-product
  ordering, venue delivery lag recorded as a non-gating finding
  (`delayed_delivery_count`, threshold per venue, OKX 15 min) instead of
  `excessive_clock_skew`, structural validity as the gate; document in the
  liquidations section that OKX details are venue-delayed and that
  `received_at` is the availability clock for research. STANDARDS_VERSION bump
  (v12). Until then the lane is capture-complete but not research-ready by the
  current label; raw is untouched.
  *2026-09-10: PR open on `feat/liquidation-replay-v12` implementing exactly
  this - `replay_liquidations_run` (STANDARDS 4.10), the three liquidation
  collectors wired to it, `backfill-trades-replay --liquidations` for the
  re-score, `STANDARDS_VERSION = 12`, 11 tests. Owner decisions: (1) merge
  (contract change); (2) after redeploy, re-score the OKX history with
  `backfill-trades-replay --liquidations --overwrite --source-root
  <raw>/okx_perp_liquidations --min-age-hours 24 --max-age-hours 720` (the
  default 24 h window silently skips older runs; labels only - the lanes are
  raw-only, nothing is promoted or deleted); (3) **the live gate censors the
  OKX tail**: `max_delay_ms` = 900 s quarantined 307 rows of the 09-08 run as
  `stale_or_clock_skew` (more than the 186 late rows that reached clean), so
  details delivered > 15 min late never reach `clean/`. Raising the OKX lane's
  `max_delay_ms` (e.g. 3600000) is a data-contract call for the owner; until
  then research on this lane knows the lag distribution is truncated at 15
  min. /code-review on the PR found (1)-(3); a 1 s `received_at` tolerance was
  added so a host clock correction cannot fail a whole day-run. Merged !=
  deployed: collectors pick up the scorer at the next runner restart.*
  *2026-09-10 14:40: DONE - #72 merged, deployed by the 13:49 reboot, and the
  hot OKX history re-scored (`--liquidations --overwrite --max-age-hours 720`):
  24 hot runs scanned, 23 scored (19 had no summary at all - the 7200 s-kill
  era runs of 09-06/07 and the two runs ended by the 09-09 redeploy and the
  09-10 crash - and 4 pre-v12 summaries replaced), all 23 replayable, only the
  live run skipped, 0 failures. `docs/lanes.md` OKX lane -> B. Owner decided
  2026-09-10 15:00: `max_delay_ms: 3600000` on the OKX lane (both configs, v13),
  live since the 15:06 redeploy (57 rows > 1 h late quarantined in the first
  1 h 50 min - the venue tail runs past 1 h, by design left in quarantine); cold-tier
  runs from 08-25..09-05 keep their pre-v12 summaries (re-score there is a
  read of the cold tier, not proposed).*
- **Text-capture P2 probes (from the 2026-07-16 feasibility doc — see
  `docs/text_source_p2_feasibility.md` §7; none urgent, no rationale here per
  the public-safe contract).** Four calls: (1) approve the 72 h keyless
  official-sources P0 probe ($0, no accounts — GitHub releases Atom+API,
  3-5 Discourse governance forums, Snapshot GraphQL, project-blog feeds;
  recommended yes); (2) Farcaster read-path unlock — free hosted-API
  account+key for the probe (recommended) vs dedicated ~2 TB node hardware
  vs defer (no public keyless endpoint exists); (3) YouTube storage-rule
  posture before any key/probe (30-day refresh-or-delete vs indefinite
  accrual; recommended default = keyless-feed-metadata-only or defer —
  transcript text of third-party videos has no permitted path); (4) X —
  acknowledge the standing NO-GO at current terms (pay-per-read, no free
  read tier, 24 h deletion propagation; corrects the 2026-07-12 local-doc
  access summary). Probes run only on explicit OK.
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
- **Repair the truncated curated runs (2026-09-07 audit, finding 1).** About a
  third of promoted runs on all 21 trades/depth lanes and the wallet-flow lane
  since June carry a partial row set (the hourly scorer scored the live
  segment; per-lane counts in the audit stamp; 7.5 % of wallet-flow fills
  missing). Raw is intact on hot and cold tiers, so the rows are recoverable.
  Options: (a) build a `repromote-short-runs` tool that finds index rows with
  `promoted_rows` below the run's clean row count, removes that run's curated
  rows, re-promotes from raw (hot or cold) and rewrites the index row —
  touches curated data, hence owner-gated; recommended, wallet-flow lane first
  (it is the §4.7 prospective evidence); (b) leave history as is and document
  the truncation for consumers. Either way the bleeding stops only when a
  minimum-age floor reaches ALL scorers: PR #54 covers trades + text, the two
  depth scorers (`backfill-replay`, `backfill-stream-depth`) still need it
  (comment on #54). *Status 2026-09-08: trades + text + wallet-flow truncation
  stopped at the 00:29Z redeploy (0 short runs since); depth lanes still
  truncating ~40 % of runs until PR #58 (merged) deploys.* **DECIDED 2026-09-08:
  option (a) — build the re-promote tool.** Tool merged as PR #63 (2026-09-08;
  `repromote-short-runs` CLI, dry-run by default, `--apply` per lane after the
  owner reads the dry-run report). **Wallet-flow lane REPAIRED 2026-09-08
  13:51Z (owner-approved apply):** 229 runs re-promoted from raw (177 from the
  cold tier), 20,079 partial rows replaced by 38,495, 0 failures; the lane's
  curated total is now 251,733 of 251,848 raw fills (99.95 %, was 92.5 %).
  Two runs still short: `20260908_001139` (summary was scored on the live
  prefix; re-scored with `--wallet-flow --overwrite` the same hour, repairable
  on the next pass) and `20260904_124639` (moved to cold by the offload job
  during the 13-min scan — the race PR #65 fixes; repairable on the next pass).
  **All ten trades lanes REPAIRED 2026-09-08 18:24–20:27 local (owner-approved
  after the read-only inventory):** 11,272 runs re-promoted (10,751 from the
  cold tier), 109.9 M partial rows replaced by 197.4 M, 0 failures; 79 runs whose
  current summaries are not replayable had 387,900 partial rows removed; 41 runs
  with prefix-scored summaries re-scored on the full segment (39 replayable, 2
  not) and picked up by a second pass. Verified against raw: every trades lane
  at 99.93–99.99 % of its raw rows (786.4 M of 786.7 M dataset-wide), part-files
  still run-pure. Narrative in `docs/HISTORY.md` 2026-09-08. **All ten depth
  lanes REPAIRED 2026-09-09 22:52 → 2026-09-10 09:07 local (owner-approved after
  Codex's read-only inventory; 36 prefix-scored summaries re-scored first):**
  12,684 runs re-promoted (12,009 from the cold tier), 186.9 M partial rows
  replaced by 337.1 M, 2 not-replayable partial runs removed, 0 failures, 0
  skips. Verified against raw: every depth lane at 99.99–100 % (1,148.7 M of
  1,148.8 M rows), 12 near-threshold runs remain short by a few rows, part-files
  run-pure (0 shared in 51,121). **The curated tier is complete again on all 21
  trades/depth lanes; this item is CLOSED.** Residue: G: fell to 138 GB free
  after absorbing ~30 GB of repaired parquet — open item 2 (normalized-tree
  retention) is now the headroom lever.
- **350 torn curated part-files from the June G:-full week (found 2026-09-08 by
  the post-repair integrity scan of all 51,593 `trades_replayable` part-files).**
  Every one is 176–643 bytes (Parquet header, no footer — a promoter flush that
  died when the disk hit 0 bytes), mtime 2026-06-19 (199 files), 06-21 (16),
  06-22 (64), 06-23 (71); 75.7 KB in total, spread over all seven sources
  (okx 86, bybit 63, mexc 52, binance 43, coinbase 38, kraken 38,
  binance-futures 30). They
  hold no readable rows, but any reader that opens a whole partition with a
  pyarrow dataset scan fails on them (`Parquet magic bytes not found`). The
  promoter writes its index row only after a successful flush, so the runs
  these belonged to were never indexed and were re-promoted whole on a later
  pass — the files are pure debris. The repair tool leaves unreadable files
  alone by design. **DECIDED + DONE 2026-09-08 (owner approved the deletion):**
  each of the 350 files was re-checked (< 2 KB and no readable Parquet metadata)
  and removed — 350 deleted, 75,701 bytes, 0 skipped. **Re-scan 2026-09-09
  22:17 (run by the parallel Codex session): 51,936 part-files, 0 corrupt, 0
  shared-run files, 795,994,667 rows.** Coordination note: the owner had also
  approved a *reversible* quarantine of the same files to Codex, which prepared
  (but had not run) a move-to-quarantine script; the files were already deleted
  by then, so that script is moot — two agents, one owner, two approvals for one
  action. Follow-up idea: make the integrity scan a `health` finding so torn
  part-files surface within a day instead of at the next repair.

Decided 2026-09-08 (recorded, closed):
- **Liquidation raw dirs get `age_only` offload rows.** Owner approved 2026-09-08;
  rows for `bybit_perp_liquidations_{btcusdt,ethusdt,solusdt}`,
  `okx_perp_liquidations` and the legacy shared `bybit_perp_liquidations` added
  to `ops.live.local.json` (backup `.bak-20260908-liq-offload`) and to
  `ops.live.example.json`; `binance_perp_liquidations` already had one. Runs
  older than 4 days move to the cold tier from the first offload pass after the
  next redeploy.
- **`raw/market/mock/` test debris deleted.** Owner approved 2026-09-08 after
  PR #59 (hermetic test roots) merged: 72 run dirs / 56.7 KB removed; the
  standing `unconfigured_lane:mock` offload warning clears at the next pass.
- **5.8 GB heartbeat-history backlog: delete after the redeploy.** Owner chose
  option (a) 2026-09-08; the action rides the redeploy checklist above (the file
  only becomes `heartbeat_history.1.jsonl` once the rotation code is running).
- **Truncated curated history: build the re-promote tool** (see the ACTIVE
  entry above; the per-lane apply remains gated).
- **Merge #54/#55/#56 then redeploy — DONE.** Owner approved the merges 09-07
  (squash-merged 17:01–17:03Z; one trivial ROADMAP conflict on #54 resolved in
  a worktree) and ran the elevated redeploy 2026-09-08 00:29Z on `50f44d7`.
  Verification in the 09-08 deploy note above: liquidation kills gone, per-symbol
  dirs live, OI curated dataset filling, trades truncation stopped.
- **Options-IV cutover step (4) — DONE.** `BinanceIV Collect History` and
  `BinanceIV Collect Deribit` disabled 2026-09-08 (all V1 tasks now Disabled);
  the plant's `binance-options-chain-snapshot` + `deribit-options-snapshot`
  lanes are the sole options collectors. Step (5) — repoint the V1
  surface-history builder at the archive — stays V1-side (that repo's
  `NEXT_STEP.md`). Cadence option unchanged: a 2-min Binance chain is one
  `interval_seconds` edit (~1 GB/day raw).

Decided 2026-08-01 (implemented locally; elevated restart pending):
- **Aged unaccounted runs: quarantine-preserve plus durable backstop.** The owner
  asked to fix the plant health warning. All 17,809 aged unaccounted runs were
  classified with bounded diagnostics and 0 failures; no raw data was deleted.
  The live offload report is now at 0 stuck. The offloader now performs the same
  preserve-first classification automatically after the full offload-age window,
  then relies on the existing byte-verified cold move. The same audit found and
  fixed oldest-deadline fairness for the serialized maintenance slot.

Decided 2026-07-13 (recorded; build ACTIVE — see open item 15):
- **2026-07-12 modelling collection request: APPROVED at P1 scope, native
  public lanes.** (Request rationale stays in the gitignored local doc; the
  approved capture surface itself is public by design.) Owner decisions
  resolved: (1) GO on a low-volume raw-text capture lane family — fixed-list
  crypto subreddits + crypto news RSS; (2) source set = P1 only for now (the
  P2 aggregator/protocol sources are deferred, revisit only with a passing
  probe; P3 stays OFF); (3) placement = **native public-repo lanes** (Limitless
  precedent: local-only artifacts drift outside CI/review/hygiene gates);
  (4) the optional P2 API key sign-up is moot for now. Probe-first shop rule
  applies: a 24-72 h scratch feed-reality probe precedes any lane code.

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
- **Per-symbol source dirs for the OI and Bybit liquidation lanes (2026-09-02) —
  IMPLEMENTED as option (a) in PR #54 (`source_suffix` on all six lanes, OI chain
  + offload rows + manifest parser follow; effective at the next redeploy).**
  The three `binance-*-open-interest` lanes and the three `bybit-*-liquidations`
  lanes write into one `<source>/<YYYYMMDD_HHMMSS>/` run dir whenever their
  segments start in the same second — systematic after a restart, because the
  segments stay synchronized — so 9 of the 12 post-restart OI runs are
  mixed-symbol and non-replayable, and the PR #54 OI curated chain would promote
  nothing from them. Options: (a) set `source_suffix` per lane in both configs
  (`binance_perp_open_interest_{btcusdt,ethusdt,solusdt}`,
  `bybit_perp_liquidations_{...}`), the per-instrument mechanism STANDARDS §2.1
  already documents; a layout change for those lanes, so the PR #54 OI
  promote/quarantine/score jobs, the offload lane rows and the research-manifest
  lane parser must follow, and the per-source `_collector_state.json` resume
  state stops being shared between symbols; (b) make `prepare_run_paths`
  collision-proof (suffix on an existing dir) — code-only, but it changes the
  run-dir naming contract in STANDARDS §2.1. Recommendation: (a), then redeploy.
  Owner-gated (layout + config + redeploy).

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
| Options chain + Deribit snapshots (Binance `eapi` BTC+ETH 15-min, ~2-min in V1's earliest weeks; Deribit BTC+ETH 5-min) | **this plant** (raw-only lanes, STANDARDS §4.9; reassigned from `G:\Binance_IV_V1` 2026-08-29 — pre-cutover history stays frozen there; IV research/surface derivation stays in that repo) |
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
  Deribit data was covered by `Binance_IV_V1`; since 2026-08-29 by the §4.9
  `deribit-options-snapshot` lane here, which also captures future book
  summaries).

---

## Manager protocol

- Plans, decisions, and dated checks live **here**, not in chat history or agent
  memory. Memory holds pointers; this file holds the plan.
- Every session that changes scope, completes an item, or makes a decision updates
  this file in the same change.
- At session start: check the dated table above and flag anything due.
- Completed work moves to [`docs/HISTORY.md`](docs/HISTORY.md) with its root-cause
  narrative; this file stays short and forward-looking.
