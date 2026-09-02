# Roadmap

**This file is the single source of truth for plans and open work.** It is
maintained by the project manager (Claude) and updated in every session that
changes scope or state. Companion docs:

- [`README.md`](README.md) — what the plant collects *today* (capability snapshot)
- [`STANDARDS.md`](STANDARDS.md) — the data contract (schemas, replayability, retention)
- [`docs/HISTORY.md`](docs/HISTORY.md) — resolved-work narrative (what was fixed, and why)

Last updated: **2026-08-17**.

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

**Last ops audit:** 2026-09-02 — **runner healthy, options-IV cutover live; the
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
finding), STANDARDS text clarified, test pins it. Promotion still needs a
curation chain — see Decision queue. (3) **`hyperliquid-leaderboard-snapshot`
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
2. **`normalized/{market,trades}` retention (no longer minor).** 66.2 GB as of
   2026-07-04, growing ~3 GB/day, and still unmanaged: `archive-offload` is
   raw-only and `cleanup` only removes zero-byte parquet. This was the primary
   plant-side driver of the -56 GB G: burn 06-30..07-04. Same blind-spot shape
   as the Kalshi normalized tree that caused the 06-17 G:-full incident, just
   ~20x slower. Needs an offload/retention policy (code change; data-lifecycle
   -> owner sign-off on the policy, implementation is autonomous).
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
    — fold into `run_cleanup`.
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

- **Redeploy for the 2026-09-02 lane fixes (PR `fix/lane-worker-names-oi-replay`).**
  The worker-name and leaderboard-path fixes live in `ops.live.local.json`
  (backup `ops.live.local.json.bak-20260902-worker-names`) and take effect only
  at `scripts/redeploy_runner.ps1`. After the restart, confirm in the heartbeat
  that `binance-{btc,eth,sol}-open-interest`, `binance-futures-rest-funding` and
  `bybit-{btc,eth,sol}-liquidations` accumulate `success_count` with 0 new
  errors, and that `hyperliquid-leaderboard-snapshot` succeeds once (daily);
  only then disable the interim `HyperliquidLeaderboardDaily` user task.
- **Open-interest curated dataset (2026-09-02).** OI runs are replayable after
  the replayer fix but nothing promotes them: the lane needs
  `quarantine-binance-perp-open-interest` + `promote-binance-perp-open-interest`
  (shipped **disabled** in `ops.live.example.json`, target
  `curated/research/open_interest`) plus an `archive-offload-cold` lane row with
  the matching promotion/quarantine indexes. That is a new curated dataset —
  STANDARDS §1 row + `STANDARDS_VERSION` bump — hence owner-gated. Until then
  the 245+ OI runs sit on the hot tier (no offload row either — deliberately,
  so an `age_only` move cannot strand them un-promoted on cold).
- **Liquidation lanes vs. the 7200 s subprocess timeout (2026-09-02).** The
  Bybit/OKX liquidation workers are `rotate_at_midnight` day-long segments, but
  the runner kills any collector subprocess after 7200 s, so every run is torn
  at the 2 h mark and never gets a replay summary (126/133 Bybit runs). Options:
  (a) per-lane subprocess timeout ≥ 25 h for the rotate-at-midnight lanes
  (recommended — the lanes were designed for daily files); (b) declare them
  raw-only (STANDARDS row) and give them `age_only` offload rows; (c) drop
  `rotate_at_midnight` and use `max_segment_seconds: 1800` like the REST lanes.
- **Options-IV cutover step (4) is ready (2026-09-02).** Both plant lanes have
  run 24 h+ clean in parallel with V1; disabling `BinanceIV Collect History`
  and `BinanceIV Collect Deribit` (there is no task literally named
  `Binance IV Collector`) is now purely the owner's call. Step (5) (repoint the
  V1 surface-history builder at the archive) stays V1-side.
- **Options-IV lane reassignment (2026-08-29, PR `feat/options-iv-snapshot-lanes`).**
  *Status 2026-09-02: merged (#51) and deployed 09-01; steps (1)–(3) done.*
  Collection of the Binance eapi options chain + Deribit options snapshots moves
  from `G:\Binance_IV_V1` into two raw-only reference lanes here (STANDARDS §4.9;
  keyless public endpoints; cadences match what the V1 series has run since
  2026-05: 15 min Binance / 5 min Deribit). Owner steps, in order: (1) merge the
  PR; (2) copy THREE things from `ops.live.example.json` into
  `ops.live.local.json` — the two lane entries, the two `archive-offload-cold`
  `age_only` lane rows, AND the two cleanup `raw_policy` pins
  (`market/*_options*=3650`; without the pins the cleanup default `raw_days: 14`
  would eventually delete unrecoverable snapshots) — then run
  `scripts/redeploy_runner.ps1` (merged != deployed; CollectorConcurrency is now
  37); (3) run OLD and NEW collectors in parallel ~24 h and check both lanes in
  the health report's `poll_lanes` table; also glance at hot-tier run-dir counts
  after any offload backlog — the shared 200-runs/pass offload budget drains
  lanes in config order and these two sit last; (4) only then disable the three
  `Binance_IV_V1` scheduled tasks (`Binance IV Collector`,
  `BinanceIV Collect History`, `BinanceIV Collect Deribit`) — overlap beats a
  gap, dedup at read time is trivial, backfill is impossible. Note
  `BinanceIV Collect History` only DERIVES the surface-history CSV from the same
  chain pull, so nothing is lost by disabling it once step (5) lands; (5)
  repoint the V1 repo's surface-history builder at the archive (V1-side work,
  tracked in that repo's `NEXT_STEP.md`). Open cadence choice: the earliest V1
  weeks sampled the Binance chain at ~2 min; if intraday IV research wants that
  resolution back, it is one `interval_seconds` edit on the lane (payload is
  ~2 MB raw per snapshot — ~1 GB/day raw at 2-min).
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
