# Roadmap

**This file is the single source of truth for plans and open work.** It is
maintained by the project manager (Claude) and updated in every session that
changes scope or state. Companion docs:

- [`README.md`](README.md) — what the plant collects *today* (capability snapshot)
- [`STANDARDS.md`](STANDARDS.md) — the data contract (schemas, replayability, retention)
- [`docs/HISTORY.md`](docs/HISTORY.md) — resolved-work narrative (what was fixed, and why)

Last updated: **2026-07-16**.

> **Operating mode — safe shaping (owner directive, 2026-07-04).** No extended
> building on Claude's initiative: no new venues, lanes, or instruments, no big
> refactors, no new subsystems. Active work is plant health and audits,
> observability, retention and hygiene, small low-risk fixes, and clean
> documentation. Expansion items below are tagged **PARKED** and need an
> explicit owner ask to start.

---

## Current state (2026-07-12)

**21 enabled collector lanes** across Binance (spot USDT + USDC, USDT-M perp via
REST), Coinbase, Kraken, Bybit (spot + linear perp), MEXC, OKX (spot + linear
perp) — all BTC. **Kalshi crypto-binary collection is TURNED OFF as of
2026-06-17** (both Kalshi jobs `enabled:false`; it was the G:-full root cause —
see `docs/HISTORY.md` 2026-06-17 + Decision queue). Full quarantine → promote
curation chain per lane, hourly score catch-up self-heal, research manifest,
cleanup retention, and cold-tier archive offload. Live runner restarted at boot
2026-07-11 ~15:50 UTC (machine reboot; SYSTEM task, `ops.live.local.json`) on
current `main` (`b3cf669`), so **PRs #24, #26, #28, and #31 are all deployed** —
nothing merged is awaiting a restart. All 21 lanes green. CI green on `main`.

---

## Dated operational checks

| Due | Check |
| --- | --- |
| ~~2026-06-18~~ DONE 06-22 | Offload-index spot-check **PASSED**: 4509 index rows == cold run-dirs 1:1 on every lane, 0 duplicates/malformed, 0 unindexed pile-up, 0 missing cold copies, 0 sampled file-count mismatches, 0 indexed runs still hot. Offload live (newest `moved_at` 2026-06-22T09:53Z). Dry-run also flags **16 `stuck_unaccounted_runs`** (raw from 06-09..06-11 never promoted: 8 `binance_perp_funding` + 8 trade/depth) — designed safety surface, but a real promotion gap to investigate. *(Re-measured 2026-07-04: the true cohort is **14,211** — the 06-16..06-23 crash-loop debris had not yet crossed the 10-day offload fence when this check ran. See the 07-04 audit stamp + Decision queue.)* |
| ~~2026-07-15~~ DONE 07-16 | **The 07-05 orphan wave crossed the offload fence as predicted**: `stuck_unaccounted=17,519` (forecast ~17.5k), `failed=0`. Health's sole finding is the expected `offload_stuck_above_baseline:17519`; the queued cleanup/backstop decision remains open. |
| ~~2026-06-19~~ DONE 06-24 | The 06-17 `robocopy /MINAGE:3` move never finished (~88% of partitions still on G:), leaving G: at **3.9 GB free**. First retry (06-22) was killed by the Bash tool's 10-min timeout after freeing ~57 GB. Relaunched **detached via `Start-Process`** (pid 48444) so it survives session/tool teardown -> **COMPLETED 2026-06-24 16:19, FAILED: 0** (45.29 M files / 555 GB moved G:->`D:\market_archive_cold`). **G: now 489 GB free.** D: holds 113,407 normalized partitions (full set). 1 partition / 2 parquet files remain on G: -- robocopy *skipped* them (already byte-present on D: from the 06-17 partial), so redundant not stranded; immaterial (489 GB free). Lesson: long-running moves must be detached, never run inside a Bash call (10-min cap). |
| 2026-07-26 | **Text raw offload wiring.** The first `raw/text/text_rss` runs cross the 10-day offload fence 2026-07-26, but the `archive-offload-text` job (shipped `enabled:false` in the example config, PR #35) is **not in the live config** — nothing moves to cold and nothing accounts text runs after that date. Volume is tiny (~2 MB/day), so this is warn-level, not urgent; but it must land **before any future `cleanup` `apply:true`** (cleanup's raw scan covers `raw/text` at the 14-day default, and deleting un-offloaded raw would discard the rebuild source). Owner action: copy the job from the example config into `ops.live.local.json` at the next runner restart opportunity, or defer knowingly. |

**Last ops audit:** 2026-07-16 — **plant GREEN before text deployment; RSS
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

**Previous ops audit:** 2026-07-12 — **plant GREEN today; two self-healed network
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
   verified live 07-12**: it caught the +4 cohort growth within a day. Audit
   with `--stuck-unaccounted-baseline 14211` until the cohort cleanup lands
   (expect the count to step to ~17.5k around 07-15 — see the dated check and
   decision queue), then reset the baseline to 0.
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
  **UPDATE 2026-07-12: the population is NOT closed.** +4 funding restart
  partials (06-27..07-02) crossed the fence (count now 14,215), and a **~3.4k
  wave of 07-05 network-outage crash-loop partials crosses ~2026-07-15** (->
  ~17.5k total; small bytes, mostly sub-MB dirs). Root pattern: a fast
  crash-loop mints run-dirs faster than they can ever be scored, and the
  funding lane orphans on *every* worker restart (inline scoring at clean
  segment close only; no catch-up scorer). This materially strengthens option
  (b) — the drip is ongoing, not historical — with (a) still wanted once to
  clear the backlog.

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
