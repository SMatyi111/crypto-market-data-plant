# Roadmap

**This file is the single source of truth for plans and open work.** It is
maintained by the project manager (Claude) and updated in every session that
changes scope or state. Companion docs:

- [`README.md`](README.md) — what the plant collects *today* (capability snapshot)
- [`STANDARDS.md`](STANDARDS.md) — the data contract (schemas, replayability, retention)
- [`docs/HISTORY.md`](docs/HISTORY.md) — resolved-work narrative (what was fixed, and why)

Last updated: **2026-06-13**.

---

## Current state (2026-06-12)

All green. 22 enabled collection lanes (21 collector workers + continuous Kalshi
quote sampling) across Binance (spot USDT + USDC, USDT-M perp via REST), Coinbase,
Kraken, Bybit (spot + linear perp), MEXC, OKX (spot + linear perp), and Kalshi
crypto binaries — all BTC. Full quarantine → promote curation chain per lane,
hourly score catch-up self-heal, research manifest, cleanup retention, and
cold-tier archive offload. CI green on `main`; live runner restarted
2026-06-12 16:51 UTC on main tip 8eb4c64 (83 jobs) — that restart also switched
the Kalshi quote lane from ~50%-duty burst sampling to continuous back-to-back
1800 s runs (owner request; raw volume roughly doubles to ~16 GB/day — see
`docs/HISTORY.md` 2026-06-12).

---

## Dated operational checks

| Due | Check |
| --- | --- |
| **2026-06-22** | First `archive-offload` candidates reach offload age. Spot-check the lane `_offload_index.jsonl` entries against the `D:\market_archive_cold` tree: files verified-moved, counts match, no `unindexed` pile-up. |

**Last ops audit:** 2026-06-11 (scheduler-stall incident response — see
`docs/HISTORY.md` 2026-06-11 entry; plant verified stable post-mitigation: 21
lanes collecting, full maintenance cycle 122s, zero errors). Ritual: if this
stamp is more than ~3 days old at session start, audit the live plant first —
see `CLAUDE.md` "Quality gates & review protocol".

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

## Decision queue (owner)

Decisions waiting on the owner; agents must not act on these without an explicit OK
(see `CLAUDE.md` Governance):

- **D:\market_archive legacy history** — retention vs. merge (open item 1 above).
  Owner deferred 2026-06-11: stays read-only until research needs pre-cutover dates.
- **Historical curated duplicates (2026-06-12 audit residue).** Until the audit
  fixes deploy+age in, curated data carries known duplicates: kraken trades (up to
  ~50 subscribe-replay prints per segment boundary since the lane went live),
  coinbase trades (one `last_match` per boundary), and possibly binance perp
  aggTrades (crash-window re-fetches). Options: (a) document + dedupe by
  `(product, trade_id)` at read time in research consumers, or (b) re-promote the
  affected lanes from raw on the fixed code (touches curated data — owner call).
  New capture is clean once the fix PR deploys.
- **Kalshi raw retention at continuous volume (queued 2026-06-13).** The continuous
  quote lane writes ~16 GB/day raw — the archive's heaviest writer (plant total
  ~38 GB/day). With offload `min_age_days=14`, steady-state raw-in-flight on G: is
  ~530 GB against ~548 GB currently free, and `D:\market_archive_cold` (7.3 TB free)
  absorbs total raw for roughly 6 months before filling. Options: (a) keep defaults,
  revisit when D: passes ~50%; (b) give the Kalshi lane a shorter offload age
  (14 -> 3-7 days) to cut the G: steady-state by ~100-180 GB; (c) post-offload
  retention cap — delete aged Kalshi raw after verified offload + curation
  (deletion = owner); (d) compress raw at offload time (JSONL gzips well; cuts
  cold-tier growth for all lanes). Deciding by the 2026-06-22 offload spot-check
  would let any config change ride that check's verification pass.
- **2026-06-13 modelling-side collection request (strategy-sensitive venue —
  details in the gitignored local request doc).** Triaged by the manager;
  status: (i) **decided 2026-06-13** — the perishable continuous WS order-book
  + reference-price capture was approved by the owner as a **local-only
  artifact** and is live on a per-user scheduled task since 2026-06-12
  ~23:42 UTC (build, smoke test, and live verification recorded in the local
  request doc; runs only while the owner is logged on — folds into the pending
  service-conversion decision there). (ii) Still pending: a low-volume daily
  external data sweep (dual-source cross-checked, re-fetchable, no
  urgency) — needs go/no-go + the same placement call. A third requested item
  turned out already satisfied by live capture, and a fourth is moot after the
  continuous Kalshi switch; both recorded in the request doc.

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
