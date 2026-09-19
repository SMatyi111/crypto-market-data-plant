# Active lanes and research-grade status (snapshot 2026-09-19)

Owner directive 2026-09-09 was **stabilize this set; no new lanes**; the owner
SUPERSEDED it on 2026-09-17 ("there is nothing to stabilize if nothing is
complete") and approved the ETH/SOL completeness build in lanes 34-43 below.
44 enabled lanes from `ops.live.local.json` once the tier 5 job is staged after PR #89
merges (43 until then; 48 configured; `binance-perp-liquidations`
disabled because fstream delivers no frames from this host; Kalshi lanes off
since the 2026-06-17 disk incident). "Grade" is what a research consumer can
assume if the plant is left exactly as it is. The curated tier was repaired
end to end on 2026-09-08 (trades) and 2026-09-10 (depth), so the full history
counts. Caveats that apply to every lane are listed after the tables.

## By category — what a reader can rely on, and for how long

"Stable since" = the date from which the lane has run in its current form with
curated data complete against raw. History lengths are as of 2026-09-10.

| Category | Lanes | Research grade | Stable since | History | Caveats |
| --- | --- | --- | --- | --- | --- |
| BTC spot trades | 7 books, 6 venues | **A** Binance, Coinbase, Kraken (gap-proof ids); **B** Bybit, MEXC, OKX | 2026-06-08 | 94 d | dedupe at read; 5–8 s per 30-min segment; holes 06-17..24, 07-05, 07-11 |
| BTC spot depth | 7 books | **A** Binance, Coinbase, Kraken, Bybit, OKX; **B** MEXC | 2026-06-08 | 94 d | Binance pre-06-09 partitions lack the synthesized snapshot row |
| BTC perp trades | Bybit, OKX (WS); Binance (REST 1 s) | **A** Binance (gap-proof, ~1 s late); **B** Bybit, OKX | 2026-06-09 | 93 d | Binance latency from polling |
| BTC perp depth | Bybit, OKX (WS); Binance (REST 2 s) | **A** Bybit, OKX; **B** Binance (2 s snapshots) | 2026-06-09 | 93 d | |
| Funding / mark / index | Binance BTC | **A** | 2026-06-09 | 93 d | one orphan run per restart, accounted |
| Open interest | Binance BTC, ETH, SOL | **A** | 2026-09-08 (v11 per-symbol) | 2 d clean; 08-25..09-08 quarantined | daily 5-min history 2020→ outside the plant (`G:\03-reference-data\binance_futures_metrics`) |
| Liquidations | Bybit BTC/ETH/SOL; OKX all swaps | **B** Bybit (clean, not gap-proof); **B** OKX (v12 receipt-ordered verdict, deployed + history re-scored 2026-09-10) | Bybit 2026-09-08; OKX 2026-09-06 (hot history) | 2 d / 4 d hot + cold since 08-25 | raw only; OKX: use `received_at` and per-product order (venue-delayed batches); details later than 1 h are quarantined by the live gate (15 min before 2026-09-10 15:06) |
| Options snapshots | Binance BTC/ETH chain 15 min; Deribit BTC/ETH 5 min | **R** reference | 2026-09-01 in the plant | 9 d here + V1 series since 2026-05 in `G:\Binance_IV_V1` | missed snapshots permanent; V1 Deribit gap 08-11..27 |
| Hyperliquid wallet flow | 10 frozen wallets, BTC/ETH/SOL | **A** (prospective) | 2026-08-09 | 32 d | 60 s poll; frozen cohort. **Audited against the chain 2026-09-14:** 39,852 of 322,529 target fills (12.4 %) were missing (wallet-9 stall fixed in PR #82; wallets 1/3/4/6 lost fills by another path) and were backfilled from `node_fills_by_block` with `raw_type=node_fills_by_block`, `received_at` 2026-09-14T17:31Z - receipt-time evaluations must exclude those rows (STANDARDS 4.7). Completeness is now measurable: `backfill-wallet-flow-from-node` dry-run |
| Hyperliquid leaderboard | daily | **R** | 2026-08-17 | 24 d | point-in-time reference |
| Hyperliquid universe positions | hourly ingest of the ladder study's sweeps | **R** reference | 2026-09-15 (sweep series; lane live at the next redeploy after 2026-09-19) | 97 sweeps on 09-19 | the study's derived rows, not venue bytes; coverage 83-95 % of OI; series ends with the study (2027-03-31) unless extended |
| Text | RSS, 5 feeds | **A** for what it is | 2026-07-16 | 56 d, ~74 items/day | thin; `ingestion_ts` is the clock |

Pre-2026-06-08 coverage exists only as the read-only `D:\market_archive`, not
merged (ROADMAP open item 1).

## Per lane

| # | Lane | Venue | Instrument | Data | Capture | Curated target | Grade |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | binance-btc-trades | Binance spot | BTCUSDT | trades | WS, 30-min segments | trades_replayable | **A** gap-proof (dense trade ids) |
| 2 | binance-btc-depth | Binance spot | BTCUSDT | depth | WS | market_replayable | **A** sequence-proven book; pre-2026-06-09 partitions lack the synthesized snapshot row |
| 3 | binance-btcusdc-trades | Binance spot | BTCUSDC | trades | WS | trades_replayable | **A** |
| 4 | binance-btcusdc-depth | Binance spot | BTCUSDC | depth | WS | market_replayable | **A** |
| 5 | coinbase-btc-trades | Coinbase | BTC-USD | trades | WS | trades_replayable | **A** gap-proof; one `last_match` duplicate per segment boundary before the fix |
| 6 | coinbase-btc-depth | Coinbase | BTC-USD | depth | WS | market_replayable | **A** multi-anchor snapshot replay |
| 7 | kraken-btc-trades | Kraken | BTC/USD | trades | WS | trades_replayable | **A** gap-proof; up to ~50 subscribe-replay duplicates per boundary, dedupe by trade_id |
| 8 | kraken-btc-depth | Kraken | BTC/USD | depth | WS | market_replayable | **A** checksum-verified book (BTC/USD precision table only) |
| 9 | bybit-btc-trades | Bybit spot | BTCUSDT | trades | WS | trades_replayable | **B** `none_native`: structurally clean, not gap-proof (UUID ids) |
| 10 | bybit-btc-depth | Bybit spot | BTCUSDT | depth | WS | market_replayable | **A** `u` sequence chained |
| 11 | mexc-btc-trades | MEXC | BTCUSDT | trades | WS | trades_replayable | **B** `none_native`, no trade id at all |
| 12 | mexc-btc-depth | MEXC | BTCUSDT | depth | WS | market_replayable | **B** `none_native` (version captured, not yet proven dense) |
| 13 | okx-btc-trades | OKX spot | BTC-USDT | trades | WS | trades_replayable | **B** `none_native` |
| 14 | okx-btc-depth | OKX spot | BTC-USDT | depth | WS | market_replayable | **A** sequence + checksum |
| 15 | bybit-perp-trades | Bybit linear perp | BTCUSDT | trades | WS | trades_replayable | **B** `none_native`; lowest-latency perp print feed |
| 16 | bybit-perp-depth | Bybit linear perp | BTCUSDT | depth | WS | market_replayable | **A** |
| 17 | okx-perp-trades | OKX swap | BTC-USDT-SWAP | trades | WS | trades_replayable | **B** `none_native` |
| 18 | okx-perp-depth | OKX swap | BTC-USDT-SWAP | depth | WS | market_replayable | **A** |
| 19 | binance-futures-rest-trades | Binance USDT-M perp | BTCUSDT | aggTrades | REST 1 s poll | trades_replayable | **A** gap-proof ids, but `received_at` lags the venue by the poll (0.5–1.5 s) |
| 20 | binance-futures-rest-depth | Binance USDT-M perp | BTCUSDT | depth | REST 2 s poll | market_replayable | **B** 2-second snapshots, not a stream |
| 21 | binance-futures-rest-funding | Binance USDT-M perp | BTCUSDT | funding, mark, index | REST 5 s poll | funding | **A** inline summary, one orphan run per worker restart (accounted) |
| 22 | binance-btc-open-interest | Binance USDT-M perp | BTCUSDT | open interest | REST 60 s poll | open_interest | **A since 2026-09-08** (v11 per-symbol dirs); 2026-08-25..09-08 legacy dir quarantined; daily 5-min history 2020→ lives outside the plant (Vision zips) |
| 23 | binance-eth-open-interest | Binance USDT-M perp | ETHUSDT | open interest | REST 60 s poll | open_interest | **A since 2026-09-08** |
| 24 | binance-sol-open-interest | Binance USDT-M perp | SOLUSDT | open interest | REST 60 s poll | open_interest | **A since 2026-09-08** |
| 25 | bybit-btc-liquidations | Bybit linear perp | BTCUSDT | liquidations | WS, day segments | raw only | **B** structurally clean day runs since 2026-09-08; `none_native`; 2026-08-25..09-08 shared-dir runs mixed-symbol |
| 26 | bybit-eth-liquidations | Bybit linear perp | ETHUSDT | liquidations | WS, day segments | raw only | **B** as above |
| 27 | bybit-sol-liquidations | Bybit linear perp | SOLUSDT | liquidations | WS, day segments | raw only | **B** as above |
| 28 | okx-swap-liquidations | OKX | all swaps | liquidations | WS, day segments | raw only | **B** `none_native`, structurally clean under the v12 receipt-ordered verdict (STANDARDS 4.10, deployed 2026-09-10; 23 finished hot runs re-scored, all replayable, venue lag and per-product reorders on record as informational findings). Use `received_at` and per-product order; the live gate quarantines details later than 1 h since 2026-09-10 15:06 (15 min before; v13); pre-09-06 runs are on the cold tier with pre-v12 summaries |
| 29 | binance-options-chain-snapshot | Binance eapi | BTC, ETH chains | options chain | REST every 15 min | raw only | **R** reference snapshots, not a book; occasional missed snapshots are permanent |
| 30 | deribit-options-snapshot | Deribit | BTC, ETH | option + future summaries | REST every 5 min | raw only | **R** reference snapshots; 17.8-min gap 2026-09-08 |
| 31 | hyperliquid-wallet-flow | Hyperliquid | 10 frozen wallets, BTC/ETH/SOL | fills | REST 60 s poll | trades_replayable | **A** per-wallet ordering (v9), availability = receipt; frozen cohort, prospective from 2026-08-09 |
| 32 | hyperliquid-leaderboard-snapshot | Hyperliquid | all wallets | leaderboard | REST daily | raw only | **R** daily point-in-time reference since 2026-08-17 |
| 33 | text-rss | 5 news feeds | — | articles + edits | HTTP 120 s poll | text | **A** for what it is: ~74 items/day, `ingestion_ts` is the clock, edits kept |
| 34 | binance-eth-funding | Binance USDT-M perp | ETHUSDT | funding, mark, index | REST 5 s poll | funding | **A** live 2026-09-17 20:48Z |
| 35 | binance-sol-funding | Binance USDT-M perp | SOLUSDT | funding, mark, index | REST 5 s poll | funding | **A** live 2026-09-17 20:48Z |
| 36 | bybit-eth-perp-trades | Bybit linear perp | ETHUSDT | trades | WS | trades_replayable | **B** `none_native` |
| 37 | bybit-sol-perp-trades | Bybit linear perp | SOLUSDT | trades | WS | trades_replayable | **B** `none_native` |
| 38 | bybit-eth-perp-depth | Bybit linear perp | ETHUSDT | depth | WS | market_replayable | **A** `u` sequence chained |
| 39 | bybit-sol-perp-depth | Bybit linear perp | SOLUSDT | depth | WS | market_replayable | **A** `u` sequence chained |
| 40 | okx-eth-perp-trades | OKX swap | ETH-USDT-SWAP | trades | WS | trades_replayable | **B** `none_native` |
| 41 | okx-sol-perp-trades | OKX swap | SOL-USDT-SWAP | trades | WS | trades_replayable | **B** `none_native` |
| 42 | okx-eth-perp-depth | OKX swap | ETH-USDT-SWAP | depth | WS | market_replayable | **A** sequence + checksum |
| 43 | okx-sol-perp-depth | OKX swap | SOL-USDT-SWAP | depth | WS | market_replayable | **A** sequence + checksum |
| 44 | hyperliquid-universe-positions-snapshot | Hyperliquid (via hl-liquidation-ladder sweep) | ~22k wallets, BTC/ETH/SOL positions | positioning (szi, entry, liq px, leverage, margin, account value) | hourly INGEST of the sweep Parquet, sha256-verified vs its manifest | raw only | **R** reference (STANDARDS 4.11); tier 5, owner 2026-09-19; configured, live at the next redeploy |

## 2026-09-17/18 — the ETH/SOL completeness build (lanes 34-43)

The set had the **cause without the effect**. ETH and SOL liquidations (lanes 26-27)
and open interest (lanes 23-24) have been recorded since 2026-09-08, while all 14
market-data lanes were BTC. So the return response to an ETH liquidation was not
computable, and neither was dOI against return for two of the three symbols.

Worse, liquidations trigger off **mark price**, which was collected for BTC only -
the trigger variable for the ETH/SOL liquidations already on disk did not exist.
Lanes 34-35 close that first and cost under 0.1 GB/day; lanes 36-43 give both symbols
a price series on the same venues whose liquidations are already recorded, so an event
study shares the venue, the book and the clock.

**Status 2026-09-18: lanes 34-35 are LIVE (since 2026-09-17 20:48Z). Lanes 36-43
are CONFIGURED but NOT YET COLLECTING** - they start at the next runner redeploy,
because the ops runner reads its config only at startup. Until then they have no
raw dirs, and `archive-offload-cold` logs an hourly `missing_lane_dir` warning for
each; that is expected and clears on redeploy.

Grades above were inherited from the BTC lane of the same venue+type when the lanes
went in, and were **confirmed by the first curated completeness check on 2026-09-19**
(readiness check for the multi-venue cascade study): every replayable raw run of all ten
lanes since the 2026-09-18 11:11Z redeploy is in curated, raw replayable events =
curated rows exactly (ratio 1.0000 on each lane: Bybit ETH/SOL trades 3,534,748 /
976,728, depth 3,512,256 / 3,418,137; OKX ETH/SOL trades 2,393,529 / 440,958, depth
1,053,180 / 1,046,729; Binance ETH/SOL funding 29,676 / 29,673), zero quarantined
events, zero non-replayable runs (61-65 scored runs per lane, only the open live run
unscored). The **I/O-ceiling re-test passed** in the same pass: across all 26
websocket/REST market lanes the post-redeploy quarantine ratio is 0.0000 except
`coinbase_trades` at 0.0001 (62 events), and the only non-replayable runs are the
pre-existing Coinbase/Kraken `trade_id_gaps` pattern (3-8 runs/day since before the
build) plus one Bybit BTC perp `excessive_clock_skew` run - no `high_quarantine_ratio`
finding anywhere, so the 2026-06-08 slow-drive failure did not resurface. History
starts 2026-09-17 20:48Z for lanes 34-35 and 2026-09-18 11:11Z for lanes 36-43; there
is no earlier ETH/SOL market data in the plant and none can be obtained retrospectively.

**Consumer caveat found in the same check - Bybit BTC trades:** curated
`trades_replayable/source=bybit/instrument=BTCUSDT` holds BOTH the spot lane
(`bybit_trades`) and the linear-perp lane (`bybit_perp_trades`) in one partition
(2026-09-18: 2,583,145 perp + 762,498 spot rows) with no distinguishing column other
than `source_run_path`. Raw never mixes (STANDARDS 4.x), but the curated partition key
does; readers must split on `source_run_path` until the partition layout is fixed
(ROADMAP open item). ETH/SOL are unaffected (no Bybit spot lane), OKX is unaffected
(`BTC-USDT` vs `BTC-USDT-SWAP`), and `market_replayable` is unaffected (`BTC-USDT` vs
`BTC-USDT-PERP`).

Capacity note: the cold tier moved G: -> `I:\market_archive_cold` (KNOWLEDGE-DEEP-A,
mirrored to J:) on 2026-09-17, taking runway from ~9 days to ~389 at the post-expansion
rate. Concurrency cap went 37 -> 45 in both runner scripts.

Grades: **A** replay-validated, curated complete vs raw, usable as-is with the
caveats below. **B** captured and structurally validated but not gap-proof
(`none_native`), or coarse cadence. **C** captured, verdict/contract unresolved.
**R** raw-only reference data, not replayable.

## Caveats that apply to everything

- **Dedupe at read time** by `(source, product, trade_id)`; where no id exists
  (MEXC) by `(exchange_time, price, size, side)`.
- **Availability clock is `received_at`**, never `exchange_time`.
- **Segment rotation** loses ~5–8 s per 30 minutes on WebSocket lanes.
- **Incident holes:** 2026-06-17..24 (disk full), 2026-07-05 11:00–17:00Z
  (all venues), 2026-07-11 07:00–15:50Z (Binance REST lanes).
- **Curated completeness:** trades lanes repaired 2026-09-08 to ≥ 99.93 % of raw;
  depth lanes repaired 2026-09-09/10 (see ROADMAP); the manifest's "ready" label
  still reports standards v10 and is not a completeness certificate.
- **Coverage starts 2026-06-08/10** for market lanes; earlier history is the
  read-only `D:\market_archive` (not merged).
