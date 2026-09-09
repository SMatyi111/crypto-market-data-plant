# Active lanes and research-grade status (snapshot 2026-09-09)

Owner directive 2026-09-09: **stabilize this set; no new lanes.** 33 enabled
lanes from `ops.live.local.json` (34 configured; `binance-perp-liquidations`
disabled because fstream delivers no frames from this host; Kalshi lanes off
since the 2026-06-17 disk incident). "Grade" is what a research consumer can
assume if the plant is left exactly as it is, once the 2026-09-09 depth repair
has finished. Caveats that apply to every lane are listed after the table.

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
| 28 | okx-swap-liquidations | OKX | all swaps | liquidations | WS, day segments | raw only | **C** capture-complete but the replay verdict is wrong for this feed (venue-delayed batches, 340 products); use `received_at` and per-product order; STANDARDS v12 proposal pending |
| 29 | binance-options-chain-snapshot | Binance eapi | BTC, ETH chains | options chain | REST every 15 min | raw only | **R** reference snapshots, not a book; occasional missed snapshots are permanent |
| 30 | deribit-options-snapshot | Deribit | BTC, ETH | option + future summaries | REST every 5 min | raw only | **R** reference snapshots; 17.8-min gap 2026-09-08 |
| 31 | hyperliquid-wallet-flow | Hyperliquid | 10 frozen wallets, BTC/ETH/SOL | fills | REST 60 s poll | trades_replayable | **A** per-wallet ordering (v9), availability = receipt; frozen cohort, prospective from 2026-08-09 |
| 32 | hyperliquid-leaderboard-snapshot | Hyperliquid | all wallets | leaderboard | REST daily | raw only | **R** daily point-in-time reference since 2026-08-17 |
| 33 | text-rss | 5 news feeds | — | articles + edits | HTTP 120 s poll | text | **A** for what it is: ~74 items/day, `ingestion_ts` is the clock, edits kept |

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
