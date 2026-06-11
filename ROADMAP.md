# Roadmap

**This file is the single source of truth for plans and open work.** It is
maintained by the project manager (Claude) and updated in every session that
changes scope or state. Companion docs:

- [`README.md`](README.md) — what the plant collects *today* (capability snapshot)
- [`STANDARDS.md`](STANDARDS.md) — the data contract (schemas, replayability, retention)
- [`docs/HISTORY.md`](docs/HISTORY.md) — resolved-work narrative (what was fixed, and why)

Last updated: **2026-06-11**.

---

## Current state (2026-06-11)

All green. 22 enabled collection lanes (21 collector workers + Kalshi quote
sampling) across Binance (spot USDT + USDC, USDT-M perp via REST), Coinbase,
Kraken, Bybit (spot + linear perp), MEXC, OKX (spot + linear perp), and Kalshi
crypto binaries — all BTC. Full quarantine → promote curation chain per lane,
hourly score catch-up self-heal, research manifest, cleanup retention, and
cold-tier archive offload. CI green on `main`; live runner restarted
2026-06-10 23:00 UTC on the current code (83 jobs).

---

## Dated operational checks

| Due | Check |
| --- | --- |
| **2026-06-22** | First `archive-offload` candidates reach offload age. Spot-check the lane `_offload_index.jsonl` entries against the `D:\market_archive_cold` tree: files verified-moved, counts match, no `unindexed` pile-up. |

**Last ops audit:** 2026-06-10 (redeploy verification: runner healthy, normalized
root landing on G:, all 21 collectors dispatched). Ritual: if this stamp is more
than ~3 days old at session start, audit the live plant first — see `CLAUDE.md`
"Quality gates & review protocol".

---

## Open work items (rough value order)

1. **Kalshi near-expiry burst sampling (requested 2026-06-11 — high leverage, small
   change).** For any hourly BTC (`KXBTC*`) market within 10 minutes of its
   `close_time`, poll its quote every **5–10 s**; cadence outside that window, lanes,
   storage layout, and fields all unchanged. Acceptance: a spot-check hour shows ≥30
   samples for the closing BTC hourly market in its final 10 minutes (currently a
   handful). Burst set is small (1–3 markets near close + neighbouring strikes);
   deprioritising far-from-expiry markets during bursts is acceptable. Full request
   context: `docs/kalshi_near_expiry_sampling_request.md` (local-only, untracked —
   modelling-side detail stays out of this public repo).
2. **D:\market_archive legacy history — decide retention or merge.** The pre-2026-06-08
   D: archive is kept read-only as history. Decide: backfill/merge its runs into the
   G: curated dataset (score with `backfill-trades-replay` / `backfill-stream-depth
   --score-only`, then let the promote jobs pick them up) or declare it cold history
   and leave it. Blocks nothing, but the disjoint pre-cutover data limits historical
   research coverage.
3. **Phase 6 candidate — inverse (coin-margined) BTCUSD perps.** Natural next
   instrument-expansion step after the linear-perp triangle. Note: Binance USDT-M
   *websocket* is jurisdiction-blocked from this box (REST works — see Constraints),
   so plan venue choice accordingly (Bybit/OKX inverse WS, or Binance dapi REST
   mirroring the fapi REST lanes).
4. **OKX funding channel.** Deferred from Phase 5. Would mirror the
   `binance-futures-rest-funding` lane (`funding-rate` channel or REST poll) so both
   perp venues carry funding context.
5. **MEXC depth → provable `sequence` upgrade.** The pushed `version` is already
   captured as `metadata.mexc_version`; if live frames prove it dense per symbol,
   upgrade the lane the way Bybit depth was upgraded (`data.u` +1). Until then depth
   stays `none_native`.
6. **Re-promote pre-fix Binance depth history (optional).** Binance depth partitions
   collected before commit `084f8c9` (2026-06-09) lack the leading synthesized
   `snapshot` row, so self-contained replay of those dates needs re-promotion from
   raw. Only matters if historical self-contained replay is wanted.
7. **Kraken checksum precision table for non-BTC/USD pairs.** `_KRAKEN_BOOK_PRECISION`
   covers BTC/USD only; other pairs fall back to `none_native`. Moot until a non-BTC
   Kraken pair is actually collected; could auto-fetch from REST `AssetPairs`.
8. **Day-bounded rotation as the default run model.** `--rotate-at-midnight` exists
   and works; the live model is 30-min wall-clock segments (`max_segment_seconds=1800`).
   Parked — analysts pull by `event_date` partition, so per-run boundaries rarely matter.
9. **Zero-gap segment rotation.** The ~5–8s WS reconnect between segments costs
   ~0.3–0.4% per segment. Eliminating it means separating connection lifecycle from
   file lifecycle in the collector core — a real refactor, parked unless that loss
   starts to matter.

## Decision queue (owner)

Decisions waiting on the owner; agents must not act on these without an explicit OK
(see `CLAUDE.md` Governance):

- **Housekeeping deletions** — the list below.
- **D:\market_archive legacy history** — retention vs. merge (open item 2 above).

### Housekeeping (pending deletion OK)

None of this is tracked in git (all gitignored); awaiting an explicit OK to delete:

- `ops.live.local.json.bak2` … `.bak5`/`.bak6` — superseded config snapshots (Jun 1–8).
- `screen-primary.png`, `screen-4k-command-result*.png` — debugging screenshots (~2.8 MB).
- `.tmp_research/` — 2026-05-30 Bybit/Kraken doc-page scrapes; findings long since
  encoded in `STANDARDS.md` and the normalizers.
- Merged remote branches on origin: `phase5-okx`, `archive-offload-cold-tier`,
  `normalized-root-threading`, `redeploy-alive-check-plant-scope`.

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
