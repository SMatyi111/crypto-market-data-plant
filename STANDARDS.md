# Data Standards

`STANDARDS_VERSION = 1`

This is the contract for what the crypto market-data plant produces and what
"replayable / research-ready" means. It describes the system **as it actually
behaves today**, not the aspiration — anything not yet implemented is called out
under **Roadmap** so downstream tools don't key off guarantees that don't exist.

If you change a schema, a partition layout, or the definition of "replayable",
bump `STANDARDS_VERSION` and update this file in the same change.

---

## 1. Datasets

Two normalized datasets, each collected per (venue, instrument) lane:

| Dataset  | Channel  | Normalizer(s)                                  | Curated target            |
| -------- | -------- | ---------------------------------------------- | ------------------------- |
| `depth`  | order book diffs | `BinanceDepthNormalizer`               | `market_replayable`       |
| `trades` | trade prints     | `BinanceTradeNormalizer`, `CoinbaseTradeNormalizer` | `trades_replayable` |

Venues live today: **Binance** (depth + trades), **Coinbase** (trades).
See Roadmap for the rest.

---

## 2. On-disk layout

### 2.1 Raw + per-run working set

A collector "run" is one segment directory:

```
<output_root>/<source>/<YYYYMMDD_HHMMSS>/
  raw/messages.jsonl[.N]    # every WS frame, append-only, fsync per line, size-rotated
  clean/events.jsonl        # normalized events that passed the live quality gate
  quarantine/events.jsonl   # normalized events that failed, each with a "reasons" list
  metrics/summary.jsonl     # streamed run metrics (partial rows + a final row)
  metrics/replay_summary.json  # the curation verdict (see §4)
```

`<source>` is the lane directory:

- `binance_depth`, `binance_trades`, `coinbase_trades`
- Per-instrument lanes append a sanitized suffix: `binance_trades_ethusdt`, etc.
  (`--source-suffix`; empty preserves the legacy single-symbol layout).

The `YYYYMMDD_HHMMSS` prefix is the run's start time. The first 8 chars
(`YYYYMMDD`) are the **run day** used by the manifest. With
`--rotate-at-midnight`, a run ends at the UTC day boundary so a run dir never
straddles two days.

**Durability:** raw JSONL is fsync'd per line, so a hard kill / power loss never
leaves a torn line — raw is the source of truth you can always rebuild from. The
normalized Parquet layer buffers up to `batch_size` (100) rows in memory, so on a
hard kill it can briefly lag raw by up to ~100 events. Rebuild normalized from raw
if they disagree.

### 2.2 Normalized Parquet (all runs, pre-curation)

```
<archive>/normalized/{market,trades}/
  schema_version=v1/source=<src>/event_date=<YYYY-MM-DD>/part-*.parquet
```

(`market` holds depth.) Written live as the run collects. Includes both clean and
quarantined-eligible rows? No — only clean events are written here.

### 2.3 Curated Parquet (replayable only)

```
<archive>/curated/research/{market_replayable,trades_replayable}/
  schema_version=v1/source=<src>/event_date=<YYYY-MM-DD>/part-*.parquet
  _promotion_index.jsonl
```

Only runs whose `replay_summary.json` says `replayable: true` are promoted here.
**This is what an analyst should read.** `_promotion_index.jsonl` records each
promoted run (`run_path`, `promoted_rows`, `promoted_at`).

### 2.4 Quarantine

```
<archive>/quarantine/market/<src>/...
  _quarantine_index.jsonl
```

Runs that fail replay are moved here so they're out of the promotion path but not
deleted (forensics).

### 2.5 Manifest

```
<archive>/curated/research/manifests/
  research_manifest_latest.json
  research_manifest_latest.md
  research_manifest_<ts>.json   # immutable snapshots
```

See §6.

> **Partition note:** the live partition key set is
> `schema_version / source / event_date`. There is **no `instrument` partition
> column yet** — a lane's instrument is encoded in the `<source>` directory
> suffix, not a partition. Per-instrument partitioning is Roadmap.

---

## 3. Event schemas

JSON keys are stable; Parquet columns mirror them (plus the partition columns
`schema_version`, `source`, `event_date`). `None`/null fields are dropped from
Parquet rows.

### 3.1 `depth` event (`NormalizedDepthUpdate`)

| Field             | Type            | Notes |
| ----------------- | --------------- | ----- |
| `source`          | str             | venue, e.g. `binance` |
| `product`         | str             | venue symbol, e.g. `BTCUSDT` |
| `channel`         | str             | `depth` |
| `event_type`      | str             | `depthUpdate` / `snapshot` |
| `event_time`      | ISO-8601 \| null | exchange event time (UTC) |
| `received_at`     | ISO-8601        | collector receipt time (UTC) |
| `first_update_id` | int \| null     | Binance `U` (sequence window start) |
| `final_update_id` | int \| null     | Binance `u` (sequence window end) |
| `instrument`      | object \| null  | resolved `InstrumentRef` (id, venue, canonical symbol, assets) |
| `bids` / `asks`   | list[[price, size]] | floats; size `0` = remove level |
| `metadata`        | object          | includes `parse_errors` when present |

### 3.2 `trades` event (`NormalizedL3Event`)

| Field          | Type            | Notes |
| -------------- | --------------- | ----- |
| `source`       | str             | venue |
| `product`      | str             | venue symbol (`BTCUSDT`, `BTC-USD`) |
| `channel`      | str             | `trades` |
| `event_type`   | str             | `trade` / `match` / `last_match` / `aggTrade` |
| `exchange_time`| ISO-8601 \| null | trade time (UTC) |
| `received_at`  | ISO-8601        | collector receipt time (UTC) |
| `side`         | `buy`/`sell`/null | **aggressor (taker) side**, normalized across venues |
| `price`        | float \| null   |  |
| `size`         | float \| null   |  |
| `trade_id`     | str \| null     | venue trade id |
| `sequence`     | int \| null     | dense per-stream counter used for gap detection (= trade_id where dense) |
| `metadata`     | object          | `buyer_is_maker`, `instrument_id`, `canonical_symbol`, venue extras, `parse_errors` |

**Cross-venue `side` convention:** `side` is always the *aggressor* side. Binance
gives `buyer_is_maker` (maker buy → taker sell). Coinbase gives the *maker* order
side, so the normalizer flips it (maker sell → taker buy). The raw venue value is
kept in `metadata` (`buyer_is_maker` / `maker_side`).

---

## 4. "Replayable" — the curation verdict

Every run writes `metrics/replay_summary.json` with at least
`{ "replayable": bool, "findings": [str] }`. Promotion keys off `replayable`.
The exact bar depends on what the feed lets us prove.

### 4.1 `depth` (sequence-bearing — Binance)

`replayable` iff **all** hold:

- `event_count > 0`
- exactly one snapshot anchor, and the update windows bridge it
  (`U`/`u` contiguity over `lastUpdateId`) — no `gaps_detected`, no
  `snapshot_anchor_gap`
- no `reordered_or_duplicate_updates`
- no `invalid_update_ranges` (`u >= U`)
- no `crossed_book_states`

This is a **strong, provable gaplessness** guarantee: the book can be
reconstructed exactly from the snapshot + diffs.

### 4.2 `trades` (sequence-bearing — Binance, Coinbase)

`replayable` iff **all** hold:

- `event_count > 0`
- `trade_id` (via `sequence`) is monotonic non-decreasing
- no `trade_id` gaps (a dense per-product counter; `delta > 1` ⇒ dropped trades)
- `price` and `size` are finite and positive
- exchange→receipt clock skew within `--max-clock-skew-ms` (default 60 s)

Findings: `non_monotonic_trade_ids`, `trade_id_gaps`, `invalid_prices`,
`invalid_sizes`, `excessive_clock_skew`, `no_events`.

### 4.3 Gap policy for non-sequence feeds (Roadmap venues)

Some feeds carry **no per-message sequence number** (Coinbase public `level2`,
Kraken `book` v1). Gaplessness is **not provable** from these streams alone.

Policy for any such adapter:

- Its replay summary MUST set `gap_detection: "none_native"`.
- `replayable` is **downgraded** to mean *structurally clean only*: a single
  snapshot, parse-clean events, monotonic timestamps — **not** gap-proof.
- Consumers MUST treat `gap_detection == "none_native"` data as best-effort and
  not assume completeness.
- When gaplessness matters, prefer a feed that exposes a connection- or
  message-level sequence (Coinbase Advanced Trade `sequence_num`, Kraken v2,
  Bybit `u`/`seq`) so the strong guarantee in §4.1/§4.2 applies.

No `none_native` adapter exists yet; this fixes the contract before one is built.

---

## 5. Live quality gate (pre-replay)

Applied per event during collection; failures go to `quarantine/events.jsonl`
with a `reasons` list and are excluded from the normalized/clean stream:

`parse_errors` (any), `invalid_side`, `non_positive_price`, `negative_size`,
`stale_or_clock_skew` (delay `> max_delay_ms` or `< -max_future_skew_ms`),
`non_monotonic_sequence` (strictly decreasing `sequence`), `unknown_event_type`.

The gate is a fast online filter; §4 replay is the authoritative, whole-run
verdict that gates promotion.

---

## 6. Consuming the data

1. **Read curated, not raw.** Pull from
   `curated/research/{market_replayable,trades_replayable}/schema_version=v1/source=<src>/event_date=<date>/`.
   Everything there passed §4.
2. **Check the manifest first.** `research_manifest_latest.json` lists each day
   with a `readiness`:
   - `ready` — promoted rows present, no bad raw runs that day
   - `ready_with_quarantine` — promoted rows present, but some raw runs were
     unreplayable / missing summaries
   - `building` — the current UTC day, still collecting
   - `missing` — nothing promoted
3. **Pin `schema_version`** in your reader so a future bump doesn't silently
   change columns under you.

> **Current limitation:** the manifest is Binance-depth-centric (single global
> day timeline keyed off `raw/market/binance_depth` + `market_replayable`
> promotion). Trades and non-Binance venues are partially counted but not yet
> first-class. Per-(venue, instrument, dataset) readiness is Roadmap (Phase 2 #4).

---

## 7. Retention

- **Raw** (`raw/market/<src>`): default **14 days**, overridable per dataset via
  the cleanup job's `raw_policy` (`market/<src>=<days>`). Raw is the rebuild
  source; keep it long enough to re-promote after a logic fix.
- **Normalized / curated / quarantine / manifests**: retained indefinitely (no
  auto-prune today). Curated is the long-lived research artifact.
- Cleanup runs in **dry-run by default** (`--apply` to act).

---

## 8. Roadmap (NOT guaranteed yet)

These are aspirations in `FOLLOW_UPS.md`, listed so nobody mistakes them for the
current contract:

- `instrument=` partition column in the curated/normalized datasets.
- Manifest keyed by `(venue, instrument, dataset, event_date)` covering trades +
  all venues, tagged with `standards_version`.
- Non-sequence depth adapters (Coinbase `level2`, Kraken `book`) under the §4.3
  policy; sequence-bearing depth for Coinbase Advanced Trade / Bybit / Kraken v2.
- Continuous day-bounded rotation as the default run model (vs. count-bounded).
