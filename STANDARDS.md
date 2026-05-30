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
| `depth`  | order book diffs | `BinanceDepthNormalizer`, `CoinbaseDepthNormalizer` | `market_replayable`  |
| `trades` | trade prints     | `BinanceTradeNormalizer`, `CoinbaseTradeNormalizer`, `KrakenTradeNormalizer`, `BybitTradeNormalizer` | `trades_replayable` |

Venues live today: **Binance** (depth + trades), **Coinbase** (trades + depth),
**Kraken** (trades), **Bybit** (trades). See Roadmap for the rest.

> **Gap-detection class differs by feed, not just by venue.** Binance depth/trades,
> Coinbase trades, and **Kraken trades** are **sequence-bearing** (§4.1/§4.2 strong
> gaplessness — Kraken v2 `trade_id` is a dense per-pair counter). Coinbase depth
> (`level2` / `level2_batch`) and **Bybit spot trades** (`publicTrade`, whose trade
> id is a UUID, not a dense counter) are **non-sequence** (`none_native`, §4.3):
> `replayable` there means *structurally clean*, **not** gap-proof. The per-lane
> `gap_detection` tag in the manifest (§6) is how a consumer tells these apart —
> note that two `trades` lanes can have **different** classes (Kraken `sequence`
> vs. Bybit `none_native`), so key off the tag, not the dataset name.

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

- `binance_depth`, `binance_trades`, `coinbase_trades`, `coinbase_depth`,
  `kraken_trades`, `bybit_trades`
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
side, so the normalizer flips it (maker sell → taker buy). Kraken (`side`) and
Bybit (`S`, capitalized) already give the *taker* side, so no flip — just
case-normalized. `buyer_is_maker` is derived for every venue (taker sold ⇒ buyer
was maker) and the raw venue value is kept in `metadata`
(`buyer_is_maker` / `maker_side`).

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

This is a **strong, provable gaplessness** guarantee: the book can be
reconstructed exactly from the snapshot + diffs.

`crossed_book_states` (best bid ≥ best ask) is **reported as a finding for
visibility but does not by itself block promotion** — a transiently crossed book
can be a legitimate venue artifact, and the gaplessness proof above is what the
guarantee rests on. Consumers who require an uncrossed book at every tick should
check `crossed_book_count` in `replay_summary.json` themselves.

### 4.2 `trades` (sequence-bearing — Binance, Coinbase, Kraken)

`replayable` iff **all** hold:

- `event_count > 0`
- `trade_id` (via `sequence`) is monotonic non-decreasing
- no `trade_id` gaps (a dense per-product counter; `delta > 1` ⇒ dropped trades)
- `price` and `size` are finite and positive
- exchange→receipt clock skew within `--max-clock-skew-ms` (default 60 s)

Findings: `non_monotonic_trade_ids`, `trade_id_gaps`, `invalid_prices`,
`invalid_sizes`, `excessive_clock_skew`, `no_events`. Summary
`gap_detection: "sequence"`, written by `replay_trades_run`.

Kraken v2 `trade` joins this class: its `trade_id` is documented as "a sequence
number, unique per book" — a dense per-pair counter — so the same gap proof
applies. (Kraken batches several trades per WS frame; the pipeline fans them out
via `normalize_many`, but each event still carries its own dense `sequence`.)

### 4.3 Gap policy for non-sequence feeds (`none_native`)

Some feeds carry **no usable dense sequence number** — either no per-message
sequence at all (Coinbase `level2` / `level2_batch` depth; Kraken `book` v1) or
only an opaque/UUID id that can't prove `delta == 1` (Bybit spot `publicTrade`,
whose `i` is a UUID and `seq` is shared across batched messages). Gaplessness is
**not provable** from these streams alone.

Policy for any such adapter:

- Its replay summary MUST set `gap_detection: "none_native"`.
- `replayable` is **downgraded** to mean *structurally clean only*: a single
  snapshot, parse-clean events, monotonic timestamps — **not** gap-proof.
- Consumers MUST treat `gap_detection == "none_native"` data as best-effort and
  not assume completeness.
- When gaplessness matters, prefer a feed that exposes a connection- or
  message-level sequence (Coinbase Advanced Trade `sequence_num`, Kraken v2,
  Bybit `u`/`seq`) so the strong guarantee in §4.1/§4.2 applies.

**Live adapter — Coinbase `depth` (`level2` / `level2_batch`).** The book
snapshot arrives **in-stream** (`event_type == "snapshot"`, full book in
`bids`/`asks`, no exchange time) instead of via REST, and diff frames
(`event_type == "l2update"`) carry no `U`/`u`. The whole-run verdict
(`replay_depth_stream_run`) sets `gap_detection: "none_native"` and:

`replayable` iff **all** hold:

- `event_count > 0`
- exactly one snapshot anchor, and it is the **first** event in the run
- event timestamps are monotonic non-decreasing (the snapshot has no exchange
  time and is skipped from this check)

Findings: `no_events`, `no_snapshot_anchor`, `multiple_snapshot_anchors`,
`snapshot_not_first_event`, `non_monotonic_event_time`, and (reported but
non-gating, as in §4.1) `crossed_book_states`. A reconnect mid-run yields a
*second* in-stream snapshot, which trips `multiple_snapshot_anchors` and ends the
run unreplayable — mirroring Binance depth's single-anchor invariant, so the
worker's next segment simply starts a fresh book.

**Live adapter — Bybit spot `trades` (`publicTrade`).** Bybit batches many trades
per WS frame (`data: [...]`), fanned out via `normalize_many`. The per-trade id
(`i`) is a **UUID**, and the only other ordering field (`seq`, the cross sequence)
is shared across batched messages — neither supports `delta == 1` gap detection,
so `sequence` is left `None` and the whole-run verdict (`replay_trades_stream_run`)
sets `gap_detection: "none_native"` and `mode: "trade_stream_none_native"`.

`replayable` iff **all** hold:

- `event_count > 0`
- exchange timestamps are monotonic non-decreasing (the only ordering signal)
- `price` and `size` are finite and positive
- exchange→receipt clock skew within `--max-clock-skew-ms` (default 60 s)

There is **no** `trade_id` gap / monotonicity check. Findings: `no_events`,
`non_monotonic_event_time`, `invalid_prices`, `invalid_sizes`,
`excessive_clock_skew`. Promoted rows still land in `trades_replayable`, so a
consumer that needs provable completeness MUST gate on the lane's
`gap_detection == "sequence"` (Kraken/Binance/Coinbase trades), not just on the
`trades` dataset.

> **Bybit keepalive limitation.** Bybit drops idle public connections after
> ~10 min and expects a `{"op":"ping"}` roughly every 20 s. The collector does not
> send app-level pings today; an active trade stream stays alive on its own traffic
> so bounded segments complete fine, but a **low-volume** symbol can have its
> segment end early on a clean server close (the collector then reconnects / the
> worker starts a fresh segment — no data loss, just shorter runs).

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
2. **Check the manifest first.** `research_manifest_latest.json` is tagged with
   `standards_version` (matches `STANDARDS_VERSION` above) and carries two views:
   - `lanes` — the canonical per-`(venue, instrument, dataset)` readiness. Each
     lane is discovered from its raw lane directory
     (`<venue>_<dataset>[_<instrument>]`), carries a `gap_detection` tag
     (`sequence` = §4.1/§4.2 strong gaplessness; `none_native` = §4.3 best-effort),
     and lists per-`event_date` `readiness`. Readiness is driven by the curated
     promotion index (`run_path` → lane, accurate per instrument) and the lane's
     raw replay summaries — **not** the Parquet partitions, which are only
     venue-partitioned today (see the partition note in §2).
   - `days` — the legacy single global day timeline (Binance depth) kept for
     back-compat; prefer `lanes`.

   `readiness` values (same rule in both views):
   - `ready` — promoted rows present, no bad raw runs that day
   - `ready_with_quarantine` — promoted rows present, but some raw runs were
     unreplayable / missing summaries
   - `building` — the current UTC day, still collecting
   - `missing` — nothing promoted
3. **Pin `schema_version`** in your reader so a future bump doesn't silently
   change columns under you. Pin `standards_version` from the manifest too if you
   key off its shape.

> **Current limitation:** curated/normalized Parquet is partitioned by
> `source=<venue>` only, so per-instrument lanes of the same venue+dataset share
> Parquet partitions. The manifest's per-instrument readiness comes from the
> promotion index (which records the originating lane), not the Parquet layout.
> An `instrument=` partition column is still Roadmap (§8).

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

- `instrument=` partition column in the curated/normalized datasets (so
  per-instrument lanes of the same venue+dataset stop sharing Parquet
  partitions; the manifest already separates them via the promotion index).
- **Depth** for Kraken and Bybit (their trades lanes are already live): Bybit
  `orderbook.{depth}` (snapshot/delta keyed on `u`, snapshot-anchored — a
  middle-ground sequence) and Kraken v2 `book` (CRC32 checksum, no sequence — a
  §4.3 `none_native` feed integrity-checked by the checksum).
- App-level keepalive ping for Bybit (`{"op":"ping"}` ~20 s) so low-volume Bybit
  lanes don't end segments early on the ~10 min idle drop (see §4.3).
- Continuous day-bounded rotation as the default run model (vs. count-bounded).
