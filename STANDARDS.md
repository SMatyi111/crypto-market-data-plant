# Data Standards

`STANDARDS_VERSION = 3`

> **v3 (2026-06-01):** Kraken `book` depth moved from `none_native` to a provable
> `checksum` guarantee — its per-frame CRC32 is now validated against the
> reconstructed top-10 book (BTC/USD), so a dropped/corrupted update is detectable
> and blocks promotion.
> **v2 (2026-06-01):** Bybit spot `orderbook` depth moved from `none_native` to a
> provable `sequence` guarantee — its `data.u` increments by exactly 1 per message
> (verified live), so dropped messages are now detectable and block promotion.

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
| `depth`  | order book diffs | `BinanceDepthNormalizer`, `CoinbaseDepthNormalizer`, `BybitDepthNormalizer`, `KrakenDepthNormalizer` | `market_replayable`  |
| `trades` | trade prints     | `BinanceTradeNormalizer`, `CoinbaseTradeNormalizer`, `KrakenTradeNormalizer`, `BybitTradeNormalizer` | `trades_replayable` |

Venues live today: **Binance** (depth + trades), **Coinbase** (trades + depth),
**Kraken** (trades + depth), **Bybit** (trades + depth). See Roadmap for the rest.

> **Gap-detection class differs by feed, not just by venue.** Three classes, tagged
> per lane as `gap_detection` in the manifest (§6):
>
> - **`sequence`** — a dense per-message counter proves gaplessness (§4.1/§4.2):
>   Binance depth/trades, Coinbase trades, **Kraken trades** (`trade_id` is a dense
>   per-pair counter), and **Bybit spot `orderbook` depth** (`data.u` increments by
>   exactly 1 per message).
> - **`checksum`** — a per-frame CRC32 over the reconstructed book proves integrity,
>   so a dropped/corrupted update is caught (§4.3): **Kraken `book` depth**.
> - **`none_native`** — no usable integrity signal, so `replayable` means
>   *structurally clean*, **not** gap-proof: Coinbase depth (`level2_50`) and **Bybit
>   spot trades** (`publicTrade`, whose trade id is a UUID).
>
> `sequence` and `checksum` are both **provable** (consumers can rely on
> completeness); `none_native` is best-effort. Two lanes of the same dataset can
> have **different** classes (e.g. Bybit depth `sequence` vs. Kraken depth `checksum`
> vs. Coinbase depth `none_native`), so key off the tag, not the dataset name.

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
  `kraken_trades`, `bybit_trades`, `bybit_depth`, `kraken_depth`
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
| `event_type`      | str             | `snapshot` / `depthUpdate` (Binance) / `l2update` (Coinbase) / `delta` (Bybit) / `update` (Kraken) |
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
sequence at all (Coinbase `level2_50` depth; Kraken `book` v1) or
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

**Live adapter — Coinbase `depth` (`level2_50`).** The public channel is
`level2_50` (verified against the live socket 2026-05-31); the plain `level2` /
`level2_batch` channels now require Coinbase auth, so they are not used. The book
snapshot arrives **in-stream** (`event_type == "snapshot"`, full book in
`bids`/`asks` — ~1.4 MiB, which is why the collector raises the WS max frame size
above the 1 MiB library default; see `CollectorConfig.max_message_bytes`), with no
exchange time, instead of via REST; diff frames (`event_type == "l2update"`) carry
no `U`/`u`. The whole-run verdict (`replay_depth_stream_run`) sets
`gap_detection: "none_native"` and:

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

**Live adapter — Bybit spot `depth` (`orderbook.{depth}`) — `sequence` (not
none_native).** Bybit's spot orderbook stream sends a frame-level `type` of
`"snapshot"` (full book) then `"delta"` frames (changed levels only, size `"0"` =
remove). It shares the in-stream-snapshot replay machinery described in this
section, **but** its diff key (`data.u`) increments by **exactly 1 per message**
(snapshot included — verified against the live socket 2026-06-01, 60 consecutive
frames all `+1`). So `replay_depth_stream_run` is called with
`sequence_metadata_key="bybit_update_id"`, which upgrades the lane to a **provable
`sequence` guarantee** (§4.1 class): `mode: "stream_snapshot_sequence"`,
`gap_detection: "sequence"`. The update id and the (cross-symbol, non-dense) cross
sequence are both preserved in `metadata` (`bybit_update_id`,
`bybit_cross_sequence`). Exchange time prefers the matching-engine timestamp
(`cts`) and falls back to the frame timestamp (`ts`).

`replayable` iff **all** hold:

- `event_count > 0`
- exactly one snapshot anchor, and it is the **first** event in the run
- event timestamps are monotonic non-decreasing
- `data.u` advances by exactly 1 across every event

A `data.u` gap now **blocks** promotion (a dropped message means the book can't be
reconstructed exactly). Findings: `no_events`, `no_snapshot_anchor`,
`multiple_snapshot_anchors`, `snapshot_not_first_event`, `non_monotonic_event_time`,
`missing_update_id`, `update_id_gaps` (`delta > 1`), `non_monotonic_update_id`
(`delta <= 0` — reorder/reset), and (reported but non-gating) `crossed_book_states`.
A reconnect yields a second snapshot and ends the run unreplayable; the next
segment starts fresh.

**Live adapter — Kraken `depth` (`book`) — `checksum` (not none_native).** Kraken's
v2 `book` channel sends a frame-level `type` of `"snapshot"` then `"update"` frames;
`data` is a **list** (one entry per symbol), fanned out via `normalize_many`. Levels
are objects (`{"price", "qty"}`, `qty 0` = remove) flattened to `[[price, size]]`.
There is **no message sequence number**, but every frame carries a CRC32 `checksum`
over the top-10 book (preserved in `metadata.kraken_checksum`). For a pair whose
native `(price, qty)` decimal precision is known (`_KRAKEN_BOOK_PRECISION`; BTC/USD
= `(1, 8)`, from the REST `AssetPairs` `pair_decimals`/`lot_decimals`),
`replay_depth_stream_run` reconstructs the top-10 book from the stored levels and
**recomputes that CRC32 after every event**, requiring it to match — a
dropped/corrupted update diverges the local book and is caught. So a known-precision
pair is `gap_detection: "checksum"`, `mode: "stream_snapshot_checksum"` (a pair
absent from the table falls back to `none_native` — no false validation). The CRC32
spec (verified against the live socket 2026-06-01, snapshot + updates all reproduced):
asks top-10 ascending then bids top-10 descending, each level `price`@price-precision
+ `qty`@qty-precision with the decimal removed and leading zeros stripped. Only
`update` frames carry a `timestamp`; the snapshot has none and is skipped from the
monotonicity check.

`replayable` iff **all** hold:

- `event_count > 0`
- exactly one snapshot anchor, and it is the **first** event in the run
- event timestamps are monotonic non-decreasing
- every event carries a checksum and all match the reconstructed book

A checksum mismatch (or a missing checksum) now **blocks** promotion. Findings:
`no_events`, `no_snapshot_anchor`, `multiple_snapshot_anchors`,
`snapshot_not_first_event`, `non_monotonic_event_time`, `missing_checksum`,
`checksum_mismatch`, and (reported but non-gating) `crossed_book_states`.

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

> **Bybit keepalive.** Bybit drops idle public connections after ~10 min and
> expects a `{"op":"ping"}` roughly every 20 s. Both Bybit lanes opt into the
> collector's app-level keepalive (`CollectorConfig.ping_message` +
> `ping_interval_seconds=20`), which sends the ping on the open socket every
> interval, concurrently with the receive loop, so a low-volume symbol no longer
> ends its segment early on the idle drop. The pong reply carries no `topic`, so
> the data path drops it (`_should_emit`) and it can't be mistaken for the
> subscription ack. Every other venue leaves the keepalive off and relies on the
> `websockets` library's protocol-level ping/pong — the live Binance collector is
> unchanged.

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
- **Per-pair precision table for Kraken `depth` checksum validation.** Checksum
  validation is live for BTC/USD (`_KRAKEN_BOOK_PRECISION`); other Kraken pairs
  fall back to `none_native` until their `(price, qty)` precision is added (from
  the REST `AssetPairs` `pair_decimals`/`lot_decimals`). Could be auto-fetched at
  collect time instead of hardcoded.
- Continuous day-bounded rotation as the default run model (vs. count-bounded).
