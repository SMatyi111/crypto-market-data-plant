# Data Standards

`STANDARDS_VERSION = 6`

> **v6 (2026-06-11):** catches the contract up to the lanes that went live 2026-06-09/10.
> (1) **OKX** spot + linear-perp lanes: `books` depth proves continuity via a **linked
> chain** — `prevSeqId(N) == seqId(N-1)`, validated by equality instead of `delta == 1`
> (`mode="stream_snapshot_chain"`) — and is tagged `gap_detection="sequence"` (provable;
> a broken link blocks promotion); OKX trades are `none_native` (§4.4). (2) **Binance
> USDT-M perp via REST polling**: aggTrades = `sequence` (dense aggregate-trade id,
> persisted resume cursor), depth = per-poll full-book snapshots (`none_native`), plus a
> new **`funding` dataset** (premiumIndex mark/index/funding-rate metric, `none_native`)
> curated to `curated/research/funding` (§4.5). (3) Perp lanes are tagged
> `perp:<venue>:<symbol>` and write to `<venue>_perp_<dataset>` lane directories.
> (4) MEXC verified against live frames and enabled. (5) Retention: aged raw runs are
> verify-moved to a cold tier by the `archive-offload` job (§7). No change to existing
> v2 Parquet schemas or partition layout.
> **v5 (2026-06-03):** in-stream-snapshot depth replay now handles **multiple snapshot
> anchors per run**. Stream-snapshot venues (Coinbase/Bybit/Kraken) re-snapshot mid-run
> by design (reconnect/resync); replay re-anchors at each `snapshot` and validates each
> sub-book independently instead of rejecting the run. The single-anchor requirement is
> replaced by "run must start with a snapshot" (`multiple_snapshot_anchors` /
> `snapshot_not_first_event` retired; new `run_does_not_start_with_snapshot`). Kraken
> additionally gets a **depth-bounded book** (`book_depth=10`): the worst level past the
> subscribed depth is evicted (Kraken drops it without a delete) so the CRC32 matches.
> This is what makes Coinbase/Bybit/Kraken depth promotable to curated. No on-disk
> schema or partition change — replay/curation semantics only.
> **v4 (2026-06-01):** normalized + curated Parquet gained an `instrument=` partition
> (the sanitized canonical symbol) via a Parquet `schema_version` `v1`→`v2` cutover —
> data is now pullable by `(venue, instrument, event_date)`. Existing `v1` data is
> untouched; new writes go to `schema_version=v2/source=…/instrument=…/event_date=…`.
> The resolved `InstrumentRef` detail moved to an `instrument_ref` column. (Note:
> `STANDARDS_VERSION` and the Parquet partition `schema_version` are different numbers
> — the latter is `v2`.)
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

Three normalized datasets, each collected per (venue, market, instrument) lane:

| Dataset  | Channel  | Normalizer(s)                                  | Curated target            |
| -------- | -------- | ---------------------------------------------- | ------------------------- |
| `depth`  | order book diffs / snapshots | `BinanceDepthNormalizer`, `CoinbaseDepthNormalizer`, `BybitDepthNormalizer`, `KrakenDepthNormalizer`, `MexcDepthNormalizer`, `OkxDepthNormalizer` (+ Binance USDT-M REST snapshot polling) | `market_replayable`  |
| `trades` | trade prints     | `BinanceTradeNormalizer`, `CoinbaseTradeNormalizer`, `KrakenTradeNormalizer`, `BybitTradeNormalizer`, `MexcTradeNormalizer`, `OkxTradeNormalizer` (+ Binance USDT-M REST aggTrades polling) | `trades_replayable` |
| `funding` | perp funding / mark-price metric | Binance USDT-M `premiumIndex` REST poll (native dict passthrough) | `funding` |

Venues live today: **Binance** (spot USDT + USDC depth + trades; USDT-M perp trades +
depth + funding via REST polling — §4.5), **Coinbase** (trades + depth), **Kraken**
(trades + depth), **Bybit** (spot + linear perp, trades + depth), **MEXC** (trades +
depth; the only **protobuf-transport** venue, verified against live frames 2026-06-09 —
see §4.3), **OKX** (spot + linear perp, trades + depth — §4.4). Perp lanes are tagged
`perp:<venue>:<symbol>` and write to their own `<venue>_perp_<dataset>` lane
directories, so perp never mixes with spot.

> **Gap-detection class differs by feed, not just by venue.** Three classes, tagged
> per lane as `gap_detection` in the manifest (§6):
>
> - **`sequence`** — a per-message id proves gaplessness, either as a dense counter
>   (§4.1/§4.2) or a linked chain (§4.4): Binance depth/trades (spot), Binance USDT-M
>   perp aggTrades (dense aggregate-trade id — §4.5), Coinbase trades, **Kraken
>   trades** (`trade_id` is a dense per-pair counter), **Bybit `orderbook` depth**
>   (`data.u` increments by exactly 1 per message; spot + linear), and **OKX `books`
>   depth** (`prevSeqId`/`seqId` linked chain, validated by equality — §4.4).
> - **`checksum`** — a per-frame CRC32 over the reconstructed book proves integrity,
>   so a dropped/corrupted update is caught (§4.3): **Kraken `book` depth**.
> - **`none_native`** — no usable integrity signal, so `replayable` means
>   *structurally clean*, **not** gap-proof: Coinbase depth (`level2_50`), **Bybit
>   trades** (`publicTrade`, whose trade id is a UUID; spot + linear), **OKX trades**,
>   **both MEXC lanes** (aggregated-deals trades carry no per-trade id; limit-depth
>   pushes independent full books whose `version` is metadata-only — see §4.3), and
>   the **Binance USDT-M perp REST `depth` + `funding`** lanes (per-poll snapshots /
>   metric rows — §4.5).
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
  raw/messages.jsonl[.N]    # every WS frame, append-only, flush per line / batched fsync, size-rotated
  clean/events.jsonl        # normalized events that passed the live quality gate
  quarantine/events.jsonl   # normalized events that failed, each with a "reasons" list
  metrics/summary.jsonl     # streamed run metrics (partial rows + a final row)
  metrics/replay_summary.json  # the curation verdict (see §4)
```

`<source>` is the lane directory:

- `binance_depth`, `binance_trades`, `coinbase_trades`, `coinbase_depth`,
  `kraken_trades`, `bybit_trades`, `bybit_depth`, `kraken_depth`, `mexc_trades`,
  `mexc_depth`
- Per-instrument lanes append a sanitized suffix: `binance_trades_ethusdt`, etc.
  (`--source-suffix`; empty preserves the legacy single-symbol layout).

The `YYYYMMDD_HHMMSS` prefix is the run's start time. The first 8 chars
(`YYYYMMDD`) are the **run day** used by the manifest. With
`--rotate-at-midnight`, a run ends at the UTC day boundary so a run dir never
straddles two days. A lane configured with the data-arrival watchdog
(`CollectorConfig.idle_timeout_seconds`) also ends a run cleanly if the feed goes
silent-but-connected — the run still finalizes (metrics + replay summary) and the
worker opens a fresh segment (see the watchdog note in §4).

**Durability:** raw/clean/quarantine JSONL is **flushed per line, fsynced in
batches** (default every 64 events or 200 ms, whichever first; per-event fsync
remains available per lane via `fsync_interval_events: 1`). Per-line flush means a
hard **process kill loses nothing** and never leaves a torn line. The weaker bound
is **power loss / OS crash**: up to one un-fsynced batch can vanish and the page
cache may persist a torn final line — readers that rebuild from raw must tolerate
a torn tail. (This batched posture replaced per-line fsync on 2026-06-09: per-event
fsync latency capped hot lanes below the feed rate, growing a received_at backlog
that tripped the clock-skew gate.) Raw remains the source of truth you rebuild
from. The normalized Parquet layer buffers up to `batch_size` (100) rows in
memory, so on a hard kill it can briefly lag raw by up to ~100 events. Rebuild
normalized from raw if they disagree.

### 2.2 Normalized Parquet (all runs, pre-curation)

```
<archive>/normalized/{market,trades}/
  schema_version=v2/source=<venue>/instrument=<canonical>/event_date=<YYYY-MM-DD>/part-*.parquet
```

(`market` holds depth.) Written live as the run collects; only clean events land
here. `<instrument>` is the **sanitized canonical symbol** (`BTC/USDT` → `BTC-USDT`),
falling back to the venue product then `unknown` when an instrument can't be
resolved. The resolved `InstrumentRef` detail is kept in an `instrument_ref` column.
Legacy `schema_version=v1` data (no `instrument=` level) predates the cutover and is
left in place — read both if you need history across the boundary.

### 2.3 Curated Parquet (replayable only)

```
<archive>/curated/research/{market_replayable,trades_replayable}/
  schema_version=v2/source=<venue>/instrument=<canonical>/event_date=<YYYY-MM-DD>/part-*.parquet
  _promotion_index.jsonl
```

Only runs whose `replay_summary.json` says `replayable: true` are promoted here.
**This is what an analyst should read** — pull by `(venue, instrument, event_date)`
straight off the path. `_promotion_index.jsonl` records each promoted run
(`run_path`, `promoted_rows`, `promoted_at`).

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

> **Partition note:** the v2 partition key set is
> `schema_version / source / instrument / event_date` — `instrument` is the
> sanitized canonical symbol, derived per row (canonical symbol → venue product →
> `unknown`). Legacy v1 data uses the 3-key `schema_version / source / event_date`
> set (no `instrument`); a reader spanning the cutover must handle both depths
> (pyarrow hive partitioning extracts each key by name regardless of order/depth).

---

## 3. Event schemas

JSON keys are stable; Parquet columns mirror them (plus the partition columns
`schema_version`, `source`, `instrument`, `event_date` in v2). `None`/null fields
are dropped from Parquet rows. In v2 the row's resolved `InstrumentRef` is stored
under `instrument_ref` (the `instrument` column name is taken by the partition).

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

### 4.2 `trades` (sequence-bearing — Binance, Coinbase, Kraken; Binance USDT-M perp aggTrades via §4.5)

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
- the run **starts** with a snapshot anchor (the first event is a `snapshot`)
- event timestamps are monotonic non-decreasing (the snapshot has no exchange
  time and is skipped from this check)

**Multi-anchor (re-snapshot) handling.** Unlike Binance depth (one REST snapshot +
reconnect-in-place alignment), stream-snapshot venues re-snapshot mid-run *by
design* — on reconnect or periodic resync the venue simply pushes a fresh
`snapshot` frame. So a run legitimately contains several snapshot anchors. Replay
treats each `event_type == "snapshot"` as a **re-anchor**: it reseeds the book
wholesale and begins a new anchored sub-book, and all integrity checks (sequence,
checksum) are applied **within** each sub-book, never across an anchor boundary.
Multiple anchors are therefore expected, not a defect; only a run that does **not**
open on a snapshot is unreplayable.

Findings: `no_events`, `no_snapshot_anchor`, `run_does_not_start_with_snapshot`,
`non_monotonic_event_time`, and (reported but non-gating, as in §4.1)
`crossed_book_states`.

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
- the run **starts** with a snapshot anchor
- event timestamps are monotonic non-decreasing
- `data.u` advances by exactly 1 across every event **within each anchored
  sub-book** (a snapshot reseeds the id baseline, so the id jump at a re-snapshot is
  not a gap)

A `data.u` gap *between consecutive deltas of the same sub-book* now **blocks**
promotion (a dropped message means the book can't be reconstructed exactly).
Findings: `no_events`, `no_snapshot_anchor`, `run_does_not_start_with_snapshot`,
`non_monotonic_event_time`, `missing_update_id`, `update_id_gaps` (`delta > 1`),
`non_monotonic_update_id` (`delta <= 0` — reorder/reset), and (reported but
non-gating) `crossed_book_states`. A reconnect yields a second snapshot, which
re-anchors the book; the run stays replayable as long as each sub-book is contiguous.

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

**Depth-bounded book.** Kraken maintains a fixed-depth book (the subscribed
`book.{N}`, default 10) and **silently evicts** the worst level once a better one
arrives past depth `N` — it does **not** send a delete for the evicted level. Replay
is therefore called with `book_depth=10` and trims each side to its `N` best levels
after every mutation, so the reconstructed top-10 stays byte-identical to Kraken's.
Without the trim the local book accrues stale deep levels and the CRC32 diverges as
soon as the book churns past the opening snapshot (empirically ~90% of frames in a
5,000-event segment).

`replayable` iff **all** hold:

- `event_count > 0`
- the run **starts** with a snapshot anchor (each mid-run re-snapshot reseeds the
  book and is validated as its own sub-book)
- event timestamps are monotonic non-decreasing
- every event carries a checksum and all match the reconstructed (depth-trimmed) book

A checksum mismatch (or a missing checksum) now **blocks** promotion. Findings:
`no_events`, `no_snapshot_anchor`, `run_does_not_start_with_snapshot`,
`non_monotonic_event_time`, `missing_checksum`, `checksum_mismatch`, and (reported
but non-gating) `crossed_book_states`.

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

**Transport note — MEXC is protobuf, not JSON.** MEXC retired its JSON websocket
(`wss://wbs.mexc.com/ws`) on 2025-08-04; public market data is now **Protocol
Buffers** on `wss://wbs-api.mexc.com/ws` (the subscription ack and PING/PONG control
frames stay JSON text). It is the only binary-transport venue: the collector decodes a
binary `PushDataV3ApiWrapper` frame into the same payload dict every JSON venue
produces, via vendored generated bindings + the `protobuf` runtime
(`crypto_collector/collectors/mexc.py`, `…/mexc_pb/`; regenerate with
`scripts/generate_mexc_protobuf.py`). The decoded dict — including a `_mexc_decode`
provenance block (schema, proto source, decoder version, frame byte length, SHA-256,
and base64 of the original frame) — is what lands in `raw/messages.jsonl`, so raw stays
a true rebuild source even though the wire bytes were protobuf. The vendored schema
and the classifications below were built from MEXC's published proto + docs and
shipped disabled until **verified against live frames on 2026-06-09 — both MEXC
lanes are now enabled** (v6 changelog item 4;
see `src/crypto_collector/proto/mexc/README.md`).

**Live adapter — MEXC spot `trades` (`aggre.deals`).** Channel
`spot@public.aggre.deals.v3.api.pb@<interval>@<SYMBOL>`. One frame batches several
deals (`publicAggreDeals.deals`), fanned out via `normalize_many`. Each deal carries
only `price`/`quantity`/`tradeType`/`time` — **no per-trade id** — so `sequence` is left
`None` and the whole-run verdict (`replay_trades_stream_run`) sets
`gap_detection: "none_native"`, `mode: "trade_stream_none_native"`. `tradeType` is the
**taker (aggressor) side** directly (`1` = buy, `2` = sell), so no flip is needed;
`buyer_is_maker` is derived. Same `replayable` bar as Bybit spot trades: `event_count >
0`, monotonic exchange timestamps, finite-positive `price`/`size`, clock skew within
`--max-clock-skew-ms`. Findings: `no_events`, `non_monotonic_event_time`,
`invalid_prices`, `invalid_sizes`, `excessive_clock_skew`. Promotes to
`trades_replayable`.

**Live adapter — MEXC spot `depth` (`limit.depth`).** Channel
`spot@public.limit.depth.v3.api.pb@<SYMBOL>@<depth>` (depth 5/10/20). MEXC's
limit-depth stream pushes the **full top-N book on every update** (`publicLimitDepths`
with `asks`/`bids` as `{"price","quantity"}`), so each frame is a complete partial-book
snapshot and is emitted with `event_type="snapshot"`. The whole-run verdict
(`replay_depth_stream_run`, default mode) treats every frame as a re-anchor and sets
`gap_detection: "none_native"`, `mode: "stream_snapshot"`. The per-frame `version` is
preserved in `metadata.mexc_version` as explicit gap-detection metadata but is **not**
used to prove gaplessness: limit-depth frames are independent full books (not a delta
chain), and the `version` is not a verified dense +1 counter — so unlike Bybit
`orderbook` (which IS `sequence`), this stays `none_native`. `replayable` iff
`event_count > 0`, the run starts with a snapshot anchor (always true — every frame is a
snapshot), and event timestamps (the wrapper `sendTime`) are monotonic non-decreasing.
Promotes to `market_replayable`. A future pass could upgrade this lane to a provable
`sequence` guarantee if a dense per-symbol diff id is verified live (the same path Bybit
depth took); the `version` is already captured for that.

> **Bybit keepalive.** Bybit drops idle public connections after ~10 min and
> expects a `{"op":"ping"}` roughly every 20 s. Both Bybit lanes opt into the
> collector's app-level keepalive (`CollectorConfig.ping_message` +
> `ping_interval_seconds=20`), which sends the ping on the open socket every
> interval, concurrently with the receive loop, so a low-volume symbol no longer
> ends its segment early on the idle drop. The pong reply carries no `topic`, so
> the data path drops it (`_should_emit`) and it can't be mistaken for the
> subscription ack. MEXC opts into the same keepalive with its own control frame
> (`{"method":"PING"}` every 20 s; the `{"msg":"PONG"}` reply has no `channel`, so
> `_should_emit` drops it) because MEXC closes a subscribed-but-silent connection after
> ~60 s. Every other venue leaves the keepalive off and relies on the `websockets`
> library's protocol-level ping/pong — the live Binance collector is unchanged.

> **Data-arrival watchdog.** A feed can ack the subscription and then go
> *silent-but-connected* — keepalive/ping still flowing, but zero data frames (exactly
> how Coinbase's now-dead `level2_batch` channel presented: acked, then nothing). Left
> unguarded, `GenericWebsocketCollector.stream` blocks forever in `async for message in
> websocket`, so the segment never reaches its count, never writes a replay summary, and
> the lane silently stops producing without raising. A collector configured with
> `CollectorConfig.idle_timeout_seconds > 0` bounds the wait for each next data frame; if
> none arrives in time it **ends the segment cleanly** (the run finalizes — metrics +
> replay summary written — and the worker loop opens a fresh segment) rather than hanging
> in `recv`. Each fire increments `idle_timeout_count`, recorded in
> `metrics/summary.jsonl` and surfaced by `health` as a non-blocking
> `idle_timeout:<worker>` finding for active workers (it self-heals via the fresh
> segment, so it does not mark the worker blocking). **Default off** (`0.0`) — the live
> Binance lanes are unaffected; enable per-lane via the ops config
> (`idle_timeout_seconds`). Complementary to the Bybit keepalive above: keepalive keeps
> the connection from being dropped; the watchdog catches a connection that stays up but
> stops delivering.

---

### 4.4 OKX `books` depth — linked-chain `sequence` (provable)

OKX `books` carries `seqId` and `prevSeqId` on every update, where
`prevSeqId(N) == seqId(N-1)` — a **linked chain** rather than a dense counter (the
ids themselves are not contiguous). The normalizer maps `prevSeqId` →
`first_update_id` and `seqId` → `final_update_id`, and replay
(`replay_depth_stream_run(chain_sequence=True)`, `mode="stream_snapshot_chain"`)
validates the chain by **equality** instead of `delta == 1`: within an anchored
sub-book, a delta whose `first_update_id` does not match the previous event's
`final_update_id` is a provable gap, and a regressed `final_update_id` is a
reorder — both **block promotion**. A snapshot reseeds the baseline (its
`prevSeqId` is `-1`), so the id jump at a re-snapshot is never miscounted. The
summary is tagged `gap_detection="sequence"` — same provable class as Binance/Bybit
depth, different proof shape. OKX **trades** carry no provable sequence and are
`none_native` (§4.3). Both apply to spot and the linear perp (`BTC-USDT-SWAP` →
`okx_perp_*` lanes).

### 4.5 Binance USDT-M perp — REST polling lanes

The Binance futures *websocket* is jurisdiction-blocked from the maintainer's
deployment (acks SUBSCRIBE, streams zero frames), while `fapi` REST works — so the
perp lanes poll REST (`binance-futures-rest-worker`), one lane per stream:

- **`trades`** (`/fapi/v1/aggTrades` → `binance_perp_trades/`): the aggregate-trade
  id is a dense per-symbol counter, and the poll cursor (last seen id + 1) is
  **persisted across segments and restarts**, so the lane is **gap-proof end-to-end**
  — `gap_detection="sequence"`, same §4.2 bar.
- **`depth`** (`/fapi/v1/depth` → `binance_perp_depth/`): per-poll independent
  full-book snapshots, no diff stream — `none_native` (§4.3), each poll is its own
  anchor.
- **`funding`** (`/fapi/v1/premiumIndex` → `binance_perp_funding/`): mark price,
  index price, and funding rate — a low-rate metric (mark ticks ~1/s, funding every
  8 h), so REST polling loses no fidelity. Normalized into the **`funding` dataset**
  and promoted to `curated/research/funding`; scored by `replay_funding_run`
  (`none_native`: finite-positive marks, monotonic timestamps — there is no sequence
  to prove).

---

## 5. Live quality gate (pre-replay)

Applied per event during collection; failures go to `quarantine/events.jsonl`
with a `reasons` list and are excluded from the normalized/clean stream:

`parse_errors` (any), `invalid_side`, `non_positive_price`, `negative_size`,
`stale_or_clock_skew` (delay `> max_delay_ms` or `< -max_future_skew_ms`),
`non_monotonic_sequence` (strictly decreasing `sequence`), `unknown_event_type`,
`invalid_trade_price` / `invalid_trade_size` (trades-channel events whose price or
size is missing or not finite-positive — mirrors the §4.2 promotion bar, so one odd
print quarantines alone instead of failing its whole segment at scoring),
`subscribe_replay` (prints a venue re-delivers at subscribe time — Kraken's
trade-channel `snapshot` frame, Coinbase's `last_match`. The previous segment
already captured them and promotion has no cross-run row dedup, so they must not
re-enter clean; they remain in raw and in quarantine).

The gate is a fast online filter; §4 replay is the authoritative, whole-run
verdict that gates promotion.

---

## 6. Consuming the data

1. **Read curated, not raw.** Pull from
   `curated/research/{market_replayable,trades_replayable,funding}/schema_version=v2/source=<src>/instrument=<canonical>/event_date=<date>/`.
   Everything there passed §4. (Legacy pre-cutover data sits under
   `schema_version=v1` with venue-only partitions.)
2. **Check the manifest first.** `research_manifest_latest.json` is tagged with
   `standards_version` (matches `STANDARDS_VERSION` above) and carries two views:
   - `lanes` — the canonical per-`(venue, instrument, dataset)` readiness. Each
     lane is discovered from its raw lane directory
     (`<venue>[_perp]_<dataset>[_<instrument>]`; perp lanes report venue
     `<venue>_perp`, covering `depth`/`trades`/`funding`), carries a
     `gap_detection` tag (`sequence`/`checksum` = provable; `none_native` = §4.3
     best-effort; `unknown` = no replay summary observed yet — never assume
     provable; a lane's tag is the worst class it has ever produced), and lists
     per-`event_date` `readiness`. Readiness is driven by the curated promotion index
     (`run_path` → lane, accurate per instrument) and the lane's raw replay
     summaries — **not** the Parquet partitions (which, since v2, do carry an
     `instrument` partition; see §2).
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

> **Note (v2):** curated/normalized Parquet now carries an `instrument=` partition
> (the sanitized canonical symbol), so per-instrument lanes of the same venue no
> longer share partitions — pull by `(venue, instrument, event_date)` straight off
> the path. The manifest's readiness still comes from the promotion index + raw
> replay summaries (authoritative per originating lane), not from globbing the
> Parquet layout. Legacy v1 partitions (venue-only) coexist for pre-cutover data.

---

## 7. Retention

- **Raw** (`raw/market/<src>`): default **14 days**, overridable per dataset via
  the cleanup job's `raw_policy` (`market/<src>=<days>`). Raw is the rebuild
  source; keep it long enough to re-promote after a logic fix.
- **Raw cold tier**: aged raw runs are **verify-moved** (not deleted) to
  `D:\market_archive_cold` by the `archive-offload` ops job. A run is offloaded
  only after it appears in its lane's promotion index (the curated copy is
  durably on disk) or quarantine index (known-bad); runs in neither index are
  never touched and are surfaced as `unindexed`. Every move is recorded in the
  lane's `_offload_index.jsonl`, so raw remains a locatable rebuild source after
  offload.
- **Normalized / curated / quarantine / manifests**: retained indefinitely (no
  auto-prune today). Curated is the long-lived research artifact.
- Cleanup runs in **dry-run by default** (`--apply` to act).

---

## 8. Roadmap (NOT guaranteed yet)

The forward-looking plan lives in [`ROADMAP.md`](ROADMAP.md). Items there are
aspirations, listed so nobody mistakes them for the current contract — notably:

- **Per-pair precision table for Kraken `depth` checksum validation.** Checksum
  validation is live for BTC/USD (`_KRAKEN_BOOK_PRECISION`); other Kraken pairs
  fall back to `none_native` until their `(price, qty)` precision is added.
- **MEXC depth upgrade to provable `sequence`** if the captured `version` proves
  dense against live frames (the Bybit-depth path).
- Continuous day-bounded rotation as the default run model (vs. wall-clock
  segment rotation).
