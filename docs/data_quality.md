# Data Quality

Research-grade data means every chunk is auditable before it is used.
[`../STANDARDS.md`](../STANDARDS.md) §4–§5 is the canonical definition; this is the
operational summary.

There are two checkpoints:

1. **Live quality gate** (per event, during collection) — fast online filter;
   failures go to `quarantine/events.jsonl` with a `reasons` list and are excluded
   from the clean/normalized stream.
2. **Replay verdict** (per run, after collection) — the authoritative, whole-run
   check that writes `metrics/replay_summary.json` and gates promotion.

## What "replayable" means depends on the feed

The bar a run must clear is set by its `gap_detection` class, recorded in
`replay_summary.json`:

- **`sequence`** (Binance depth/trades, Binance USDT-M perp aggTrades via REST,
  Coinbase trades, Kraken trades, Bybit `orderbook` depth, OKX `books` depth) —
  a per-message id proves gaplessness, either dense (Bybit orderbook `data.u` is
  +1 per message) or as a linked chain (OKX `prevSeqId(N) == seqId(N-1)`, validated
  by equality — STANDARDS §4.4). `replayable` here means gap-proof.
- **`checksum`** (Kraken `book` depth) — a per-frame CRC32 over the reconstructed
  top-10 book is validated, so a dropped/corrupted update is caught. Also
  **provable** — `replayable` means gap-proof.
- **`none_native`** (Coinbase depth, Bybit trades, OKX trades, both MEXC lanes,
  Binance perp REST depth/funding) — no usable integrity signal (no sequence at
  all, or only a UUID / shared counter). `replayable` is downgraded to
  **structurally clean only**: runs start with a snapshot anchor, parse-clean
  events, monotonic timestamps — **not** gap-proof. Consumers MUST key off the
  lane's `gap_detection` tag, not the dataset name.

## Quality Gates (live filter)

The collector writes bad or suspicious events to quarantine instead of silently
dropping them. The gate flags: parse errors, invalid side, non-positive price,
negative size, stale / clock-skewed events, non-monotonic sequence, and unknown
event types.

Depth runs are additionally checked at replay time for:

- a single snapshot anchor (REST anchor for Binance; in-stream `snapshot` event
  for the `none_native` feeds, which must be the first event)
- replayability from snapshot plus deltas (`sequence` feeds: `U`/`u` contiguity,
  no reorders/dupes, valid update ranges)
- monotonic event timestamps (`none_native` feeds)
- crossed-book states (reported for visibility; non-gating)

## Quarantine

Use:

```powershell
market-data-plant quarantine-runs --source-root G:\market_archive\raw\market\binance_depth --quarantine-root G:\market_archive\quarantine\market\binance_depth
```

Quarantined runs are tracked in `_quarantine_index.jsonl` and skipped by promotion.

## Promotion

Use:

```powershell
market-data-plant promote-replayable --source-root G:\market_archive\raw\market\binance_depth --target-root G:\market_archive\curated\research\market_replayable
```

Promotion writes a `_promotion_index.jsonl` so repeated runs are idempotent.

The quarantine → promote chain is **per-lane**: point `--source-root` at any lane
(`...\raw\market\<lane>`) and the matching target. Depth lanes promote into
`market_replayable`; trades lanes into `trades_replayable`. The live ops runner
does this automatically per enabled lane; the commands above are for
manual/backfill use.

## Manifest

Use:

```powershell
market-data-plant research-manifest --archive-root G:\market_archive --output-root G:\market_archive\curated\research\manifests
```

The manifest is the per-`(venue, instrument, dataset, event_date)` readiness
contract. Each lane carries a `gap_detection` tag and per-date readiness
(`ready` / `ready_with_quarantine` / `building` / `missing`). See
[`../STANDARDS.md`](../STANDARDS.md) §6 for how to consume it.
