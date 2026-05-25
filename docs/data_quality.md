# Data Quality

Research-grade data means every chunk is auditable before it is used.

## Quality Gates

The collector writes bad or suspicious events to quarantine instead of silently dropping them.

Depth chunks are checked for:

- valid timestamps
- non-stale events
- sequence and metadata consistency
- replayability from snapshot plus deltas
- crossed-book reconstruction
- replay gaps and snapshot gaps

## Quarantine

Use:

```powershell
market-data-plant quarantine-runs --source-root D:\market_archive\raw\market\binance_depth --quarantine-root D:\market_archive\quarantine\market\binance_depth
```

Quarantined runs are tracked in `_quarantine_index.jsonl` and skipped by promotion.

## Promotion

Use:

```powershell
market-data-plant promote-replayable --source-root D:\market_archive\raw\market\binance_depth --target-root D:\market_archive\curated\research\market_replayable
```

Promotion writes a `_promotion_index.jsonl` so repeated runs are idempotent.

## Manifest

Use:

```powershell
market-data-plant research-manifest --archive-root D:\market_archive --output-root D:\market_archive\curated\research\manifests
```

The manifest summarizes ready, building, and missing dates for curated market data.
