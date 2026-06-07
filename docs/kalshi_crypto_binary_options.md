# Kalshi Crypto Binary Options

The Kalshi collector is public REST collection only. It does not place orders,
does not call signed endpoints, and does not require credentials.

## Commands

Discover BTC/ETH crypto binary series and open markets:

```powershell
market-data-plant kalshi-discover-crypto --target-assets BTC ETH --target-frequencies fifteen_min hourly
```

Collect a bounded quote snapshot run:

```powershell
market-data-plant kalshi-collect-crypto-quotes --sample-count 120 --poll-interval-seconds 5 --stale-after-seconds 3
```

Summarize a run:

```powershell
market-data-plant kalshi-summarize-crypto-quotes --input-path D:\market_archive\raw\market\kalshi_crypto_quotes\<run_id>
```

For live experiments, keep `--stale-after-seconds` near the polling cadence you are
trying to evaluate, usually `2` or `3`. Use `--markets-per-series` to cap the open
markets fetched from each selected series.

## Output Layout

Collection writes one run directory under the normal raw market root:

```text
D:\market_archive
  raw\
    market\
      kalshi_crypto_quotes\<run_id>\
        raw\messages.jsonl              # /series and /markets API response envelopes
        clean\events.jsonl              # side-specific normalized quote telemetry
        quarantine\events.jsonl         # reserved for rejected normalized rows
        metrics\summary.jsonl
        metrics\collection_summary.json
  normalized\
    binary_options\
      schema_version=v2\
        source=kalshi\
          instrument=<market_ticker_side>\
            event_date=YYYY-MM-DD\
              part-*.parquet
```

Discovery reports are written under:

```text
D:\market_archive\curated\research\kalshi_crypto_binary_options
```

## Normalized Rows

Each open market produces up to two quote rows per poll, one for `YES` and one for
`NO`. Rows include request timing, Kalshi tickers, side-specific symbol, frequency,
close/expiration timing, bid/ask/ask size, payout ratio, quote identity and staleness,
volume/open-interest/liquidity fields, fractional trading flag, and HTTP/error
telemetry.

`quote_id` is derived from:

```text
symbol + bid + ask + ask_size + close_time
```

Repeated REST snapshots are not counted as quote updates. A quote update is a
subsequent `quote_id` transition within the same `symbol`; the first observed state
for a symbol is only the baseline. The collector labels rows with
`quote_first_observed`, `quote_changed`, `quote_repeated`, `quote_age_ms`, and
`quote_stale`, and the summarizer applies the same per-symbol transition rule.
