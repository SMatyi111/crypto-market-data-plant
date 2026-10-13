# Alt canary stage one: operator runbook (controls only, nothing launched)

**Status: controls merged, default off.** This runbook is the exact, finite activation
checklist for the stage-one alt canary (one Bybit linear symbol's trades and depth lanes
plus the daily universe snapshot) under STANDARDS 4.18. Running it is an owner decision
recorded in the ROADMAP decision queue. Nothing in this document has been executed
against the live plant; no control directory exists; no lane is enabled.

## What the controls bound, and what they do not

| Bound | Mechanism | Limit of the claim |
| --- | --- | --- |
| One attempt | `prepare` refuses an existing directory; no reset/renew/retry API; job-identity digest per lane checked at every claim | an administrator can prepare a second directory; that is a new attempt by design |
| 24 h | UTC and monotonic deadline from the first grant; segment deadline clamped; idle wait ends at the deadline; expiry refuses dispatch | worker loop only: start-up, OS calls, runner cadence and teardown add to the real stop time and are reported, not bounded |
| 2 GiB shared | chunked pre-write reservation (16 MiB) plus a 4 MiB per-frame margin, 1 MiB inbound frame cap, bytes accounted from the sinks after each frame | one frame can overshoot the grant if its fan-out exceeds 4x; recorded as `overshoot_bytes`, never silent; no compression credit |
| Headroom | free space sampled at every chunk and in preflight: floor + unspent pool (+ 256 MiB external overhead in preflight) | a sample, not a reservation; other writers can consume space meanwhile |
| Exact stop | recorded pid + kernel creation time, re-checked through the handle used to terminate; unknown liveness blocks claims and finalization | a non-elevated shell cannot open SYSTEM workers: run status/stop/finalize elevated or they report unknown |
| Receipts | exclusive-create, fsynced JSON per grant; `finalize` writes `receipt.json` only when terminal, all grants closed, no writer alive or unknown | a receipt that fails to write is listed as unverified; partial runs stay on disk |

Not covered by the canary: curated copies (promotion for the canary sources must stay
disabled; preflight refuses otherwise), ops logs and heartbeats (outside the pool, inside
the 256 MiB external overhead), and the runner's subprocess timeout kill, which targets
the venv launcher stub rather than the worker (the recorded identity is the worker's own).

## Checklist (finite; stop at the first failed step)

All paths below are the live plant's; the control directory must sit outside every run,
offload and cleanup tree. Run from the live checkout only after it has been pulled to a
main that contains this change; never from an older main.

1. **Pull the live checkout** (`G:\01-active\trading\crypto-market-data-plant`) to the
   merged main. Verify `python -m crypto_collector.canary --help` runs with
   `PYTHONPATH=src`.
2. **Edit `ops.live.local.json`** (keep every new row `enabled:false` for now):
   - copy the three lane rows from `ops.live.example.json`: `bybit-gram-perp-trades`,
     `bybit-gram-perp-depth`, `bybit-linear-universe-snapshot`;
   - add `"canary_control": "G:\\market_archive\\canary\\gram-stage-one"` to each row's
     `args` (the exact path you will prepare in step 3);
   - optionally set `"canary_hold_seconds": 1800` and `"canary_min_free_gib": 215` (defaults);
   - do NOT copy the GRAM promoter, quarantine or score rows, or copy them disabled;
   - copy only the offload and cleanup-policy rows for these three sources
     (rollout correction 8).
3. **Prepare the attempt** (creates control state only, refuses an existing directory),
   then restrict the directory to SYSTEM and Administrators. This ACL is the defence:
   `stop --terminate` acts on the pid, creation time and kernel image path that the
   control DB records (checked through the terminating handle), so whoever can write the
   DB decides what a terminate targets:
   ```
   python -m crypto_collector.canary prepare G:\market_archive\canary\gram-stage-one gram-stage-one ^
     --config ops.live.local.json --symbol GRAMUSDT ^
     --job bybit-gram-perp-trades --job bybit-gram-perp-depth --job bybit-linear-universe-snapshot
   icacls G:\market_archive\canary\gram-stage-one /inheritance:r /grant:r "SYSTEM:(OI)(CI)F" "Administrators:(OI)(CI)F"
   ```
   Keep the printed lane identities with the activation record.
4. **Derive the pool cap**: `enabled collector lanes in ops.live.local.json + 3 + 2`,
   set `[int]$CollectorConcurrency` in BOTH `scripts/run_ops_runner.ps1` and
   `scripts/redeploy_runner.ps1`, parse-check both (ASCII), run
   `tests/test_repo_hygiene.py`. (With 47 enabled lanes today that is 52.)
5. **Fresh preflight** (read-only; must print `"ok": true`):
   ```
   python -m crypto_collector.canary preflight G:\market_archive\canary\gram-stage-one ^
     --config ops.live.local.json --min-free-gib 215 --scripts-root scripts
   ```
   It refuses enabled promoters/scorers for the canary sources, identity drift, lane
   arg drift, insufficient sampled headroom on the lane output roots and a cap below
   the derived need.
6. **Enable** the three rows (`enabled:true`) and nothing else. Re-run step 5.
7. **Redeploy** with the fixed script from the pulled main:
   `powershell -ExecutionPolicy Bypass -File scripts\redeploy_runner.ps1` (preflight), then
   `... -Apply`. Record the new runner pid.
8. **First 10 minutes**: `python -m crypto_collector.canary status <dir>` (elevated) shows
   one open grant per stream lane with `alive: true`; `health` shows no new finding; the
   universe lane runs at its next 00:05 UTC slot and its grant closes `complete`.
9. **Daily**: status (remaining seconds/bytes, grants, receipts), the measurements in the
   rollout ticket (bytes per run, quarantine ratio, replayable ratio, receipt lag, RSS,
   G: free trajectory), and the 1 MiB response cap watch item on the universe lane.
10. **End of the attempt** (automatic at 24 h or 2 GiB, or by decision):
    - automatic: workers refuse further grants and hold; `status` shows `stopped`;
    - by decision: `python -m crypto_collector.canary stop <dir>` (dry run), then
      `... --apply --wait-seconds 120`; add `--terminate` only if a recorded writer is
      still alive after the wait (it terminates that exact process only);
    - then set the three rows `enabled:false`, redeploy with the fixed script, and run
      `python -m crypto_collector.canary finalize <dir>` elevated. It refuses while any
      recorded writer is alive or unknown. `receipt.json` is the attempt's record.
11. **Never**: delete or move the control directory, reset the DB, re-prepare the same
    path, enable a second symbol, the ticker poll or liquidation lanes on this attempt,
    or promote the canary sources before the owner reviews the receipts.

## Reading a receipt

`receipts/<lane>__<grant>.json`: `reason` (`limit`, `deadline`, `canary_budget`,
`canary_expired`, `canary_overshoot`, `operator_stop`, `exception:<Type>`, `abandoned`,
or `<reason>:receipt_failed:<Error>`), `replayable` (the lane scorer's verdict for that
run, `null` when unscored), `reserved_bytes` / `accounted_bytes` / `measured_bytes` /
`overshoot_bytes`, `files`, `pid` and `process_identity`. `receipt.json` lists replayable
and unreplayable-or-unscored runs, grants without a receipt, and total consumption.
