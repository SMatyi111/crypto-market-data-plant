# CLAUDE.md — working rules for this repo

Read [`ROADMAP.md`](ROADMAP.md) first (the plan + dated checks live there), then
[`STANDARDS.md`](STANDARDS.md) before touching anything that writes or reads data.
Resolved-work narrative: [`docs/HISTORY.md`](docs/HISTORY.md). Runbook:
[`docs/windows_service.md`](docs/windows_service.md).

## Hard rules

- **`.ps1` files must stay ASCII-only.** They run under Windows PowerShell 5.1,
  which misdecodes UTF-8 em-dashes/typographic quotes into string-terminating curly
  quotes. After editing any `.ps1`, parse-check it:
  `powershell -NoProfile -Command "[void][scriptblock]::Create((Get-Content -Raw <file>))"`.
- **Run tests with `PYTHONPATH=src`** (`python -m pytest`). Do not trust a bare
  editable install — it has historically resolved to a retired external tree.
- **The ops runner reads its config once at startup.** Config or code changes do
  NOT take effect until the runner restarts (`scripts/redeploy_runner.ps1`, or
  reboot — the SYSTEM task `CryptoMarketDataPlant` starts it at boot). Say so when
  delivering a change: merged ≠ deployed.
- **When adding collector lanes, bump `-CollectorConcurrency`** in
  `scripts/run_ops_runner.ps1` AND `scripts/redeploy_runner.ps1` (one slot per
  worker lane — currently 21). This has silently starved new lanes twice.
- **Thread new per-lane config fields centrally** through `_run_segmented_worker`
  in `cli.py`, never by extending a per-venue `build_segment_args` lambda — those
  lambdas silently dropped `market` and `jsonl_fsync` in the past. Add a regression
  test that the field survives dispatch.
- **Exactly one promoter per lane.** `promote-replayable` jobs are the only thing
  that writes curated parquet; scorers must use `--score-only`. Two promoters
  duplicate curated rows.
- If you change a schema, partition layout, or the meaning of "replayable":
  **bump `STANDARDS_VERSION`** in both `src/crypto_collector/config.py` and
  `STANDARDS.md`, same change.

## Environment facts (verified, don't rediscover)

- Live archive root: `G:\market_archive` (NVMe). Old `D:\market_archive` =
  read-only history; `D:\market_archive_cold` = offload cold tier.
- Binance USDT-M futures **websocket is blocked** from this box (acks SUBSCRIBE,
  zero frames); `fapi` REST works — hence the REST-polling perp lanes.
- Coinbase BTC-USDC is delisted.
- Non-elevated shells can't see the SYSTEM task's arguments or other users'
  process command lines, and can't create `Global\` mutexes.
- Local-only (gitignored, do not commit): `ops.live.local.json`, `*.bak*`,
  `.tmp_research/`, `screen-*.png`, `artifacts/`.

## Workflow

- Branch + PR to `main`; CI is GitHub Actions (windows-latest, py3.11 + 3.12).
  Keep tests hermetic — no dependence on live data or big disks.
- Update `ROADMAP.md` in the same change when scope/status moves; move finished
  narrative to `docs/HISTORY.md`.
- This repo is public-safe by contract: no keys, no signed endpoints, no archive
  data, no notebooks (see `docs/publication_safety.md`).

## Quality gates & review protocol

- **CI gates (all must be green before handing a PR to the owner):** pytest
  (windows-latest, py3.11+3.12), ruff (bug-focused rule set in `pyproject.toml` —
  a lint failure means "probable bug", never cosmetics), and the repo-hygiene
  tests (`tests/test_repo_hygiene.py`: ASCII-only `.ps1`, concurrency-cap covers
  enabled lanes, runner-script defaults in sync).
- **Every code PR** gets a `/code-review` pass at medium effort before the owner
  sees it. Fix confirmed findings in the same PR, or list them explicitly in the
  PR description with why they were left. Docs-only PRs skip agent review.
- **Add `/security-review`** when a change touches collectors/network endpoints,
  configs, subprocess or script execution, or the publication surface.
- **Session-start ops audit ritual:** if the "Last ops audit" stamp in ROADMAP.md
  is more than ~3 days old, audit the live plant before starting new work: health
  report, job success rate since last restart, per-lane freshness/backlog,
  quarantine ratios, G: disk headroom, offload index sanity. Findings go to
  ROADMAP; update the stamp; escalate only decisions.

## Governance (who decides what)

- **Claude acts autonomously** (always via PR — the owner merges): bug fixes,
  tests, docs, reviews, hygiene and refactors that do not change the data
  contract.
- **The owner decides:** PR merges, live runner restarts/redeploys, STANDARDS /
  contract changes, new venues/lanes/instruments (rate-limit + jurisdiction
  exposure), data retention or deletion, anything that publishes content or
  spends money.
- Pending owner decisions queue in ROADMAP's **Decision queue** — don't interrupt
  mid-task, and never act on a queued decision without an explicit OK.
