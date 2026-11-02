# Quarterly Earnings Shocks — Moneycontrol collector

Collect available Moneycontrol earnings comparisons and upcoming analyst estimates for the current Nifty 500. Python's standard library is sufficient; no paid API key is required.

## Daily schedule

GitHub Actions checks for data **every day at 8:35 AM India time**, including weekends. The cron expression is `5 3 * * *` in UTC. The workflow also supports **Actions → Daily Moneycontrol collection → Run workflow** and tests collection when collector code changes on `main`.

The collector runs on GitHub's servers. Your computer can be switched off. GitHub schedules can start late under load, so 8:35 is the requested start time rather than an exact execution-time guarantee. See [GitHub's scheduling documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).

The workflow restores the committed SQLite history, fetches the latest available cards, records new and revised values, saves an immutable capture, and commits the updated data to `main`. Even unchanged financial values get a dated observation so that the research has evidence of what was visible on each collection day. Database versions are deduplicated. Access cooldowns and identifier caches persist between runs.

## Read the data

After the first workflow run, open:

- [`data/moneycontrol/exports/latest_metrics.json`](data/moneycontrol/exports/latest_metrics.json): latest observed revenue, net profit and EPS actuals/estimates, with period, reporting basis, capture times and flags.
- [`data/moneycontrol/exports/coverage.json`](data/moneycontrol/exports/coverage.json): every constituent, including companies without a captured card.
- [`data/moneycontrol/changes.json`](data/moneycontrol/changes.json): new/revised metric versions and the unchanged count for the latest run.
- [`data/moneycontrol/exports/latest_run.json`](data/moneycontrol/exports/latest_run.json): success/partial/error, pagination, counts and errors.
- [`data/moneycontrol/moneycontrol.sqlite3`](data/moneycontrol/moneycontrol.sqlite3): complete collection history, versions, observations and source URLs.
- [`data/moneycontrol/snapshots/`](data/moneycontrol/snapshots/): immutable gzip JSON captures grouped by collection date in India.
- [`data/moneycontrol/raw/`](data/moneycontrol/raw/): compressed original source responses indexed by SHA-256.

Each workflow also provides a downloadable `moneycontrol-<run-id>` artifact, retained for 30 days. The committed history remains in Git independently of artifact retention. Failed or partial runs remain visibly failed in Actions; available data, logs and access cooldowns are preserved rather than reporting a successful empty fetch.

## Scope and interpretation

Result/estimate values come from Moneycontrol. The official Nifty Indices constituent file verifies membership and is refreshed daily. The source's index filter can differ from current constituents, so the collector scans its wider public card view and filters locally. Missing company identifiers are verified through Moneycontrol by ISIN. The official download may temporarily contain extra entries; all published entries are kept, with a count warning.

This collects the cards Moneycontrol makes available; it does not guarantee analyst estimates for all 500 companies or retrieve every company's full financial statement. No card in these estimate feeds does not establish that a company has not announced results. Null is missing data; zero remains zero. Revenue and net profit are INR crore, and EPS is INR per share.

The source does not expose individual analyst forecasts, analyst counts, dispersion, a reliable estimate-as-of timestamp or the first public release timestamp in these feeds. Its high/low expectation tabs describe expected year-over-year profit growth, not the highest/lowest analyst forecasts. Completed-result comparisons cannot establish what consensus was before the announcement. A future event study must use a capture verified to precede the actual release.

Forecast aggregation and earnings-adjustment methodology for the exact comparison cards is not fully documented. Consolidated/standalone basis is preserved, and known discrepancies are flagged. Sun Pharma Q2 FY2026–27 has a known profit mismatch against its company release; the source value is preserved for review.

Daily snapshots can miss forecast revisions made between 8:35 AM and an intraday announcement. Market/sector returns, five-trading-day stock returns, historical index membership and Tapetide dispersion are not collected here. Collection history starts with the first cloud run; no historical consensus is reconstructed.

## Run locally

```bash
python -m unittest discover -s tests -v
python mc_pipeline.py run
python scripts/snapshot_run.py
python mc_pipeline.py status
python mc_pipeline.py export --as-of '2026-11-03T08:35:00+05:30'
```

The example cutoff only selects captures that actually exist. The configuration sets `publish_directory` to null, so it contains no personal Windows export path. Local `data/` is ignored by default; the workflow explicitly commits only its intended data files.

## Source access

Requests are spaced at least 1.5 seconds apart. Temporary network/server errors are retried. HTTP 401/403 initiates a 24-hour host cooldown; HTTP 429 initiates a cooldown of at least one hour. The collector does not use account cookies, proxies or access-control workarounds. Source availability and schemas can change, and persistent failures require maintenance. Moneycontrol's extraction and reuse conditions apply; this repository grants no licence to the underlying data.

- [Moneycontrol estimates](https://www.moneycontrol.com/markets/earnings/estimates/)
- [Official Nifty 500 constituent file](https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv)
