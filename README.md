# Market Dashboard

Static multi-tab market dashboard for:
- US ETFs
- UCITS ETFs listed on the London Stock Exchange
- US stocks

The UI is built with plain HTML/CSS/JS and is hosted on GitHub Pages. The data is pre-built using a Python script and refreshed by GitHub Actions every 10 minutes.

## Local setup

To build the data and serve the site locally:

```bash
python3.11 -m pip install --user -r requirements.txt
python3.11 scripts/build_data.py --out-dir data
python3 -m http.server 8000
```

The app will be available at `http://localhost:8000`.

## GitHub Pages deployment

The `Deploy dashboard to GitHub Pages` workflow publishes the repository root whenever changes are pushed to `main` or `master`. The `Build Dashboard Data` workflow fetches market and FX data every 10 minutes, commits changed JSON files, and thereby triggers a Pages deployment.

After enabling Pages for the repository, select **GitHub Actions** as the source under **Settings → Pages**. The dashboard will then be available at:

`https://shrijitnair.github.io/ETF-Dashboard/`

The dashboard is near-live rather than tick-by-tick real-time: it displays the latest successful data build. Market data is fetched from Yahoo Finance through `yfinance`, so provider delays, rate limits, and market holidays may affect freshness.

The 3Y and 5Y columns show annualized CAGR in INR. YTD appears immediately after the 3M return. CAGR is calculated as `(ending value / starting value)^(1 / years) - 1` using the same calendar-period anchors as the other return metrics.

## Watchlist files

- `config/watchlists.json`
  - curated theme groups and default instruments
- `config/custom_watchlists.json`
  - custom user additions under `Custom` group. Edit this file to add new instruments, then rebuild the data.

Each item declares:
- `ticker`: display ticker in the UI
- `source_ticker`: exact Yahoo Finance symbol
- `name`: fallback label
- `exchange`: fallback exchange label
- `currency`: fallback currency

## Current UCITS exclusions

These were intentionally left out because Yahoo reports them as non-USD quotes:
- `IUSE.L`
- `IWDE.L`
- `SEMI.L`
- `SP20.L`

## Output files

Running the builder generates:
- `data/dashboard.json`: combined API payload
- `data/snapshot.json`: grouped table rows
- `data/history.json`: historical price points for the detail chart
- `data/meta.json`: refresh timestamp, columns, and build failures
