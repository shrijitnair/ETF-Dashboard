# Market Dashboard

Backend-hosted multi-tab market dashboard for:
- US ETFs
- UCITS ETFs listed on the London Stock Exchange
- US stocks

The UI is still plain HTML/CSS/JS, but it now runs behind a small Flask server so the page can persist user-added tickers and rebuild the dashboard data in place.

## Local setup

```bash
python3.11 -m pip install --user -r requirements.txt
python3.11 scripts/build_data.py --out-dir data
python3.11 server.py
```

The app listens on `http://127.0.0.1:8123` by default.

## API

- `GET /api/dashboard`
  - returns the combined dashboard payload used by the frontend
- `POST /api/instruments`
  - request body: `{"tab_id":"us-etfs|ucits-etfs-lse|us-stocks","ticker":"<yahoo_symbol>"}`
  - persists the ticker into the selected tab’s `Custom` group and rebuilds the dashboard

## Watchlist files

- `config/watchlists.json`
  - curated theme groups and default instruments
- `config/custom_watchlists.json`
  - server-persisted user additions under `Custom`

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
