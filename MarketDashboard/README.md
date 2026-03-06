# Market Dashboard

Static multi-tab market dashboard for:
- US ETFs
- UCITS ETFs listed on the London Stock Exchange
- US stocks

The UI is built with plain HTML/CSS/JS and is designed to be hosted on GitHub Pages or any static file server. The data is pre-built using a Python script.

## Local setup

To build the data and serve the site locally:

```bash
python3.11 -m pip install --user -r requirements.txt
python3.11 scripts/build_data.py --out-dir data
python3 -m http.server 8000
```

The app will be available at `http://localhost:8000`.

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
