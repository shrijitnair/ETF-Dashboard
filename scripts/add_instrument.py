#!/usr/bin/env python3
"""Validate and add one instrument to the custom dashboard watchlist."""

from __future__ import annotations

import argparse
import re
from typing import Any, Dict, Optional, Sequence, Tuple

import yfinance as yf

try:
    from scripts.build_data import (
        CUSTOM_GROUP_ID,
        CUSTOM_GROUP_LABEL,
        fetch_ticker_info,
        load_json,
        normalize_currency_key,
        normalize_custom_config,
        write_json_atomic,
    )
except ModuleNotFoundError:
    from build_data import (
        CUSTOM_GROUP_ID,
        CUSTOM_GROUP_LABEL,
        fetch_ticker_info,
        load_json,
        normalize_currency_key,
        normalize_custom_config,
        write_json_atomic,
    )


TARGET_TABS: Dict[str, Dict[str, Any]] = {
    "us-etfs": {
        "label": "NASDAQ ETFs",
        "asset_type": "etf",
        "allowed_currencies": {"USD"},
        "default_currency": "USD",
        "expected_quote_type": "ETF",
    },
    "ucits-etfs-lse": {
        "label": "UCITS ETFs (LSE)",
        "asset_type": "etf",
        "allowed_currencies": {"USD", "GBP"},
        "default_currency": "USD",
        "expected_quote_type": "ETF",
    },
    "us-stocks": {
        "label": "US Stocks",
        "asset_type": "stock",
        "allowed_currencies": {"USD"},
        "default_currency": "USD",
        "expected_quote_type": "EQUITY",
    },
}

TAB_ALIASES = {
    "US Stocks": "us-stocks",
    "NASDAQ ETFs": "us-etfs",
    "UCITS ETFs (LSE)": "ucits-etfs-lse",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", required=True, help="Ticker, optionally prefixed with an exchange.")
    parser.add_argument("--tab-id", required=True, choices=sorted(TAB_ALIASES))
    parser.add_argument("--config", default="config/watchlists.json")
    parser.add_argument("--custom-config", default="config/custom_watchlists.json")
    return parser.parse_args()


def normalize_symbol(value: str) -> str:
    symbol = str(value or "").strip().upper()
    if ":" in symbol:
        symbol = symbol.rsplit(":", 1)[1]
    if symbol.endswith(".L"):
        symbol = symbol[:-2]
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9._-]{0,19}", symbol):
        raise ValueError("Use a valid ticker symbol, for example CSCO or VUSA.")
    return symbol


def instrument_definition(symbol: str, tab_id: str) -> Tuple[str, str, Dict[str, Any]]:
    tab_id = TAB_ALIASES.get(tab_id, tab_id)
    tab = TARGET_TABS[tab_id]
    display_ticker = normalize_symbol(symbol)
    source_ticker = f"{display_ticker}.L" if tab_id == "ucits-etfs-lse" else display_ticker
    return display_ticker, source_ticker, tab


def item_source_ticker(item: Dict[str, Any]) -> str:
    return str(item.get("source_ticker") or item.get("ticker") or "").strip().upper()


def tab_contains_source(tab: Optional[Dict[str, Any]], source_ticker: str) -> bool:
    if not tab:
        return False
    return any(
        item_source_ticker(item) == source_ticker
        for group in tab.get("groups", [])
        for item in group.get("items", [])
        if isinstance(item, dict)
    )


def validate_market_data(
    display_ticker: str,
    source_ticker: str,
    tab: Dict[str, Any],
) -> Dict[str, str]:
    history = yf.Ticker(source_ticker).history(
        period="6mo",
        interval="1d",
        auto_adjust=False,
    )
    close_series = history.get("Close")
    if close_series is None or close_series.dropna().shape[0] < 5:
        raise ValueError(f"Yahoo Finance returned insufficient price history for {source_ticker}.")

    info, info_error = fetch_ticker_info(source_ticker)
    quote_type = str(info.get("quoteType") or "").upper()
    if quote_type and quote_type != tab["expected_quote_type"]:
        raise ValueError(
            f"{display_ticker} is reported as {quote_type}, not a {tab['expected_quote_type'].lower()}."
        )

    raw_currency = info.get("currency") or tab["default_currency"]
    currency = normalize_currency_key(raw_currency)
    if currency not in tab["allowed_currencies"]:
        allowed = " or ".join(sorted(tab["allowed_currencies"]))
        raise ValueError(f"{display_ticker} is quoted in {currency}; {tab['label']} requires {allowed}.")

    latest_date = close_series.dropna().index[-1].strftime("%Y-%m-%d")
    if info_error:
        print(f"Yahoo metadata warning for {source_ticker}: {info_error}")

    return {
        "name": str(info.get("longName") or info.get("shortName") or display_ticker),
        "exchange": str(info.get("fullExchangeName") or info.get("exchange") or ""),
        "currency": str(raw_currency),
        "latest_date": latest_date,
    }


def add_to_custom_config(
    curated_config: Dict[str, Any],
    custom_config: Dict[str, Any],
    tab_id: str,
    display_ticker: str,
    source_ticker: str,
    metadata: Dict[str, str],
) -> Dict[str, Any]:
    normalized = normalize_custom_config(custom_config, curated_config.get("tabs", []))
    curated_tab = next((tab for tab in curated_config.get("tabs", []) if tab.get("id") == tab_id), None)
    custom_tab = next((tab for tab in normalized.get("tabs", []) if tab.get("id") == tab_id), None)

    if tab_contains_source(curated_tab, source_ticker) or tab_contains_source(custom_tab, source_ticker):
        raise ValueError(f"{display_ticker} is already tracked in {TARGET_TABS[tab_id]['label']}.")

    custom_group = next(
        (group for group in custom_tab.get("groups", []) if group.get("id") == CUSTOM_GROUP_ID),
        None,
    )
    if custom_group is None:
        custom_group = {"id": CUSTOM_GROUP_ID, "label": CUSTOM_GROUP_LABEL, "items": []}
        custom_tab.setdefault("groups", []).append(custom_group)

    custom_group.setdefault("items", []).append(
        {
            "ticker": display_ticker,
            "source_ticker": source_ticker,
            "name": metadata["name"],
            "exchange": metadata["exchange"],
            "currency": metadata["currency"],
        }
    )
    return normalized


def add_instrument(
    symbol: str,
    tab_id: str,
    config_path: str = "config/watchlists.json",
    custom_config_path: str = "config/custom_watchlists.json",
) -> Dict[str, str]:
    internal_tab_id = TAB_ALIASES.get(tab_id, tab_id)
    display_ticker, source_ticker, tab = instrument_definition(symbol, internal_tab_id)
    metadata = validate_market_data(display_ticker, source_ticker, tab)
    curated_config = load_json(config_path)
    custom_config = load_json(custom_config_path)
    updated_config = add_to_custom_config(
        curated_config,
        custom_config,
        internal_tab_id,
        display_ticker,
        source_ticker,
        metadata,
    )
    write_json_atomic(custom_config_path, updated_config)
    return {
        "ticker": display_ticker,
        "source_ticker": source_ticker,
        "tab_label": tab["label"],
        "name": metadata["name"],
        "currency": metadata["currency"],
        "latest_date": metadata["latest_date"],
    }


def main() -> None:
    args = parse_args()
    result = add_instrument(args.symbol, args.tab_id, args.config, args.custom_config)
    print(
        "Added {ticker} ({source_ticker}) to {tab_label}; latest Yahoo date: {latest_date}".format(
            **result
        )
    )


if __name__ == "__main__":
    main()
