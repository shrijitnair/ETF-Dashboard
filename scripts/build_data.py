#!/usr/bin/env python3
"""Build and serve market dashboard data for the backend-hosted app."""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import yfinance as yf


DEFAULT_CONFIG_PATH = "config/watchlists.json"
DEFAULT_CUSTOM_CONFIG_PATH = "config/custom_watchlists.json"
DEFAULT_OUTPUT_DIR = "data"

LOOKBACK_PERIOD = "6y"
CUSTOM_GROUP_ID = "custom"
CUSTOM_GROUP_LABEL = "Custom"
RETURN_BASIS = "inr_adjusted"
PERIOD_BASIS = "yahoo_style_calendar"

SNAPSHOT_FILENAME = "snapshot.json"
HISTORY_FILENAME = "history.json"
META_FILENAME = "meta.json"
DASHBOARD_FILENAME = "dashboard.json"

SUPPORTED_INR_FX_TICKERS = {
    "USD": "USDINR=X",
    "EUR": "EURINR=X",
    "GBP": "GBPINR=X",
    "INR": None,
}

CURRENCY_ALIASES = {
    "GBX": "GBP",
}

TRADING_DAY_RETURN_WINDOWS = {
    "daily_pct": 1,
    "five_day_pct": 5,
}

CALENDAR_RETURN_WINDOWS = {
    "one_month_pct": {"months": 1},
    "three_month_pct": {"months": 3},
    "one_year_pct": {"years": 1},
    "three_year_pct": {"years": 3},
    "five_year_pct": {"years": 5},
}

PERIOD_RULES = {
    "daily_pct": "1 trading session",
    "five_day_pct": "5 trading sessions",
    "one_month_pct": "1 calendar month, first trading day on or after the anchor date",
    "three_month_pct": "3 calendar months, first trading day on or after the anchor date",
    "one_year_pct": "1 calendar year, first trading day on or after the anchor date",
    "three_year_pct": "3 calendar years, first trading day on or after the anchor date",
    "five_year_pct": "5 calendar years, first trading day on or after the anchor date",
    "ytd_pct": "first trading day on or after January 1 of the latest series year",
}


@dataclass
class WatchItem:
    item_id: str
    tab_id: str
    tab_label: str
    group_id: str
    group_label: str
    asset_type: str
    ticker: str
    source_ticker: str
    fallback_name: str
    fallback_exchange: str
    fallback_currency: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to curated watchlist config JSON.")
    parser.add_argument(
        "--custom-config",
        default=DEFAULT_CUSTOM_CONFIG_PATH,
        help="Path to persisted custom watchlist config JSON.",
    )
    parser.add_argument("--out-dir", default=DEFAULT_OUTPUT_DIR, help="Directory for generated JSON files.")
    return parser.parse_args()


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json_atomic(path: str, payload: Dict[str, Any]) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    temp_handle = tempfile.NamedTemporaryFile("w", delete=False, dir=directory, encoding="utf-8")
    try:
        with temp_handle:
            json.dump(payload, temp_handle, indent=2)
            temp_handle.write("\n")
        os.replace(temp_handle.name, path)
    finally:
        if os.path.exists(temp_handle.name):
            try:
                os.unlink(temp_handle.name)
            except OSError:
                pass


def normalize_source_ticker(value: str) -> str:
    return value.strip().upper()


def normalize_currency_key(currency: Optional[str]) -> str:
    normalized = str(currency or "").strip().upper()
    return CURRENCY_ALIASES.get(normalized, normalized)


def supports_inr_return_currency(currency: Optional[str]) -> bool:
    return normalize_currency_key(currency) in SUPPORTED_INR_FX_TICKERS


def get_inr_fx_ticker_for_currency(currency: Optional[str]) -> Optional[str]:
    return SUPPORTED_INR_FX_TICKERS.get(normalize_currency_key(currency))


def display_ticker_from_source(source_ticker: str) -> str:
    normalized = normalize_source_ticker(source_ticker)
    if normalized.endswith(".L"):
        return normalized[:-2]
    return normalized


def build_item_id(tab_id: str, display_ticker: str) -> str:
    return "{}:{}".format(tab_id, display_ticker)


def default_custom_config(base_tabs: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "tabs": [
            {
                "id": tab["id"],
                "label": tab["label"],
                "asset_type": tab["asset_type"],
                "groups": [
                    {
                        "id": CUSTOM_GROUP_ID,
                        "label": CUSTOM_GROUP_LABEL,
                        "items": [],
                    }
                ],
            }
            for tab in base_tabs
        ]
    }


def normalize_custom_config(raw_custom_config: Dict[str, Any], base_tabs: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    default_config = default_custom_config(base_tabs)
    existing_tabs = {
        tab.get("id"): tab
        for tab in raw_custom_config.get("tabs", [])
        if isinstance(tab, dict) and tab.get("id")
    }

    normalized_tabs: List[Dict[str, Any]] = []
    for default_tab in default_config["tabs"]:
        existing_tab = existing_tabs.get(default_tab["id"], {})
        existing_items: List[Dict[str, Any]] = []
        for group in existing_tab.get("groups", []):
            if not isinstance(group, dict) or group.get("id") != CUSTOM_GROUP_ID:
                continue
            for item in group.get("items", []):
                if not isinstance(item, dict):
                    continue
                source_ticker = normalize_source_ticker(item.get("source_ticker", ""))
                display_ticker = item.get("ticker") or display_ticker_from_source(source_ticker)
                if not source_ticker or not display_ticker:
                    continue
                existing_items.append(
                    {
                        "ticker": display_ticker,
                        "source_ticker": source_ticker,
                        "name": item.get("name", display_ticker),
                        "exchange": item.get("exchange", ""),
                        "currency": item.get("currency", ""),
                    }
                )
            break

        normalized_tabs.append(
            {
                "id": default_tab["id"],
                "label": default_tab["label"],
                "asset_type": default_tab["asset_type"],
                "groups": [
                    {
                        "id": CUSTOM_GROUP_ID,
                        "label": CUSTOM_GROUP_LABEL,
                        "items": existing_items,
                    }
                ],
            }
        )

    return {"tabs": normalized_tabs}


def ensure_custom_config(custom_config_path: str, base_tabs: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if os.path.exists(custom_config_path):
        raw_custom_config = load_json(custom_config_path)
    else:
        raw_custom_config = {}

    normalized = normalize_custom_config(raw_custom_config, base_tabs)
    if raw_custom_config != normalized:
        write_json_atomic(custom_config_path, normalized)
    return normalized


def merge_tabs(curated_tabs: Sequence[Dict[str, Any]], custom_tabs: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    custom_map = {tab["id"]: tab for tab in custom_tabs}
    merged: List[Dict[str, Any]] = []
    for curated_tab in curated_tabs:
        tab_copy = {
            "id": curated_tab["id"],
            "label": curated_tab["label"],
            "asset_type": curated_tab["asset_type"],
            "groups": copy.deepcopy(curated_tab.get("groups", [])),
        }
        custom_tab = custom_map.get(curated_tab["id"])
        if custom_tab:
            tab_copy["groups"].extend(copy.deepcopy(custom_tab.get("groups", [])))
        merged.append(tab_copy)
    return merged


def load_watchlists(
    config_path: str = DEFAULT_CONFIG_PATH,
    custom_config_path: str = DEFAULT_CUSTOM_CONFIG_PATH,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    base_config = load_json(config_path)
    curated_tabs = base_config.get("tabs", [])
    custom_config = ensure_custom_config(custom_config_path, curated_tabs)
    custom_tabs = custom_config.get("tabs", [])
    merged_tabs = merge_tabs(curated_tabs, custom_tabs)
    return curated_tabs, custom_tabs, merged_tabs


def build_watch_items(merged_tabs: Sequence[Dict[str, Any]]) -> List[WatchItem]:
    items: List[WatchItem] = []
    seen_keys = set()
    for tab in merged_tabs:
        for group in tab.get("groups", []):
            for raw_item in group.get("items", []):
                source_ticker = normalize_source_ticker(raw_item.get("source_ticker", raw_item.get("ticker", "")))
                display_ticker = raw_item.get("ticker") or display_ticker_from_source(source_ticker)
                if not source_ticker or not display_ticker:
                    continue
                dedupe_key = (tab["id"], source_ticker)
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                items.append(
                    WatchItem(
                        item_id=build_item_id(tab["id"], display_ticker),
                        tab_id=tab["id"],
                        tab_label=tab["label"],
                        group_id=group["id"],
                        group_label=group["label"],
                        asset_type=tab["asset_type"],
                        ticker=display_ticker,
                        source_ticker=source_ticker,
                        fallback_name=raw_item.get("name", display_ticker),
                        fallback_exchange=raw_item.get("exchange", ""),
                        fallback_currency=raw_item.get("currency", ""),
                    )
                )
    return items


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def sanitize_history(history: pd.DataFrame) -> pd.DataFrame:
    if history is None or history.empty:
        return pd.DataFrame()

    cleaned = history.copy()
    cleaned.index = pd.to_datetime(cleaned.index).tz_localize(None)
    cleaned = cleaned.sort_index()
    if "Close" in cleaned.columns:
        cleaned["Close"] = pd.to_numeric(cleaned["Close"], errors="coerce")
    cleaned = cleaned.dropna(subset=["Close"])
    return cleaned


def load_single_history(source_ticker: str, period: str = LOOKBACK_PERIOD) -> pd.DataFrame:
    history = yf.Ticker(source_ticker).history(period=period, interval="1d", auto_adjust=False)
    return sanitize_history(history)


def extract_history_frame(batch_history: pd.DataFrame, source_ticker: str) -> pd.DataFrame:
    if batch_history.empty:
        return pd.DataFrame()

    if not isinstance(batch_history.columns, pd.MultiIndex):
        return sanitize_history(batch_history)

    level_0 = set(batch_history.columns.get_level_values(0))
    level_1 = set(batch_history.columns.get_level_values(1))

    if source_ticker in level_0:
        frame = batch_history[source_ticker]
        return sanitize_history(frame)

    if source_ticker in level_1:
        frame = batch_history.xs(source_ticker, axis=1, level=1, drop_level=True)
        return sanitize_history(frame)

    return pd.DataFrame()


def download_histories(source_tickers: Sequence[str]) -> Dict[str, pd.DataFrame]:
    unique_tickers = sorted(set(source_tickers))
    if not unique_tickers:
        return {}

    batch_history = pd.DataFrame()
    try:
        batch_history = yf.download(
            tickers=unique_tickers if len(unique_tickers) > 1 else unique_tickers[0],
            period=LOOKBACK_PERIOD,
            interval="1d",
            auto_adjust=False,
            group_by="ticker",
            progress=False,
            threads=False,
        )
    except Exception:
        batch_history = pd.DataFrame()

    history_map: Dict[str, pd.DataFrame] = {}
    for source_ticker in unique_tickers:
        history = extract_history_frame(batch_history, source_ticker)
        if history.empty:
            try:
                history = load_single_history(source_ticker)
            except Exception:
                history = pd.DataFrame()
        history_map[source_ticker] = history
    return history_map


def fetch_ticker_info(source_ticker: str, retries: int = 2) -> Tuple[Dict[str, Any], Optional[str]]:
    last_error: Optional[str] = None
    for attempt in range(retries + 1):
        try:
            info = yf.Ticker(source_ticker).info
            if isinstance(info, dict):
                return info, None
            return {}, None
        except Exception as exc:
            last_error = str(exc)
            if attempt < retries:
                time.sleep(0.6 * (attempt + 1))
    return {}, last_error


def previous_value(series: pd.Series, offset: int) -> Optional[float]:
    valid_series = series.dropna()
    if len(valid_series) <= offset:
        return None
    return safe_float(valid_series.iloc[-(offset + 1)])


def calc_window_return(series: pd.Series, offset: int) -> Optional[float]:
    current_price = previous_value(series, 0)
    past_price = previous_value(series, offset)
    if current_price is None or past_price in (None, 0):
        return None
    return ((current_price / past_price) - 1.0) * 100.0


def shift_calendar_anchor(date: pd.Timestamp, months: int = 0, years: int = 0) -> pd.Timestamp:
    return date - pd.DateOffset(months=months, years=years)


def find_anchor_position(valid_series: pd.Series, anchor_date: pd.Timestamp) -> Tuple[Optional[int], bool]:
    index = valid_series.index
    position = int(index.searchsorted(anchor_date, side="left"))
    if position >= len(index):
        return None, False

    anchor_timestamp = pd.Timestamp(index[position])
    has_sufficient_history = position > 0 or anchor_timestamp == anchor_date
    return position, has_sufficient_history


def calc_calendar_return(series: pd.Series, months: int = 0, years: int = 0) -> Optional[float]:
    valid_series = series.dropna()
    if valid_series.empty:
        return None

    current_price = safe_float(valid_series.iloc[-1])
    if current_price in (None, 0):
        return None

    latest_date = pd.Timestamp(valid_series.index[-1])
    anchor_date = shift_calendar_anchor(latest_date, months=months, years=years)
    anchor_position, has_sufficient_history = find_anchor_position(valid_series, anchor_date)
    if anchor_position is None or not has_sufficient_history:
        return None

    anchor_price = safe_float(valid_series.iloc[anchor_position])
    if anchor_price in (None, 0):
        return None
    return ((current_price / anchor_price) - 1.0) * 100.0


def calc_ytd_return(series: pd.Series) -> Optional[float]:
    valid_series = series.dropna()
    if valid_series.empty:
        return None
    current_price = safe_float(valid_series.iloc[-1])
    if current_price in (None, 0):
        return None

    latest_date = pd.Timestamp(valid_series.index[-1])
    year_start = pd.Timestamp(latest_date.year, 1, 1)
    start_position = int(valid_series.index.searchsorted(year_start, side="left"))
    if start_position >= len(valid_series):
        return None

    start_price = safe_float(valid_series.iloc[start_position])
    if start_price in (None, 0):
        return None
    return ((current_price / start_price) - 1.0) * 100.0


def extract_latest_price(series: pd.Series) -> Optional[float]:
    valid_series = series.dropna()
    if valid_series.empty:
        return None
    return safe_float(valid_series.iloc[-1])


def extract_row_meta(info: Dict[str, Any], item: WatchItem) -> Dict[str, Any]:
    name = (
        info.get("longName")
        or info.get("shortName")
        or info.get("displayName")
        or item.fallback_name
    )
    exchange = (
        info.get("fullExchangeName")
        or info.get("exchange")
        or item.fallback_exchange
    )
    currency = info.get("currency") or item.fallback_currency
    quote_type = (info.get("quoteType") or "").upper()
    aum = None
    if item.asset_type == "etf":
        for field in ("totalAssets", "netAssets", "fundNetAssets"):
            aum = safe_float(info.get(field))
            if aum is not None:
                break

    return {
        "name": name,
        "exchange": exchange,
        "currency": currency,
        "aum": aum,
        "quote_type": quote_type,
    }


def format_aum(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    for threshold, suffix in (
        (1_000_000_000_000, "T"),
        (1_000_000_000, "B"),
        (1_000_000, "M"),
    ):
        if value >= threshold:
            return "{:.1f}{}".format(value / threshold, suffix)
    return "{:,.0f}".format(value)


def build_chart_points(series: pd.Series) -> List[Dict[str, Any]]:
    return build_chart_points_with_inr(series, None)


def build_inr_close_series(
    close_series: pd.Series,
    currency: Optional[str],
    fx_histories: Dict[str, pd.DataFrame],
) -> pd.Series:
    valid_close = close_series.dropna()
    if valid_close.empty:
        return pd.Series(dtype="float64")

    currency_key = normalize_currency_key(currency)
    if currency_key == "INR":
        return valid_close.astype("float64")

    fx_ticker = get_inr_fx_ticker_for_currency(currency)
    if not fx_ticker:
        return pd.Series(dtype="float64")

    fx_history = fx_histories.get(fx_ticker, pd.DataFrame())
    fx_close = fx_history.get("Close", pd.Series(dtype="float64")).dropna()
    if fx_close.empty:
        return pd.Series(dtype="float64")

    aligned_fx = fx_close.reindex(valid_close.index, method="ffill").bfill()
    inr_close = pd.to_numeric(valid_close, errors="coerce") * aligned_fx
    return inr_close.astype("float64")


def build_chart_points_with_inr(local_series: pd.Series, inr_series: Optional[pd.Series]) -> List[Dict[str, Any]]:
    valid_local = local_series.dropna()
    if valid_local.empty:
        return []

    points: List[Dict[str, Any]] = []
    aligned_inr = None
    if inr_series is not None and not inr_series.empty:
        aligned_inr = inr_series.reindex(valid_local.index)

    for index, value in valid_local.items():
        close = safe_float(value)
        if close is None:
            continue
        inr_close = None
        if aligned_inr is not None:
            inr_close = safe_float(aligned_inr.loc[index])
        points.append(
            {
                "date": index.strftime("%Y-%m-%d"),
                "close": round(close, 4),
                "inr_close": round(inr_close, 4) if inr_close is not None else None,
            }
        )
    return points


def sort_rows(rows: Iterable[Dict[str, Any]], asset_type: str) -> List[Dict[str, Any]]:
    if asset_type == "etf":
        def key(row: Dict[str, Any]) -> Tuple[int, float, str]:
            aum = row.get("aum")
            if aum is None:
                return (1, 0.0, row["ticker"])
            return (0, -aum, row["ticker"])
    else:
        def key(row: Dict[str, Any]) -> Tuple[int, float, str]:
            perf = row.get("three_month_pct")
            if perf is None:
                return (1, 0.0, row["ticker"])
            return (0, -perf, row["ticker"])

    return sorted(rows, key=key)


def load_dashboard_payload(out_dir: str = DEFAULT_OUTPUT_DIR) -> Dict[str, Any]:
    return load_json(os.path.join(out_dir, DASHBOARD_FILENAME))


def build_dashboard_data(
    config_path: str = DEFAULT_CONFIG_PATH,
    custom_config_path: str = DEFAULT_CUSTOM_CONFIG_PATH,
    out_dir: str = DEFAULT_OUTPUT_DIR,
) -> Dict[str, Any]:
    os.makedirs(out_dir, exist_ok=True)

    curated_tabs, custom_tabs, merged_tabs = load_watchlists(config_path, custom_config_path)
    del curated_tabs, custom_tabs
    items = build_watch_items(merged_tabs)
    built_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    rows_by_tab_group: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    history_payload: Dict[str, Dict[str, Any]] = {}
    failures: List[Dict[str, str]] = []

    histories = download_histories([item.source_ticker for item in items])
    info_cache: Dict[str, Dict[str, Any]] = {}

    for source_ticker in sorted(set(item.source_ticker for item in items)):
        info, error = fetch_ticker_info(source_ticker)
        info_cache[source_ticker] = info
        if error:
            failures.append({"source_ticker": source_ticker, "stage": "info", "error": error})

    required_fx_tickers = sorted(
        {
            fx_ticker
            for item in items
            for fx_ticker in [get_inr_fx_ticker_for_currency(info_cache.get(item.source_ticker, {}).get("currency") or item.fallback_currency)]
            if fx_ticker
        }
    )
    fx_histories = download_histories(required_fx_tickers)
    for fx_ticker in required_fx_tickers:
        if fx_histories.get(fx_ticker, pd.DataFrame()).empty:
            failures.append(
                {
                    "source_ticker": fx_ticker,
                    "stage": "fx",
                    "error": "No FX history returned.",
                }
            )

    for item in items:
        rows_by_tab_group.setdefault(item.tab_id, {}).setdefault(item.group_id, [])
        history = histories.get(item.source_ticker, pd.DataFrame())
        if history.empty:
            failures.append(
                {
                    "source_ticker": item.source_ticker,
                    "item_id": item.item_id,
                    "stage": "history",
                    "error": "No price history returned.",
                }
            )

        close_series = history.get("Close", pd.Series(dtype="float64"))
        latest_price = extract_latest_price(close_series)
        row_meta = extract_row_meta(info_cache.get(item.source_ticker, {}), item)
        inr_close_series = build_inr_close_series(close_series, row_meta["currency"], fx_histories)
        if not supports_inr_return_currency(row_meta["currency"]):
            failures.append(
                {
                    "source_ticker": item.source_ticker,
                    "item_id": item.item_id,
                    "stage": "currency",
                    "error": "Unsupported INR conversion currency: {}.".format(row_meta["currency"] or "unknown"),
                }
            )
        elif not close_series.dropna().empty and inr_close_series.empty:
            failures.append(
                {
                    "source_ticker": item.source_ticker,
                    "item_id": item.item_id,
                    "stage": "fx_alignment",
                    "error": "Unable to align FX history for INR return calculations.",
                }
            )

        row = {
            "item_id": item.item_id,
            "ticker": item.ticker,
            "source_ticker": item.source_ticker,
            "name": row_meta["name"],
            "asset_type": item.asset_type,
            "exchange": row_meta["exchange"],
            "currency": row_meta["currency"],
            "last_price": latest_price,
            "daily_pct": calc_window_return(inr_close_series, TRADING_DAY_RETURN_WINDOWS["daily_pct"]),
            "five_day_pct": calc_window_return(inr_close_series, TRADING_DAY_RETURN_WINDOWS["five_day_pct"]),
            "one_month_pct": calc_calendar_return(inr_close_series, **CALENDAR_RETURN_WINDOWS["one_month_pct"]),
            "three_month_pct": calc_calendar_return(inr_close_series, **CALENDAR_RETURN_WINDOWS["three_month_pct"]),
            "one_year_pct": calc_calendar_return(inr_close_series, **CALENDAR_RETURN_WINDOWS["one_year_pct"]),
            "three_year_pct": calc_calendar_return(inr_close_series, **CALENDAR_RETURN_WINDOWS["three_year_pct"]),
            "five_year_pct": calc_calendar_return(inr_close_series, **CALENDAR_RETURN_WINDOWS["five_year_pct"]),
            "ytd_pct": calc_ytd_return(inr_close_series),
            "aum": row_meta["aum"],
            "aum_display": format_aum(row_meta["aum"]) if item.asset_type == "etf" else "",
        }
        rows_by_tab_group[item.tab_id][item.group_id].append(row)
        history_payload[item.item_id] = {
            "item_id": item.item_id,
            "ticker": item.ticker,
            "source_ticker": item.source_ticker,
            "name": row_meta["name"],
            "currency": row_meta["currency"],
            "points": build_chart_points_with_inr(close_series, inr_close_series),
        }

    snapshot_tabs: List[Dict[str, Any]] = []
    for tab in merged_tabs:
        groups: List[Dict[str, Any]] = []
        for group in tab.get("groups", []):
            rows = rows_by_tab_group.get(tab["id"], {}).get(group["id"], [])
            groups.append(
                {
                    "id": group["id"],
                    "label": group["label"],
                    "rows": sort_rows(rows, tab["asset_type"]),
                }
            )
        snapshot_tabs.append(
            {
                "id": tab["id"],
                "label": tab["label"],
                "asset_type": tab["asset_type"],
                "groups": groups,
            }
        )

    snapshot_payload = {
        "built_at": built_at,
        "tabs": snapshot_tabs,
    }
    history_document = {
        "built_at": built_at,
        "series": history_payload,
    }
    meta_payload = {
        "built_at": built_at,
        "return_basis": RETURN_BASIS,
        "return_basis_label": "INR-adjusted",
        "period_basis": PERIOD_BASIS,
        "period_basis_label": "Yahoo-style calendar anchors",
        "period_rules": PERIOD_RULES,
        "chart_range_rules": {
            "1M": PERIOD_RULES["one_month_pct"],
            "3M": PERIOD_RULES["three_month_pct"],
            "6M": "6 calendar months, first trading day on or after the anchor date",
            "1Y": PERIOD_RULES["one_year_pct"],
            "3Y": PERIOD_RULES["three_year_pct"],
            "5Y": PERIOD_RULES["five_year_pct"],
        },
        "supported_inr_fx_pairs": {
            currency: fx_ticker if fx_ticker is not None else "IDENTITY"
            for currency, fx_ticker in SUPPORTED_INR_FX_TICKERS.items()
        },
        "tabs": [
            {
                "id": tab["id"],
                "label": tab["label"],
                "asset_type": tab["asset_type"],
            }
            for tab in merged_tabs
        ],
        "columns": [
            {"key": "ticker", "label": "Ticker", "type": "string"},
            {"key": "name", "label": "Name", "type": "string"},
            {"key": "last_price", "label": "Last", "type": "number"},
            {"key": "daily_pct", "label": "1D INR", "type": "number"},
            {"key": "five_day_pct", "label": "5D INR", "type": "number"},
            {"key": "one_month_pct", "label": "1M INR", "type": "number"},
            {"key": "three_month_pct", "label": "3M INR", "type": "number"},
            {"key": "one_year_pct", "label": "1Y INR", "type": "number"},
            {"key": "three_year_pct", "label": "3Y INR", "type": "number"},
            {"key": "five_year_pct", "label": "5Y INR", "type": "number"},
            {"key": "ytd_pct", "label": "YTD INR", "type": "number"},
            {"key": "aum", "label": "AUM", "type": "currency_large", "asset_types": ["etf"]},
        ],
        "failures": failures,
    }
    dashboard_payload = {
        "built_at": built_at,
        "snapshot": snapshot_payload,
        "history": history_document,
        "meta": meta_payload,
    }

    write_json_atomic(os.path.join(out_dir, SNAPSHOT_FILENAME), snapshot_payload)
    write_json_atomic(os.path.join(out_dir, HISTORY_FILENAME), history_document)
    write_json_atomic(os.path.join(out_dir, META_FILENAME), meta_payload)
    write_json_atomic(os.path.join(out_dir, DASHBOARD_FILENAME), dashboard_payload)
    return dashboard_payload


def main() -> None:
    args = parse_args()
    dashboard_payload = build_dashboard_data(args.config, args.custom_config, args.out_dir)
    instrument_count = sum(
        len(group["rows"])
        for tab in dashboard_payload["snapshot"]["tabs"]
        for group in tab["groups"]
    )
    print("Generated dashboard data for {} instruments into {}".format(instrument_count, args.out_dir))


if __name__ == "__main__":
    main()
