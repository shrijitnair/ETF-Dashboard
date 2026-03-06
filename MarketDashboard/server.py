#!/usr/bin/env python3
"""Flask server for the backend-hosted market dashboard."""

from __future__ import annotations

import copy
import os
from contextlib import contextmanager
from fcntl import LOCK_EX, LOCK_UN, flock
from typing import Any, Dict, Optional

from flask import Flask, jsonify, request, send_from_directory

from scripts.build_data import (
    CUSTOM_GROUP_ID,
    DASHBOARD_FILENAME,
    DEFAULT_CONFIG_PATH,
    DEFAULT_CUSTOM_CONFIG_PATH,
    DEFAULT_OUTPUT_DIR,
    build_dashboard_data,
    build_item_id,
    display_ticker_from_source,
    fetch_ticker_info,
    load_dashboard_payload,
    load_json,
    load_single_history,
    load_watchlists,
    normalize_custom_config,
    normalize_currency_key,
    normalize_source_ticker,
    supports_inr_return_currency,
    write_json_atomic,
)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, DEFAULT_CONFIG_PATH)
CUSTOM_CONFIG_PATH = os.path.join(BASE_DIR, DEFAULT_CUSTOM_CONFIG_PATH)
OUTPUT_DIR = os.path.join(BASE_DIR, DEFAULT_OUTPUT_DIR)
LOCK_PATH = os.path.join(BASE_DIR, "config", ".dashboard.lock")

app = Flask(__name__, static_folder=BASE_DIR, static_url_path="")


def dashboard_path() -> str:
    return os.path.join(OUTPUT_DIR, DASHBOARD_FILENAME)


def ensure_dashboard_payload() -> Dict[str, Any]:
    if not os.path.exists(dashboard_path()):
        return build_dashboard_data(CONFIG_PATH, CUSTOM_CONFIG_PATH, OUTPUT_DIR)
    return load_dashboard_payload(OUTPUT_DIR)


def api_error(reason: str, message: str, status_code: int):
    response = jsonify({"ok": False, "reason": reason, "message": message})
    response.status_code = status_code
    return response


@contextmanager
def dashboard_lock():
    os.makedirs(os.path.dirname(LOCK_PATH), exist_ok=True)
    handle = open(LOCK_PATH, "w", encoding="utf-8")
    try:
        flock(handle, LOCK_EX)
        yield
    finally:
        flock(handle, LOCK_UN)
        handle.close()


def get_tab_definition(tab_id: str) -> Optional[Dict[str, Any]]:
    curated_tabs, _, _ = load_watchlists(CONFIG_PATH, CUSTOM_CONFIG_PATH)
    for tab in curated_tabs:
        if tab["id"] == tab_id:
            return tab
    return None


def source_ticker_exists(merged_tabs, source_ticker: str) -> bool:
    normalized = normalize_source_ticker(source_ticker)
    for tab in merged_tabs:
        for group in tab.get("groups", []):
            for item in group.get("items", []):
                existing_source = normalize_source_ticker(item.get("source_ticker", item.get("ticker", "")))
                if existing_source == normalized:
                    return True
    return False


def append_custom_item(custom_config: Dict[str, Any], tab_id: str, item: Dict[str, Any]) -> Dict[str, Any]:
    updated = copy.deepcopy(custom_config)
    for tab in updated["tabs"]:
        if tab["id"] != tab_id:
            continue
        for group in tab.get("groups", []):
            if group.get("id") == CUSTOM_GROUP_ID:
                group.setdefault("items", []).append(item)
                return updated
    raise ValueError("Custom group missing for tab {}".format(tab_id))


def find_row_by_item_id(dashboard_payload: Dict[str, Any], item_id: str) -> Optional[Dict[str, Any]]:
    for tab in dashboard_payload["snapshot"]["tabs"]:
        for group in tab["groups"]:
            for row in group["rows"]:
                if row["item_id"] == item_id:
                    return row
    return None


def count_instruments(dashboard_payload: Dict[str, Any]) -> int:
    return sum(
        len(group["rows"])
        for tab in dashboard_payload["snapshot"]["tabs"]
        for group in tab["groups"]
    )


@app.after_request
def disable_api_cache(response):
    if request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/")
def index():
    return send_from_directory(BASE_DIR, "index.html")


@app.get("/api/dashboard")
def get_dashboard():
    return jsonify(ensure_dashboard_payload())


@app.post("/api/refresh")
def refresh_dashboard():
    try:
        with dashboard_lock():
            dashboard_payload = build_dashboard_data(CONFIG_PATH, CUSTOM_CONFIG_PATH, OUTPUT_DIR)
    except Exception as exc:
        return api_error("build_failed", "The dashboard rebuild failed: {}.".format(exc), 500)

    return jsonify(
        {
            "ok": True,
            "built_at": dashboard_payload["built_at"],
            "instrument_count": count_instruments(dashboard_payload),
        }
    )


@app.post("/api/instruments")
def add_instrument():
    payload = request.get_json(silent=True) or {}
    tab_id = str(payload.get("tab_id", "")).strip()
    source_ticker = normalize_source_ticker(str(payload.get("ticker", "")).strip())
    if not tab_id or not source_ticker:
        return api_error("invalid_request", "tab_id and ticker are required.", 400)

    tab_definition = get_tab_definition(tab_id)
    if not tab_definition:
        return api_error("invalid_tab", "Unknown dashboard tab: {}.".format(tab_id), 400)

    if tab_id == "ucits-etfs-lse" and not source_ticker.endswith(".L"):
        return api_error("invalid_ticker", "UCITS additions must use the Yahoo LSE ticker, for example CSPX.L.", 400)

    info, info_error = fetch_ticker_info(source_ticker)
    try:
        history = load_single_history(source_ticker)
    except Exception as exc:
        return api_error("invalid_ticker", "Yahoo Finance did not return price history for {}: {}.".format(source_ticker, exc), 400)
    if history.empty:
        message = "No Yahoo Finance price history was returned for {}.".format(source_ticker)
        if info_error:
            message = "{} {}".format(message, info_error)
        return api_error("invalid_ticker", message, 400)

    if not info and info_error:
        return api_error("invalid_ticker", "Yahoo Finance metadata is unavailable for {}.".format(source_ticker), 400)

    quote_type = str(info.get("quoteType", "")).upper()
    asset_type = tab_definition["asset_type"]
    if asset_type == "etf" and quote_type != "ETF":
        return api_error(
            "asset_type_mismatch",
            "{} is not reported by Yahoo Finance as an ETF.".format(source_ticker),
            400,
        )
    if asset_type == "stock" and quote_type == "ETF":
        return api_error(
            "asset_type_mismatch",
            "{} is reported by Yahoo Finance as an ETF, not an equity.".format(source_ticker),
            400,
        )

    currency = str(info.get("currency", "")).upper()
    if not supports_inr_return_currency(currency):
        return api_error(
            "unsupported_currency",
            "{} is quoted in {} on Yahoo Finance. Only currencies with INR FX support are accepted.".format(
                source_ticker,
                currency or "an unknown currency",
            ),
            400,
        )

    display_ticker = display_ticker_from_source(source_ticker)
    fallback_name = (
        info.get("longName")
        or info.get("shortName")
        or info.get("displayName")
        or display_ticker
    )
    fallback_exchange = info.get("fullExchangeName") or info.get("exchange") or ""
    custom_item = {
        "ticker": display_ticker,
        "source_ticker": source_ticker,
        "name": fallback_name,
        "exchange": fallback_exchange,
        "currency": normalize_currency_key(currency or ""),
    }

    with dashboard_lock():
        curated_tabs, _, merged_tabs = load_watchlists(CONFIG_PATH, CUSTOM_CONFIG_PATH)
        if source_ticker_exists(merged_tabs, source_ticker):
            return api_error("duplicate", "{} is already tracked in the dashboard.".format(source_ticker), 409)

        raw_custom_config = load_json(CUSTOM_CONFIG_PATH) if os.path.exists(CUSTOM_CONFIG_PATH) else {}
        normalized_custom_config = normalize_custom_config(raw_custom_config, curated_tabs)
        original_custom_config = copy.deepcopy(normalized_custom_config)
        updated_custom_config = append_custom_item(normalized_custom_config, tab_id, custom_item)
        write_json_atomic(CUSTOM_CONFIG_PATH, updated_custom_config)

        try:
            dashboard_payload = build_dashboard_data(CONFIG_PATH, CUSTOM_CONFIG_PATH, OUTPUT_DIR)
        except Exception as exc:
            write_json_atomic(CUSTOM_CONFIG_PATH, original_custom_config)
            return api_error("build_failed", "The dashboard rebuild failed: {}.".format(exc), 500)

    item_id = build_item_id(tab_id, display_ticker)
    row = find_row_by_item_id(dashboard_payload, item_id)
    return jsonify(
        {
            "ok": True,
            "built_at": dashboard_payload["built_at"],
            "item": row,
            "tab_id": tab_id,
        }
    )


if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8123"))
    app.run(host=host, port=port)
