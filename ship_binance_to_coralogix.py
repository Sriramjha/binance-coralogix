#!/usr/bin/env python3
"""
Binance account / exchange activity → Coralogix Logs API.

Polls USER_DATA history endpoints for the last N minutes and ships events to:
  POST https://ingress.<domain>/logs/v1/singles

Default sources (no trading permission required):
  GET /sapi/v1/capital/deposit/hisrec
  GET /sapi/v1/capital/withdraw/history
  GET /sapi/v1/asset/transfer
  GET /sapi/v1/c2c/orderMatch/listUserOrderHistory

Optional (need BINANCE_SYMBOLS):
  GET /api/v3/myTrades
  GET /api/v3/allOrders

Usage:
  cp .env.example .env
  pip install -r requirements.txt
  python ship_binance_to_coralogix.py --once --lookback-minutes 5   # cron
  python ship_binance_to_coralogix.py                               # loop

API key needs Enable Reading only. Do not enable trade or withdraw.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

LOGGER = logging.getLogger("binance-coralogix")

SEEN_ID_LIMIT = 4000
OVERLAP_SECONDS = 2
DEFAULT_PAGE_LIMIT = 1000
TRANSFER_PAGE_SIZE = 100
DAY_MS = 24 * 60 * 60 * 1000

# Binance startTime/endTime spans must stay strictly below these documented maxima.
SOURCE_MAX_WINDOW_MS = {
    "deposit": 90 * DAY_MS - 1000,
    "withdraw": 90 * DAY_MS - 1000,
    "transfer": 7 * DAY_MS - 1000,
    "c2c": 30 * DAY_MS - 1000,
    "trade": DAY_MS - 1000,
    "order": DAY_MS - 1000,
}

C2C_PAGE_SIZE = 100

DEFAULT_SOURCES = ["deposit", "withdraw", "transfer", "c2c"]
OPTIONAL_SOURCES = {"trade", "order"}
KNOWN_SOURCES = set(DEFAULT_SOURCES) | OPTIONAL_SOURCES

# Common wallet-to-wallet moves. Override with BINANCE_TRANSFER_TYPES.
DEFAULT_TRANSFER_TYPES = [
    "MAIN_UMFUTURE",
    "UMFUTURE_MAIN",
    "MAIN_CMFUTURE",
    "CMFUTURE_MAIN",
    "MAIN_MARGIN",
    "MARGIN_MAIN",
    "MAIN_FUNDING",
    "FUNDING_MAIN",
    "MAIN_PORTFOLIO_MARGIN",
    "PORTFOLIO_MARGIN_MAIN",
]

# Coralogix severity: 1 Debug … 6 Critical
WITHDRAW_STATUS_SEVERITY = {
    0: 4,  # email sent
    2: 4,  # awaiting approval
    3: 5,  # rejected
    4: 3,  # processing
    6: 3,  # completed
}
DEPOSIT_STATUS_SEVERITY = {
    0: 4,  # pending
    1: 3,  # success
    6: 4,  # credited but cannot withdraw
}
ORDER_STATUS_SEVERITY = {
    "NEW": 3,
    "PARTIALLY_FILLED": 3,
    "FILLED": 3,
    "CANCELED": 3,
    "PENDING_CANCEL": 3,
    "REJECTED": 5,
    "EXPIRED": 4,
    "EXPIRED_IN_MATCH": 4,
}
C2C_STATUS_SEVERITY = {
    "PENDING": 4,
    "TRADING": 4,
    "BUYER_PAYED": 4,
    "BUYER_PAID": 4,
    "DISTRIBUTING": 4,
    "COMPLETED": 3,
    "IN_APPEAL": 5,
    "CANCELLED": 4,
    "CANCELED": 4,
    "CANCELLED_BY_SYSTEM": 4,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_ms() -> int:
    return int(utc_now().timestamp() * 1000)


def parse_ts_to_ms(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value * 1000) if value < 10_000_000_000 else int(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.isdigit():
        return parse_ts_to_ms(int(text))
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def env_csv(name: str, default: Optional[List[str]] = None) -> List[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return list(default or [])
    return [part.strip() for part in raw.split(",") if part.strip()]


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def event_timestamp_ms(event: Dict[str, Any]) -> Optional[int]:
    for key in (
        "insertTime",
        "applyTime",
        "completeTime",
        "timestamp",
        "time",
        "updateTime",
        "createTime",
        "createdAt",
    ):
        parsed = parse_ts_to_ms(event.get(key))
        if parsed is not None:
            return parsed
    return None


def event_id_for(source: str, event: Dict[str, Any]) -> str:
    explicit = event.get("cx_event_id")
    if explicit:
        return str(explicit)

    if source == "deposit":
        return str(event.get("id") or event.get("txId") or "")
    if source == "withdraw":
        return str(event.get("id") or event.get("withdrawOrderId") or event.get("txId") or "")
    if source == "transfer":
        return str(event.get("tranId") or event.get("id") or "")
    if source == "c2c":
        return str(event.get("orderNumber") or event.get("orderNo") or event.get("id") or "")
    if source == "trade":
        symbol = event.get("symbol") or ""
        trade_id = event.get("id")
        return f"{symbol}:{trade_id}" if trade_id is not None else ""
    if source == "order":
        symbol = event.get("symbol") or ""
        order_id = event.get("orderId")
        update = event.get("updateTime") or event.get("time")
        if order_id is None:
            return ""
        return f"{symbol}:{order_id}:{update}"
    return str(event.get("id") or "")


def normalize_source(value: Any) -> str:
    text = str(value or "").strip().lower()
    for prefix in ("binance_", "binance-"):
        if text.startswith(prefix):
            return text[len(prefix) :]
    return text


def map_severity(source: str, event: Dict[str, Any]) -> int:
    kind = normalize_source(source) or normalize_source(event.get("event_source"))
    if kind == "withdraw":
        try:
            return WITHDRAW_STATUS_SEVERITY.get(int(event.get("status")), 4)
        except (TypeError, ValueError):
            return 4
    if kind == "deposit":
        try:
            return DEPOSIT_STATUS_SEVERITY.get(int(event.get("status")), 3)
        except (TypeError, ValueError):
            return 3
    if kind == "order":
        status = str(event.get("status") or "").upper()
        return ORDER_STATUS_SEVERITY.get(status, 3)
    if kind == "c2c":
        status = str(event.get("orderStatus") or event.get("status") or "").upper()
        return C2C_STATUS_SEVERITY.get(status, 3)
    return 3


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                self.data.update(loaded)
        except (OSError, json.JSONDecodeError) as exc:
            LOGGER.warning("Could not load state file %s: %s", self.path, exc)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)

    def _bucket(self, key: str) -> Dict[str, Any]:
        bucket = self.data.get(key)
        if not isinstance(bucket, dict):
            bucket = {"seen_ids": [], "last_time_ms": None}
            self.data[key] = bucket
        bucket.setdefault("seen_ids", [])
        bucket.setdefault("last_time_ms", None)
        return bucket

    def get_seen_ids(self, key: str) -> Set[str]:
        return {str(x) for x in (self._bucket(key).get("seen_ids") or [])}

    def get_last_time_ms(self, key: str) -> Optional[int]:
        value = self._bucket(key).get("last_time_ms")
        return int(value) if value is not None else None

    def update(self, key: str, event_ids: Iterable[str], last_time_ms: Optional[int]) -> None:
        bucket = self._bucket(key)
        merged = list(dict.fromkeys(list(event_ids) + list(bucket.get("seen_ids") or [])))
        bucket["seen_ids"] = merged[:SEEN_ID_LIMIT]
        if last_time_ms is not None:
            prev = bucket.get("last_time_ms")
            bucket["last_time_ms"] = (
                max(int(prev), last_time_ms) if prev is not None else last_time_ms
            )


class BinanceClient:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str,
        *,
        recv_window: int = 10000,
        timeout: float = 60.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.base_url = base_url.rstrip("/")
        self.recv_window = max(1, min(int(recv_window), 60000))
        self.timeout = timeout
        self.session = session or requests.Session()
        self.time_offset_ms = 0
        self.headers = {
            "X-MBX-APIKEY": api_key,
            "Accept": "application/json",
        }

    def sync_time(self) -> None:
        payload = self._request("GET", "/api/v3/time", signed=False)
        server_time = payload.get("serverTime")
        if server_time is None:
            LOGGER.warning("Could not sync Binance server time")
            return
        self.time_offset_ms = int(server_time) - utc_now_ms()
        LOGGER.debug("Binance time offset=%sms", self.time_offset_ms)

    def _timestamp(self) -> int:
        return utc_now_ms() + self.time_offset_ms

    def _sign(self, query_string: str) -> str:
        return hmac.new(self.api_secret, query_string.encode("utf-8"), hashlib.sha256).hexdigest()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = True,
        max_retries: int = 6,
    ) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(max_retries):
            query: Dict[str, Any] = {}
            if params:
                query.update({k: v for k, v in params.items() if v is not None})
            url = f"{self.base_url}{path}"
            if signed:
                query["recvWindow"] = self.recv_window
                query["timestamp"] = self._timestamp()
                query_string = urlencode(query, doseq=True)
                url = f"{url}?{query_string}&signature={self._sign(query_string)}"
                request_params: Optional[Dict[str, Any]] = None
            else:
                request_params = query or None

            try:
                resp = self.session.request(
                    method,
                    url,
                    headers=self.headers,
                    params=request_params,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_error = exc
                sleep_for = min(2**attempt, 30)
                LOGGER.warning("Request error (%s); retrying in %ss", exc, sleep_for)
                time.sleep(sleep_for)
                continue

            if resp.status_code == 429 or resp.status_code == 418:
                retry_after = resp.headers.get("Retry-After")
                try:
                    sleep_for = max(1, int(float(retry_after))) if retry_after else min(2**attempt, 60)
                except ValueError:
                    sleep_for = min(2**attempt, 60)
                LOGGER.warning("Rate limited by Binance (%s); sleeping %ss", resp.status_code, sleep_for)
                time.sleep(sleep_for)
                continue

            if resp.status_code >= 500:
                sleep_for = min(2**attempt, 30)
                LOGGER.warning("Binance %s on %s; retrying in %ss", resp.status_code, path, sleep_for)
                time.sleep(sleep_for)
                continue

            body: Any = {}
            if resp.content:
                try:
                    body = resp.json()
                except ValueError:
                    body = {"raw": resp.text[:500]}

            if isinstance(body, dict) and body.get("code") == -1021 and attempt < max_retries - 1:
                LOGGER.warning("Timestamp outside recvWindow; resyncing clock")
                try:
                    self.sync_time()
                except Exception:
                    LOGGER.exception("Clock resync failed")
                time.sleep(1)
                continue

            if not resp.ok:
                raise RuntimeError(self._format_api_error(path, resp.status_code, body, resp.text))

            if isinstance(body, dict) and "code" in body and "msg" in body and "rows" not in body:
                # Some Binance errors still return HTTP 200
                if int(body.get("code") or 0) < 0:
                    raise RuntimeError(self._format_api_error(path, resp.status_code, body, resp.text))
            return body

        raise RuntimeError(f"Binance API failed after retries for {path}: {last_error}")

    @staticmethod
    def _format_api_error(path: str, status: int, body: Any, text: str) -> str:
        code = body.get("code") if isinstance(body, dict) else None
        msg = body.get("msg") if isinstance(body, dict) else None
        hint = ""
        if code in (-2015, -2014, -2008):
            hint = (
                " Check BINANCE_API_KEY / BINANCE_API_SECRET, Enable Reading, "
                "and that this host IP is allowlisted on the key."
            )
        elif code == -1021:
            hint = " Host clock is out of sync with Binance. Enable NTP or increase BINANCE_RECV_WINDOW."
        elif code == -1002:
            hint = " You are not authorized to execute this request. Enable Reading on the API key."
        detail = f" code={code} msg={msg}" if code is not None else f" {text[:400]}"
        return f"Binance API error {status} for {path}:{detail}.{hint}"

    def fetch_deposits(self, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        offset = 0
        while True:
            page = self._request(
                "GET",
                "/sapi/v1/capital/deposit/hisrec",
                params={
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "offset": offset,
                    "limit": DEFAULT_PAGE_LIMIT,
                },
            )
            if not isinstance(page, list) or not page:
                break
            events.extend(page)
            if len(page) < DEFAULT_PAGE_LIMIT:
                break
            offset += len(page)
        return events

    def fetch_withdrawals(self, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        offset = 0
        while True:
            page = self._request(
                "GET",
                "/sapi/v1/capital/withdraw/history",
                params={
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "offset": offset,
                    "limit": DEFAULT_PAGE_LIMIT,
                },
            )
            if not isinstance(page, list) or not page:
                break
            events.extend(page)
            if len(page) < DEFAULT_PAGE_LIMIT:
                break
            offset += len(page)
        return events

    def fetch_transfers(self, transfer_type: str, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        current = 1
        while True:
            payload = self._request(
                "GET",
                "/sapi/v1/asset/transfer",
                params={
                    "type": transfer_type,
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "current": current,
                    "size": TRANSFER_PAGE_SIZE,
                },
            )
            rows = payload.get("rows") if isinstance(payload, dict) else None
            if not rows:
                break
            events.extend(rows)
            total = int(payload.get("total") or 0)
            if current * TRANSFER_PAGE_SIZE >= total or len(rows) < TRANSFER_PAGE_SIZE:
                break
            current += 1
        return events

    def fetch_trades(self, symbol: str, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
        page = self._request(
            "GET",
            "/api/v3/myTrades",
            params={
                "symbol": symbol,
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": DEFAULT_PAGE_LIMIT,
            },
        )
        events = list(page) if isinstance(page, list) else []
        if len(events) >= DEFAULT_PAGE_LIMIT:
            LOGGER.warning(
                "Trade %s returned %s rows (API max). Narrow BINANCE_SYMBOLS or lookback.",
                symbol,
                len(events),
            )
        return events

    def fetch_orders(self, symbol: str, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
        page = self._request(
            "GET",
            "/api/v3/allOrders",
            params={
                "symbol": symbol,
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": DEFAULT_PAGE_LIMIT,
            },
        )
        events = list(page) if isinstance(page, list) else []
        if len(events) >= DEFAULT_PAGE_LIMIT:
            LOGGER.warning(
                "Orders %s returned %s rows (API max). Narrow BINANCE_SYMBOLS or lookback.",
                symbol,
                len(events),
            )
        return events

    def fetch_c2c_orders(self, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        page = 1
        while True:
            payload = self._request(
                "GET",
                "/sapi/v1/c2c/orderMatch/listUserOrderHistory",
                params={
                    "startTimestamp": start_ms,
                    "endTimestamp": end_ms,
                    "page": page,
                    "rows": C2C_PAGE_SIZE,
                },
            )
            rows = self._c2c_rows(payload)
            events.extend(rows)
            total = self._c2c_total(payload)
            if not rows or len(rows) < C2C_PAGE_SIZE:
                break
            if total and page * C2C_PAGE_SIZE >= total:
                break
            page += 1
        return events

    @staticmethod
    def _c2c_rows(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if not isinstance(payload, dict):
            return []
        data = payload.get("data")
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            rows = data.get("data") or data.get("list") or data.get("rows") or []
            return [row for row in rows if isinstance(row, dict)]
        return []

    @staticmethod
    def _c2c_total(payload: Any) -> int:
        if not isinstance(payload, dict):
            return 0
        if payload.get("total") is not None:
            try:
                return int(payload["total"])
            except (TypeError, ValueError):
                return 0
        data = payload.get("data")
        if isinstance(data, dict) and data.get("total") is not None:
            try:
                return int(data["total"])
            except (TypeError, ValueError):
                return 0
        return 0


class CoralogixShipper:
    def __init__(
        self,
        api_key: str,
        domain: str,
        application_name: str,
        subsystem_name: str,
        batch_size: int = 200,
        timeout: float = 60.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        domain = domain.lstrip(".").strip()
        self.url = f"https://ingress.{domain}/logs/v1/singles"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        self.application_name = application_name
        self.subsystem_name = subsystem_name
        self.batch_size = max(1, batch_size)
        self.timeout = timeout
        self.session = session or requests.Session()

    def _to_cx_record(self, event: Dict[str, Any], *, category: str) -> Dict[str, Any]:
        ts = event_timestamp_ms(event) or utc_now_ms()
        computer = (
            event.get("network")
            or event.get("coin")
            or event.get("asset")
            or event.get("symbol")
        )
        record: Dict[str, Any] = {
            "applicationName": self.application_name,
            "subsystemName": self.subsystem_name,
            "severity": map_severity(event.get("event_source") or event.get("cx_category"), event),
            "category": category,
            "timestamp": ts,
            "text": json.dumps(event, separators=(",", ":"), default=str),
        }
        if computer:
            record["computerName"] = str(computer)
        return record

    def ship(self, events: Iterable[Dict[str, Any]], *, category: str) -> int:
        batch: List[Dict[str, Any]] = []
        sent = 0

        def flush() -> None:
            nonlocal sent, batch
            if not batch:
                return
            resp = self.session.post(
                self.url,
                headers=self.headers,
                data=json.dumps(batch, separators=(",", ":")),
                timeout=self.timeout,
            )
            if not resp.ok:
                raise RuntimeError(
                    f"Coralogix ingest error {resp.status_code}: {resp.text[:500]}"
                )
            sent += len(batch)
            LOGGER.info("Shipped %s logs to Coralogix (%s)", len(batch), category)
            batch = []

        for event in events:
            enriched = dict(event)
            enriched.setdefault("cx_source", "binance")
            enriched.setdefault("cx_category", category)
            batch.append(self._to_cx_record(enriched, category=category))
            if len(batch) >= self.batch_size:
                flush()
        flush()
        return sent


def window_start_ms(state: StateStore, key: str, lookback_minutes: int) -> int:
    now_ms = utc_now_ms()
    lookback_ms = lookback_minutes * 60 * 1000
    window_start = now_ms - lookback_ms
    overlap_ms = OVERLAP_SECONDS * 1000
    last_time_ms = state.get_last_time_ms(key)
    if last_time_ms is not None and last_time_ms < window_start:
        LOGGER.info("Catching up %s from last checkpoint (gap larger than lookback)", key)
        return max(0, last_time_ms - overlap_ms)
    return max(0, window_start - overlap_ms)


def iter_time_windows(start_ms: int, end_ms: int, max_span_ms: int) -> List[tuple[int, int]]:
    if start_ms >= end_ms or max_span_ms <= 0:
        return []
    windows: List[tuple[int, int]] = []
    cursor = start_ms
    while cursor < end_ms:
        chunk_end = min(cursor + max_span_ms, end_ms)
        windows.append((cursor, chunk_end))
        cursor = chunk_end
    return windows


def filter_new_events(
    source: str,
    events: List[Dict[str, Any]],
    seen_ids: Set[str],
    extra: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        enriched = dict(event)
        if extra:
            enriched.update(extra)
        enriched["event_source"] = f"binance_{source}"
        event_id = event_id_for(source, enriched)
        if event_id and event_id in seen_ids:
            continue
        if event_id:
            enriched["cx_event_id"] = event_id
        kept.append(enriched)
    return kept


def ship_source(
    source: str,
    events: List[Dict[str, Any]],
    shipper: CoralogixShipper,
    state: StateStore,
    state_key: str,
    *,
    dry_run: bool,
    window_end_ms: Optional[int] = None,
) -> int:
    new_ids: List[str] = []
    last_ts = state.get_last_time_ms(state_key)
    for event in events:
        event_id = str(event.get("cx_event_id") or event_id_for(source, event) or "")
        if event_id:
            new_ids.append(event_id)
        event_ts = event_timestamp_ms(event)
        if event_ts is not None:
            last_ts = event_ts if last_ts is None else max(last_ts, event_ts)
    if window_end_ms is not None:
        last_ts = window_end_ms if last_ts is None else max(last_ts, window_end_ms)

    if not events:
        LOGGER.info("%s: no new events", state_key)
        if not dry_run and last_ts is not None:
            state.update(state_key, [], last_ts)
            state.save()
        return 0

    if dry_run:
        LOGGER.info("Dry run: would ship %s %s events", len(events), state_key)
        for event in events[:3]:
            LOGGER.info("Sample %s: %s", state_key, json.dumps(event, default=str)[:400])
        return 0

    sent = shipper.ship(events, category=f"binance-{source}")
    state.update(state_key, new_ids, last_ts)
    state.save()
    LOGGER.info("%s: shipped %s events", state_key, sent)
    return sent


def poll_source_windows(
    source: str,
    state_key: str,
    fetch_fn: Callable[[int, int], List[Dict[str, Any]]],
    shipper: CoralogixShipper,
    state: StateStore,
    *,
    lookback_minutes: int,
    now_ms: int,
    dry_run: bool,
    extra: Optional[Dict[str, Any]] = None,
    log_label: Optional[str] = None,
) -> int:
    label = log_label or state_key
    start_ms = window_start_ms(state, state_key, lookback_minutes)
    total = 0
    for chunk_start, chunk_end in iter_time_windows(
        start_ms, now_ms, SOURCE_MAX_WINDOW_MS[source]
    ):
        try:
            raw = fetch_fn(chunk_start, chunk_end)
        except RuntimeError as exc:
            LOGGER.error("%s failed: %s", label, exc)
            break
        events = filter_new_events(source, raw, state.get_seen_ids(state_key), extra=extra)
        LOGGER.info(
            "%s scan: scanned=%s kept=%s window=%s .. %s",
            label,
            len(raw),
            len(events),
            ms_to_iso(chunk_start),
            ms_to_iso(chunk_end),
        )
        total += ship_source(
            source,
            events,
            shipper,
            state,
            state_key,
            dry_run=dry_run,
            window_end_ms=chunk_end,
        )
    return total


def poll_sources(
    binance: BinanceClient,
    shipper: CoralogixShipper,
    state: StateStore,
    *,
    sources: List[str],
    lookback_minutes: int,
    transfer_types: List[str],
    symbols: List[str],
    dry_run: bool,
) -> int:
    now_ms = utc_now_ms()
    total = 0

    if "deposit" in sources:
        total += poll_source_windows(
            "deposit",
            "deposit",
            binance.fetch_deposits,
            shipper,
            state,
            lookback_minutes=lookback_minutes,
            now_ms=now_ms,
            dry_run=dry_run,
            log_label="Deposit",
        )

    if "withdraw" in sources:
        total += poll_source_windows(
            "withdraw",
            "withdraw",
            binance.fetch_withdrawals,
            shipper,
            state,
            lookback_minutes=lookback_minutes,
            now_ms=now_ms,
            dry_run=dry_run,
            log_label="Withdraw",
        )

    if "transfer" in sources:
        for transfer_type in transfer_types:
            total += poll_source_windows(
                "transfer",
                f"transfer:{transfer_type}",
                lambda start, end, transfer_type=transfer_type: binance.fetch_transfers(
                    transfer_type, start, end
                ),
                shipper,
                state,
                lookback_minutes=lookback_minutes,
                now_ms=now_ms,
                dry_run=dry_run,
                extra={"transferType": transfer_type},
                log_label=f"Transfer {transfer_type}",
            )

    if "c2c" in sources:
        try:
            total += poll_source_windows(
                "c2c",
                "c2c",
                binance.fetch_c2c_orders,
                shipper,
                state,
                lookback_minutes=lookback_minutes,
                now_ms=now_ms,
                dry_run=dry_run,
                log_label="C2C",
            )
        except Exception as exc:
            LOGGER.error("C2C failed: %s", exc)

    if "trade" in sources:
        for symbol in symbols:
            total += poll_source_windows(
                "trade",
                f"trade:{symbol}",
                lambda start, end, symbol=symbol: binance.fetch_trades(symbol, start, end),
                shipper,
                state,
                lookback_minutes=lookback_minutes,
                now_ms=now_ms,
                dry_run=dry_run,
                log_label=f"Trade {symbol}",
            )

    if "order" in sources:
        for symbol in symbols:
            total += poll_source_windows(
                "order",
                f"order:{symbol}",
                lambda start, end, symbol=symbol: binance.fetch_orders(symbol, start, end),
                shipper,
                state,
                lookback_minutes=lookback_minutes,
                now_ms=now_ms,
                dry_run=dry_run,
                log_label=f"Order {symbol}",
            )

    return total


def resolve_lookback_minutes(args: argparse.Namespace) -> int:
    if getattr(args, "lookback_minutes", None) is not None:
        return int(args.lookback_minutes)
    if getattr(args, "lookback_hours", None) is not None:
        return int(args.lookback_hours) * 60
    if os.getenv("LOOKBACK_MINUTES"):
        return int(os.getenv("LOOKBACK_MINUTES", "5"))
    if os.getenv("LOOKBACK_HOURS"):
        return int(os.getenv("LOOKBACK_HOURS", "1")) * 60
    return 5


def run_once(args: argparse.Namespace) -> int:
    sources = [s.lower() for s in env_csv("BINANCE_SOURCES", DEFAULT_SOURCES)]
    unknown = [s for s in sources if s not in KNOWN_SOURCES]
    if unknown:
        raise SystemExit(f"Unknown BINANCE_SOURCES: {unknown}. Known: {sorted(KNOWN_SOURCES)}")

    symbols = [s.upper() for s in env_csv("BINANCE_SYMBOLS")]
    if set(sources) & OPTIONAL_SOURCES and not symbols:
        raise SystemExit(
            "BINANCE_SYMBOLS is required when BINANCE_SOURCES includes trade or order "
            "(comma-separated, e.g. BTCUSDT,ETHUSDT)."
        )

    binance = BinanceClient(
        api_key=require_env("BINANCE_API_KEY"),
        api_secret=require_env("BINANCE_API_SECRET"),
        base_url=os.getenv("BINANCE_BASE_URL", "https://api.binance.com").strip(),
        recv_window=int(os.getenv("BINANCE_RECV_WINDOW", "10000")),
        timeout=float(os.getenv("HTTP_TIMEOUT_SECONDS", "60")),
    )
    binance.sync_time()

    shipper = CoralogixShipper(
        api_key=(
            os.getenv("CORALOGIX_SEND_YOUR_DATA_KEY", "").strip()
            if args.dry_run
            else require_env("CORALOGIX_SEND_YOUR_DATA_KEY")
        )
        or "dry-run",
        domain=os.getenv("CORALOGIX_DOMAIN", "coralogix.com").strip(),
        application_name=os.getenv("CORALOGIX_APPLICATION_NAME", "binance"),
        subsystem_name=os.getenv("CORALOGIX_SUBSYSTEM_NAME", "exchange-logs"),
        batch_size=int(os.getenv("CORALOGIX_BATCH_SIZE", "200")),
        timeout=float(os.getenv("HTTP_TIMEOUT_SECONDS", "60")),
    )
    state = StateStore(Path(os.getenv("STATE_FILE", "./state.json")))
    lookback_minutes = resolve_lookback_minutes(args)
    LOGGER.info(
        "Polling Binance lookback=%s minute(s) sources=%s dry_run=%s",
        lookback_minutes,
        ",".join(sources),
        args.dry_run,
    )
    return poll_sources(
        binance,
        shipper,
        state,
        sources=sources,
        lookback_minutes=lookback_minutes,
        transfer_types=env_csv("BINANCE_TRANSFER_TYPES", DEFAULT_TRANSFER_TYPES),
        symbols=symbols,
        dry_run=args.dry_run,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Poll Binance account activity and ship it to Coralogix"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single poll cycle and exit (recommended for cron)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch Binance logs but do not send to Coralogix or update state",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=None,
        help="Lookback window in minutes (default: 5)",
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=None,
        help="Lookback window in hours (alternative to --lookback-minutes)",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (default: INFO)",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.once or args.dry_run:
        sent = run_once(args)
        LOGGER.info("Done. Total events shipped: %s", sent)
        return 0

    interval = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
    LOGGER.info("Starting continuous poller (interval=%ss)", interval)
    while True:
        cycle_start = time.time()
        try:
            sent = run_once(args)
            LOGGER.info("Cycle complete. Shipped %s events", sent)
        except Exception:
            LOGGER.exception("Poll cycle failed")
        elapsed = time.time() - cycle_start
        sleep_for = max(1.0, interval - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    sys.exit(main())
