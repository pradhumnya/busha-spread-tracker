"""
busha_spread_tracker.py — shared library
-----------------------------------------
Providers, DB layer, SpreadPoller, HTML template.
Used by both poller.py (one-shot GitHub Actions script) and server.py (FastAPI dashboard).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BUSHA_PROD = "https://api.busha.io"
BUSHA_SANDBOX = "https://api.sandbox.busha.so"
DEFAULT_INTERVAL = 3600.0        # poll cadence (seconds) — run hourly via GH Actions
DEFAULT_MID_REFRESH = 3600.0     # mid-market refresh (1 hour)
DEFAULT_SHEETS_PUSH = 3600.0     # Google Sheets push (1 hour)
DEFAULT_DB_PATH = "busha_spread.db"
DEFAULT_MARKUP_BPS = 15.0
HTTP_TIMEOUT = 10.0

# ---------------------------------------------------------------------------
# Mid-market providers
#
# These three sources represent fundamentally different things:
#   Frankfurter (ECB): true interbank mid-market, updated ~daily by ECB.
#   open.er-api.com:   retail composite from multiple sources; not true mid.
#   CBN NFEM:          Nigerian official rate; lags global mid and is not
#                      accessible from non-Nigerian IPs.
# ---------------------------------------------------------------------------


class MidMarketProvider(ABC):
    """Abstract base. Each provider returns a float rate."""
    name: str = "abstract"

    @abstractmethod
    def fetch(self, session: requests.Session) -> float:
        ...


class FrankfurterProvider(MidMarketProvider):
    """ECB-tracked USD/NGN. Free, no auth. Refreshes ~daily on the upstream."""
    name = "Frankfurter (ECB reference rate, mid-market)"
    URL = "https://api.frankfurter.app/latest"

    def fetch(self, session: requests.Session) -> float:
        r = session.get(self.URL, params={"base": "USD", "symbols": "NGN"},
                        timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        rate = data.get("rates", {}).get("NGN")
        if rate is None:
            raise RuntimeError(f"NGN missing from Frankfurter response: {data}")
        return float(rate)


class OpenErApiProvider(MidMarketProvider):
    """open.er-api.com — free, no auth, hourly-ish updates."""
    name = "open.er-api.com (free, retail composite — not true mid-market)"
    URL = "https://open.er-api.com/v6/latest/USD"

    def fetch(self, session: requests.Session) -> float:
        r = session.get(self.URL, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        rate = data.get("rates", {}).get("NGN")
        if rate is None:
            raise RuntimeError(f"NGN missing from open.er-api response: {data}")
        return float(rate)


class CBNProvider(MidMarketProvider):
    """CBN JSON API — returns the central rate for US DOLLAR."""
    name = "CBN NFEM (Nigerian official rate — not global mid-market)"
    URL = "https://www.cbn.gov.ng/api/GetAllExchangeRates"

    def fetch(self, session: requests.Session) -> float:
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        r = session.get(self.URL, headers=headers, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        for entry in r.json():
            if str(entry.get("currency", "")).upper() == "US DOLLAR":
                return float(entry["centralrate"])
        raise RuntimeError("US DOLLAR not found in CBN API response")


class StaticProvider(MidMarketProvider):
    """Manual override. Useful when you want to plug in a fixed rate."""
    name = "Static (manual)"

    def __init__(self, rate: float):
        self._rate = float(rate)

    def fetch(self, session: requests.Session) -> float:
        return self._rate


def make_provider() -> MidMarketProvider:
    kind = os.environ.get("MID_PROVIDER", "frankfurter").lower()
    if kind == "frankfurter":
        return FrankfurterProvider()
    if kind == "open_er_api":
        return OpenErApiProvider()
    if kind == "cbn":
        return CBNProvider()
    if kind == "static":
        rate = os.environ.get("MID_STATIC_RATE")
        if not rate:
            raise SystemExit("MID_PROVIDER=static requires MID_STATIC_RATE")
        return StaticProvider(float(rate))
    raise SystemExit(f"Unknown MID_PROVIDER: {kind}")


# ---------------------------------------------------------------------------
# Database — Postgres only via psycopg3 ConnectionPool
# ---------------------------------------------------------------------------

class Database:
    """Thin wrapper around a psycopg3 ConnectionPool."""

    def __init__(self, url: str):
        from psycopg_pool import ConnectionPool
        self._pool = ConnectionPool(url, min_size=1, max_size=3, open=True)
        with self._pool.connection() as conn:
            conn.execute("SELECT 1")
        try:
            from urllib.parse import urlparse
            self._host = urlparse(url).hostname or "unknown"
        except Exception:
            self._host = "unknown"
        logging.info("Using PostgreSQL at %s", self._host)

    def execute_write(self, sql: str, params: tuple = ()) -> None:
        with self._pool.connection() as conn:
            conn.execute(sql, params)

    def execute_read(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._pool.connection() as conn:
            cur = conn.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def execute_script(self, statements: list[str]) -> None:
        with self._pool.connection() as conn:
            for stmt in statements:
                stmt = stmt.strip()
                if stmt:
                    conn.execute(stmt)

    def close(self) -> None:
        self._pool.close()


def make_database() -> Database:
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise SystemExit("DATABASE_URL env var is required.")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return Database(url)


# ---------------------------------------------------------------------------
# Schema (Postgres)
# ---------------------------------------------------------------------------

_SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS spread_snapshots (
        id                  BIGSERIAL PRIMARY KEY,
        fetched_at          TEXT             NOT NULL,
        fetched_ts_ms       BIGINT           NOT NULL,
        busha_rate          DOUBLE PRECISION,
        markup_bps          DOUBLE PRECISION,
        markup_amount       DOUBLE PRECISION,
        quoted_rate         DOUBLE PRECISION,
        quoted_source       TEXT             NOT NULL,
        mid_market_rate     DOUBLE PRECISION,
        mid_market_source   TEXT             NOT NULL,
        mid_market_age_sec  INTEGER,
        spread_abs          DOUBLE PRECISION,
        spread_pct          DOUBLE PRECISION,
        spread_bps          DOUBLE PRECISION,
        pair                TEXT             NOT NULL DEFAULT 'USDTNGN'
    )""",
    """ALTER TABLE spread_snapshots ADD COLUMN IF NOT EXISTS
        pair TEXT NOT NULL DEFAULT 'USDTNGN'""",
    """CREATE INDEX IF NOT EXISTS idx_spread_snapshots_ts
        ON spread_snapshots(fetched_ts_ms DESC)""",
    """CREATE INDEX IF NOT EXISTS idx_spread_snapshots_pair
        ON spread_snapshots(pair)""",
    """CREATE OR REPLACE VIEW v_latest_spread AS
    SELECT * FROM spread_snapshots ORDER BY fetched_ts_ms DESC LIMIT 1""",
    """CREATE OR REPLACE VIEW v_spread_1min AS
    SELECT
        date_trunc('minute', fetched_at::timestamptz) AS minute_bucket,
        COUNT(*)               AS samples,
        AVG(quoted_rate)       AS avg_quoted_rate,
        AVG(mid_market_rate)   AS avg_mid_market_rate,
        AVG(spread_pct)        AS avg_spread_pct,
        MIN(spread_pct)        AS min_spread_pct,
        MAX(spread_pct)        AS max_spread_pct
    FROM spread_snapshots
    WHERE quoted_rate IS NOT NULL AND mid_market_rate IS NOT NULL
    GROUP BY minute_bucket
    ORDER BY minute_bucket""",
]


def init_db(db: Database) -> None:
    db.execute_script(_SCHEMA_STATEMENTS)


# ---------------------------------------------------------------------------
# SpreadPoller — one-shot (not a Thread)
# ---------------------------------------------------------------------------

class SpreadPoller:
    def __init__(self, db, busha_base, busha_api_key, provider, markup_bps=DEFAULT_MARKUP_BPS, pair="USDTNGN"):
        self.db = db
        self.busha_base = busha_base.rstrip("/")
        self.provider = provider
        self.markup_bps = markup_bps
        self.pair = pair.upper()
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": "busha-spread-tracker/1.0"})
        if busha_api_key:
            self.session.headers["Authorization"] = f"Bearer {busha_api_key}"

    def fetch_mid(self) -> Optional[float]:
        try:
            rate = self.provider.fetch(self.session)
            logging.info("Mid-market: 1 USD = %.4f NGN (%s)", rate, self.provider.name)
            return rate
        except Exception as e:
            logging.warning("Mid-market fetch failed: %s", e)
            return None

    def fetch_busha(self) -> Optional[float]:
        base_token = self.pair[:-3]   # "USDT" or "USDC"
        counter_token = self.pair[-3:]  # "NGN"
        try:
            r = self.session.get(f"{self.busha_base}/v1/pairs",
                                  params={"base": base_token, "counter": counter_token}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            body = r.json()
            for p in (body.get("data") or []):
                if (p.get("id") or "").upper() == self.pair:
                    bp = p.get("buy_price")
                    if isinstance(bp, dict):
                        return float(bp.get("amount"))
            return None
        except Exception as e:
            logging.warning("Busha fetch failed: %s", e)
            return None

    def poll_once(self) -> dict:
        mid = self.fetch_mid()
        busha_rate = self.fetch_busha()

        markup_amount = (mid * self.markup_bps / 10_000) if mid is not None else None
        display_rate = (busha_rate + markup_amount) if (busha_rate is not None and markup_amount is not None) else None

        now = datetime.now(timezone.utc)
        fetched_at = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        fetched_ts_ms = int(now.timestamp() * 1000)

        spread_abs = (display_rate - mid) if (display_rate is not None and mid is not None) else None
        spread_pct = (spread_abs / mid * 100.0) if (spread_abs is not None and mid) else None
        spread_bps_val = (spread_abs / mid * 10_000.0) if (spread_abs is not None and mid) else None

        base = self.pair.replace("NGN", "")   # "USDT" or "USDC"
        quoted_source = f"PrimeVault Partner Network ({base}/NGN)"
        self.db.execute_write(
            """INSERT INTO spread_snapshots
               (fetched_at, fetched_ts_ms, busha_rate, markup_bps, markup_amount,
                quoted_rate, quoted_source, mid_market_rate, mid_market_source,
                mid_market_age_sec, spread_abs, spread_pct, spread_bps, pair)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (fetched_at, fetched_ts_ms, busha_rate, self.markup_bps, markup_amount,
             display_rate, quoted_source,
             mid, self.provider.name, 0,
             spread_abs, spread_pct, spread_bps_val, self.pair),
        )
        logging.info("Snapshot: quoted=%.4f mid=%.4f spread_bps=%.1f",
                     display_rate or 0, mid or 0, spread_bps_val or 0)
        return {
            "fetched_at": fetched_at, "quoted_rate": display_rate,
            "mid_market_rate": mid, "spread_pct": spread_pct,
            "spread_bps": spread_bps_val, "spread_abs": spread_abs,
        }


# ---------------------------------------------------------------------------
# Google Sheets push (one-shot)
# ---------------------------------------------------------------------------

SHEETS_HEADER = [
    "Timestamp (UTC)", "PrimeVault Quoted Rate (NGN/USDT)", "Mid-Market Rate (NGN/USD)",
    "Spread (NGN)", "Spread %", "Spread bps", "Mid Source",
]


def push_to_sheets_once(db: Database) -> bool:
    sheet_id = os.environ.get("SHEETS_ID")
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not sheet_id or not creds:
        return False
    if not os.path.exists(creds):
        logging.warning("GOOGLE_APPLICATION_CREDENTIALS not found: %s", creds)
        return False
    rows = db.execute_read(
        "SELECT * FROM spread_snapshots WHERE quoted_rate IS NOT NULL "
        "AND mid_market_rate IS NOT NULL ORDER BY fetched_ts_ms DESC LIMIT 1"
    )
    if not rows:
        return False
    snap = rows[0]
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds_obj = Credentials.from_service_account_file(creds, scopes=scopes)
        gc = gspread.authorize(creds_obj)
        sh = gc.open_by_key(sheet_id)
        ws_name = os.environ.get("SHEETS_WORKSHEET", "Spread Log")
        try:
            ws = sh.worksheet(ws_name)
        except Exception:
            ws = sh.add_worksheet(title=ws_name, rows=1000, cols=10)
            ws.append_row(SHEETS_HEADER, value_input_option="RAW")
        first_row = ws.row_values(1) if ws.row_count > 0 else []
        if first_row != SHEETS_HEADER:
            try:
                ws.update("A1:G1", [SHEETS_HEADER])
            except Exception:
                pass
        row = [
            snap.get("fetched_at"), snap.get("quoted_rate"), snap.get("mid_market_rate"),
            round(snap.get("spread_abs") or 0.0, 4), round(snap.get("spread_pct") or 0.0, 4),
            round(snap.get("spread_bps") or 0.0, 2), snap.get("mid_market_source"),
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logging.info("Pushed to Google Sheets: %s", snap.get("fetched_at"))
        return True
    except Exception as e:
        logging.warning("Sheets push failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# Dashboard HTML template
# ---------------------------------------------------------------------------

DASHBOARD_HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__BASE__/NGN &mdash; PrimeVault Rates</title>
<link rel="icon" href="data:image/x-icon;base64,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AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD+dzP//ncz//53M//+dzP//ncz//53M///eTNQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//95M1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/ncz//53M//+dzP//ncz//53M//+dzP//3kzUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD+dzP//ncz//53M//+dzP//ncz//53M///eTNQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//95M1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/3g0u/94NLv/eDS7/3g0u/94NLv/eDS7/3czaft4NET7eDRE+3g0RPt4NET7eDRE+3g0RPt4NET7eDRE+3g0RPt4NET7eDRE+3g0RP90LgsAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD+dzSv/ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//3kzKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53NK/+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M///eTMoAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/nc0r/53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//95MygAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD+dzSv/ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//3kzKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53NK/+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M///eTMoAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP54NNf+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M/8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/ng01/53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD+eDTX/ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP54NNf+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M/8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/ng01/53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//95NLr/eTS6/3k0uv95NLr/eTS6/3k0uv97NToAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/dzU6+3Y0Rft2NEX7djRF+3Y0Rft2NEX+eDOq/ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//3kzUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP13M4v+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M///eTNQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/Xczi/53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//95M1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD9dzOL/ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//3kzUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP13M4v+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//9djN9+3g2Qvt4NkL7eDZC+3g2Qvt4NkL6djc4AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+nY3OPt4NkL7eDZC+3g2Qvt4NkL7eDZC/Xc0hf94M73/eDO9/3gzvf94M73/eDO9/3gzvf53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//54NNcAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD+eDTX/ncz//53M//+dzP//ncz//53M//9eDR1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ng01wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP54NNf+dzP//ncz//53M//+dzP//ncz//14NHUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+eDTXAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/ng01/53M//+dzP//ncz//53M//+dzP//Xg0dQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//54NNcAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD+eDTX/ncz//53M//+dzP//ncz//53M//9eDR1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ng01wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP54NNf+dzP//ncz//53M//+dzP//ncz//14NHUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKAAAADAAAABgAAAAAQAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP54NOj+eDTo/ng06P54NOj+eDTo/ng06P54NOj+eDTo/ng06P95NYP/ejcX/3o3F/96Nxf/ejcX/3o3F/96Nxf/ejcX/3o3F/96Nxf/ejcX/3o3F/96Nxf/ejcX/3o3F/96Nxf/ejcX/3o3F/96Nxf/dzMP//8AAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP97Nzz/ezc8/3s3PP97Nzz/ezc8/3s3PP97Nzz/ezc8/3s3PP14NX3+dzPD/nczw/53M8P+dzPD/nczw/53M8P+dzPD/nczw/53M8P+dzPD/nczw/53M8P+dzPD/nczw/53M8P+dzPD/nczw/53M8P9eDSA/4AzCgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP12NHv+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//9dzSn/3Y7DQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP12NHv+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//9dzSn/3Y7DQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP12NHv+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//9dzSn/3Y7DQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP12NHv+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//9dzSn/3Y7DQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP12NHv+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//9dzSn/3Y7DQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP12NHv+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//9dzSn/3Y7DQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP12NHv+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//9dzSn/3Y7DQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//95NDv/eTQ7/3k0O/95NDv/eTQ7/3k0O/95NDv/eTQ7/3k0O/97MR8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD7eDRE/nc0uv54NMT+eDTE/ng0xP54NMT+eDTE/ng0xP54NMT+dzPH/ncz9v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M+j+dzPo/ncz6P53M+j+dzPo/ncz6P53M+j+dzPo/ncz6P95M3gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/gEAI/3QuFv96Nxf/ejcX/3o3F/96Nxf/ejcX/3o3F/96Nxf4eDQi/ncz3P53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADqgEAM/ncz2P53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADqgEAM/ncz2P53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADqgEAM/ncz2P53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADqgEAM/ncz2P53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADqgEAM/ncz2P53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//94NIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADqgEAM/ncz2P53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//93NI//dDoW/3Q6Fv90Ohb/dDoW/3Q6Fv90Ohb/dDoW/3Q6Fv95MRX/gEAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/gEAI/3kxFf90Ohb/dDoW/3Q6Fv90Ohb/dDoW/3Q6Fv90Ohb3eDgg/nczyf53M+n+dzPp/ncz6f53M+n+dzPp/ncz6f53M+n+dzPp/ncz6f53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M+H+dzTB/nc0wf53NMH+dzTB/nc0wf53NMH+dzTB/nc0wf52NLf7djVDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD7djVD/nY0t/53NMH+dzTB/nc0wf53NMH+dzTB/nc0wf53NMH+eDS7/3Y1Uv93NT7/dzU+/3c1Pv93NT7/dzU+/3c1Pv93NT7/dzU+/3c1Pv53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53NPL/ejRYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP0/3Y0JwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53NPL/ejRYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP0/3Y0JwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53NPL/ejRYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP0/3Y0JwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53NPL/ejRYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP0/3Y0JwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53NPL/ejRYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP0/3Y0JwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53NPL/ejRYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP0/3Y0JwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP//ncz//53NPL/ejRYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/ejRY/nc08v53M//+dzP//ncz//53M//+dzP//ncz//53M//+dzP0/3Y0JwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==" type="image/x-icon">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  :root {
    --fg: #1a1a1a; --bg: #fafafa; --muted: #6b6b6b; --line: #e6e6e6;
    --accent: #0b6e3a; --warn: #b8740a; --bad: #b3261e; --card: #fff;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.5 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;
         color: var(--fg); background: var(--bg); padding: 24px; }
  h1 { margin: 0 0 4px; font-size: 20px; font-weight: 600; }
  .sub { color: var(--muted); font-size: 13px; margin-bottom: 24px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
           gap: 12px; margin-bottom: 24px; }
  .card { background: var(--card); border: 1px solid var(--line); border-radius: 8px;
          padding: 14px 16px; }
  .card .label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em;
                 color: var(--muted); margin-bottom: 6px; }
  .card .value { font-size: 22px; font-weight: 600; font-variant-numeric: tabular-nums; }
  .card .footnote { color: var(--muted); font-size: 11px; margin-top: 4px; }
  .pos { color: var(--bad); }
  .neg { color: var(--accent); }
  .pills { margin-bottom: 16px; }
  .pill { display: inline-block; padding: 4px 12px; margin-right: 6px; border-radius: 20px;
           border: 1px solid var(--line); background: var(--card); cursor: pointer;
           font-size: 13px; font-weight: 500; color: var(--muted); }
  .pill.active { background: var(--accent); color: #fff; border-color: var(--accent); }
  .chart-wrap { background: var(--card); border: 1px solid var(--line); border-radius: 8px;
                padding: 16px; margin-bottom: 24px; }
  .chart-wrap h2 { margin: 0 0 12px; font-size: 14px; font-weight: 600; color: var(--muted);
                    text-transform: uppercase; letter-spacing: 0.04em; }
  table { width: 100%; border-collapse: collapse; background: var(--card);
          border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }
  th, td { text-align: right; padding: 8px 12px; border-bottom: 1px solid var(--line);
           font-variant-numeric: tabular-nums; }
  th { background: #f4f4f4; font-weight: 600; font-size: 12px; text-transform: uppercase;
       letter-spacing: 0.04em; color: var(--muted); }
  th:first-child, td:first-child { text-align: left; }
  tr:last-child td { border-bottom: none; }
  tr.live td { background: #fff8e1; }
  .meta { color: var(--muted); font-size: 12px; margin-top: 16px; }
  .pulse { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
           background: var(--accent); margin-right: 6px;
           animation: pulse 2s ease-in-out infinite; }
  .pulse.stale { background: var(--bad); animation: none; }
  @keyframes pulse { 0%,100% {opacity: 1;} 50% {opacity: 0.4;} }
  .caveat { background: #fff8e1; border: 1px solid #f0e1a8; color: #6b4d00;
            border-radius: 8px; padding: 10px 14px; font-size: 12px; margin-bottom: 16px; }
  .no-data { text-align: center; color: var(--muted); padding: 32px; font-size: 13px; }
  .table-wrap { overflow-y: auto; max-height: 480px; border: 1px solid var(--line);
                border-radius: 8px; margin-bottom: 8px; }
  .table-wrap table { border: none; border-radius: 0; margin: 0; }
  .table-footer { font-size: 12px; color: var(--muted); margin-bottom: 24px; padding: 4px 2px; }
</style>
</head>
<body>
  <h1>__BASE__/NGN &mdash; PrimeVault Rates</h1>
  <div class="sub"><span id="pulse" class="pulse"></span><span id="status">Connecting...</span></div>

  <div class="caveat">
    Rates shown are PrimeVault's quoted rates based on live pricing from our partner network, updated hourly.
    Rates applied to actual transactions are determined at the time of execution and may differ.
  </div>

  <div class="cards">
    <div class="card">
      <div class="label">PrimeVault Quoted Rate</div>
      <div class="value"><span id="quoted">&mdash;</span> <small style="font-size:13px;color:var(--muted)">NGN</small></div>
      <div class="footnote">NGN per 1 __BASE__</div>
    </div>
    <div class="card">
      <div class="label">Mid-Market Rate</div>
      <div class="value"><span id="mid">&mdash;</span> <small style="font-size:13px;color:var(--muted)">NGN</small></div>
      <div class="footnote" id="mid_source"></div>
    </div>
    <div class="card">
      <div class="label">Spread %</div>
      <div class="value"><span id="spread_pct">&mdash;</span><small style="font-size:13px;color:var(--muted)"> %</small></div>
      <div class="footnote"><span id="spread_abs">&mdash;</span> NGN absolute</div>
    </div>
    <div class="card">
      <div class="label">Spread bps</div>
      <div class="value"><span id="spread_bps">&mdash;</span></div>
      <div class="footnote">basis points vs mid</div>
    </div>
  </div>

  <div class="pills">
    <span class="pill active" onclick="setWindow('24h')">24h</span>
    <span class="pill" onclick="setWindow('7d')">7d</span>
    <span class="pill" onclick="setWindow('30d')">30d</span>
    <span class="pill" onclick="setWindow('all')">All time</span>
    <span id="window_range" style="font-size:12px;color:var(--muted);margin-left:10px;line-height:30px;"></span>
  </div>

  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Timestamp (UTC)</th>
          <th>PrimeVault Quoted Rate (NGN)</th>
          <th>Mid-Market Rate (NGN)</th>
          <th>Spread (NGN)</th>
          <th>Spread %</th>
          <th>Spread bps</th>
        </tr>
      </thead>
      <tbody id="tbody">
        <tr><td colspan="6" class="no-data">Loading&#x2026;</td></tr>
      </tbody>
    </table>
  </div>
  <div class="table-footer" id="table-footer"></div>

  <div class="chart-wrap" style="margin-top:24px">
    <h2>NGN/__BASE__ Rates</h2>
    <canvas id="ratesChart" height="120"></canvas>
  </div>

  <div class="chart-wrap">
    <h2>Spread (bps)</h2>
    <canvas id="spreadChart" height="80"></canvas>
  </div>

<script>
const TABLE_MAX = 50;
const fmt = (v, d=2) => v == null ? "—" : Number(v).toLocaleString(undefined, {minimumFractionDigits: d, maximumFractionDigits: d});
const fmtTime = ts => {
  if (!ts) return "—";
  return String(ts).replace("T", " ").slice(0, 19);
};
// Window-aware label: 24h → "21:57", 7d → "May 10 21:57", 30d/all → "May 10"
const fmtLabel = (ts, w) => {
  if (!ts) return "";
  const d = new Date(String(ts));
  const mo = d.toLocaleString("en", {month: "short", timeZone: "UTC"});
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hhmm = String(d.getUTCHours()).padStart(2, "0") + ":" + String(d.getUTCMinutes()).padStart(2, "0");
  if (w === "24h") return hhmm;
  if (w === "7d")  return mo + " " + day + " " + hhmm;
  return mo + " " + day;
};
const sign = v => v == null ? "" : (v > 0 ? "pos" : "neg");

let _window = "24h";
let ratesChart = null;
let spreadChart = null;

function windowToFrom(w) {
  const now = new Date();
  const ms = {"24h": 86400e3, "7d": 604800e3, "30d": 2592000e3};
  return ms[w] ? new Date(now - ms[w]).toISOString() : null;
}

function setWindow(w) {
  _window = w;
  document.querySelectorAll(".pill").forEach(p => {
    p.classList.toggle("active", p.textContent.trim() === w || (w === "all" && p.textContent.trim() === "All time"));
  });
  tick();
}

function initCharts() {
  const commonX = { ticks: { maxTicksLimit: 12, maxRotation: 0 } };
  const rCtx = document.getElementById("ratesChart").getContext("2d");
  ratesChart = new Chart(rCtx, {
    type: "line",
    data: { labels: [], datasets: [
      { label: "PrimeVault Quoted Rate", data: [], borderColor: "#0b6e3a",
        backgroundColor: "rgba(11,110,58,0.08)", borderWidth: 2, pointRadius: 2, tension: 0.3 },
      { label: "Mid-Market Rate", data: [], borderColor: "#b8740a",
        backgroundColor: "rgba(184,116,10,0.08)", borderWidth: 2, pointRadius: 2, tension: 0.3,
        borderDash: [4, 3] },
    ]},
    options: {
      responsive: true, interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "top" } },
      scales: { x: commonX, y: { ticks: { callback: v => Number(v).toLocaleString() } } },
    },
  });

  const sCtx = document.getElementById("spreadChart").getContext("2d");
  spreadChart = new Chart(sCtx, {
    type: "line",
    data: { labels: [], datasets: [
      { label: "Spread bps", data: [], borderColor: "#b3261e",
        backgroundColor: "rgba(179,38,30,0.08)", borderWidth: 2, pointRadius: 2, tension: 0.3, fill: true },
    ]},
    options: {
      responsive: true, plugins: { legend: { display: false } },
      scales: { x: commonX, y: { ticks: { callback: v => v + " bps" } } },
    },
  });
}

function updateCharts(rows, w) {
  // rows newest-first → reverse for chronological
  const sorted = [...rows].reverse();
  const labels  = sorted.map(r => fmtLabel(r.fetched_at, w));
  const quoted  = sorted.map(r => r.quoted_rate);
  const mid     = sorted.map(r => r.mid_market_rate);
  const bps     = sorted.map(r => r.spread_bps);

  // Tick density and point size by window
  const tickLimits = {"24h": 12, "7d": 14, "30d": 15, "all": 12};
  const pts        = {"24h": 3,  "7d": 2,  "30d": 1,  "all": 1};
  const ticks = tickLimits[w] || 12;
  const pt    = pts[w] || 2;

  [ratesChart, spreadChart].forEach(c => {
    c.options.scales.x.ticks.maxTicksLimit = ticks;
  });
  ratesChart.data.datasets.forEach(ds => ds.pointRadius = pt);
  spreadChart.data.datasets.forEach(ds => ds.pointRadius = pt);

  ratesChart.data.labels = labels;
  ratesChart.data.datasets[0].data = quoted;
  ratesChart.data.datasets[1].data = mid;
  ratesChart.update();

  spreadChart.data.labels = labels;
  spreadChart.data.datasets[0].data = bps;
  spreadChart.update();
}

async function tick() {
  try {
    const fromTs = windowToFrom(_window);
    const _ak   = "__API_KEY__";
    const _pair = "__PAIR__";
    let histUrl = "/api/history?limit=1000&key=" + _ak + "&pair=" + _pair;
    if (fromTs) histUrl += "&from=" + encodeURIComponent(fromTs);

    const safeJson = r => r.ok ? r.json() : Promise.reject(new Error("HTTP " + r.status));
    const [histRes, healthRes] = await Promise.all([
      fetch(histUrl).then(safeJson),
      fetch("/health").then(safeJson),
    ]);

    // Top cards always use the same row as the first table entry (max rate for latest hour)
    const latestRes = (histRes && histRes.data && histRes.data.length > 0) ? histRes.data[0] : null;
    if (latestRes) {
      document.getElementById("quoted").textContent = fmt(latestRes.quoted_rate, 4);
      document.getElementById("mid").textContent = fmt(latestRes.mid_market_rate, 4);
      const sp = document.getElementById("spread_pct");
      sp.textContent = fmt(latestRes.spread_pct, 2);
      sp.className = sign(latestRes.spread_pct);
      const sa = document.getElementById("spread_abs");
      sa.textContent = fmt(latestRes.spread_abs, 2);
      sa.className = sign(latestRes.spread_abs);
      const sb = document.getElementById("spread_bps");
      sb.textContent = fmt(latestRes.spread_bps, 0);
      sb.className = sign(latestRes.spread_bps);
      document.getElementById("mid_source").textContent = "";
    }

    const rows = (histRes && histRes.data) ? histRes.data : [];
    const rangeEl = document.getElementById("window_range");
    if (rows.length > 0) {
      const oldest = rows[rows.length - 1].fetched_at;
      const newest = rows[0].fetched_at;
      rangeEl.textContent = fmtTime(oldest) + " → " + fmtTime(newest);
    } else {
      rangeEl.textContent = "No data in this window";
    }

    const tbody = document.getElementById("tbody");
    const footerEl = document.getElementById("table-footer");
    if (rows.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" class="no-data">No data yet for this window. Data updates hourly.</td></tr>';
      footerEl.textContent = "";
      updateCharts([], _window);
    } else {
      const display = rows.slice(0, TABLE_MAX);
      tbody.innerHTML = display.map((r, i) => `
        <tr class="${i === 0 ? 'live' : ''}">
          <td>${fmtTime(r.fetched_at)}</td>
          <td>${fmt(r.quoted_rate, 4)}</td>
          <td>${fmt(r.mid_market_rate, 4)}</td>
          <td class="${sign(r.spread_abs)}">${fmt(r.spread_abs, 2)}</td>
          <td class="${sign(r.spread_pct)}">${fmt(r.spread_pct, 2)}%</td>
          <td class="${sign(r.spread_bps)}">${fmt(r.spread_bps, 0)}</td>
        </tr>
      `).join("");
      footerEl.textContent = rows.length > TABLE_MAX
        ? "Showing latest " + TABLE_MAX + " of " + rows.length + " hourly entries — see charts below for the full window."
        : "Showing all " + rows.length + " hourly entries.";
      updateCharts(rows, _window);
    }

    const stale = healthRes && healthRes.stale;
    document.getElementById("pulse").className = "pulse" + (stale ? " stale" : "");
    const lastAt = (healthRes && healthRes.last_snapshot_at) ? healthRes.last_snapshot_at : null;
    document.getElementById("status").textContent =
      (lastAt ? "Last updated " + fmtTime(lastAt) : "No data yet") +
      (stale ? " · STALE" : "");
  } catch (e) {
    document.getElementById("status").textContent = "Waking up… retrying in 10s";
    document.getElementById("pulse").className = "pulse stale";
    setTimeout(tick, 10000);
  }
}

initCharts();
tick();
setInterval(tick, 60000);
</script>
</body>
</html>
"""
