"""
Busha USDT/NGN Spread Tracker
-----------------------------
Polls Busha's /v1/pairs every second for the USDT/NGN buy_price (the rate a
customer pays in NGN to receive 1 USDT), refreshes a mid-market USD/NGN
reference rate on a slower cadence, computes the spread, persists each
snapshot to SQLite, and serves:

    GET /                      -> redirects to /dashboard
    GET /dashboard             -> public HTML table that auto-refreshes every second
    GET /api/latest            -> current snapshot as JSON
    GET /api/history?limit=N   -> recent N snapshots as JSON
    GET /health                -> liveness + freshness

The dashboard is a single self-contained HTML page using only vanilla JS, so
it renders without any external dependencies. To make it publicly viewable
you can either:
    - run `cloudflared tunnel --url http://localhost:8000` and share the URL
    - run `ngrok http 8000`
    - or `pip install datasette && datasette busha_spread.db` for a separate
      SQL-explorable public view of the underlying data

USDT vs USD: Busha quotes the USDT/NGN buy price. Public mid-market APIs
return USD/NGN. USDT pegs to USD typically within ±0.1%, so USD/NGN serves
as a clean mid-market proxy. The dashboard surfaces this caveat.

Run:
    pip install requests fastapi 'uvicorn[standard]'
    python busha_spread_tracker.py
    # then open http://localhost:8000/dashboard

Configurable via env vars:
    BUSHA_API_KEY                (optional bearer token)
    BUSHA_ENV                    "prod" (default) or "sandbox"
    MID_PROVIDER                 "frankfurter" (default), "open_er_api", "static"
    MID_STATIC_RATE              decimal, used when MID_PROVIDER=static
    POLL_INTERVAL_SEC            default 1.0
    MID_REFRESH_SEC              default 3600 (1 hour)
    DB_PATH                      default busha_spread.db
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sqlite3
import sys
import threading
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BUSHA_PROD = "https://api.busha.io"
BUSHA_SANDBOX = "https://api.sandbox.busha.so"
DEFAULT_INTERVAL = 5.0          # Busha poll cadence (seconds)
DEFAULT_MID_REFRESH = 3600.0    # mid-market refresh (1 hour)
DEFAULT_SHEETS_PUSH = 3600.0    # Google Sheets push (1 hour)
DEFAULT_DB_PATH = "busha_spread.db"
HTTP_TIMEOUT = 10.0

# ---------------------------------------------------------------------------
# Mid-market providers
# ---------------------------------------------------------------------------

class MidMarketProvider(ABC):
    """Abstract base. Each provider returns (rate, source_label)."""
    name: str = "abstract"

    @abstractmethod
    def fetch(self, session: requests.Session) -> float:
        ...


class FrankfurterProvider(MidMarketProvider):
    """ECB-tracked USD/NGN. Free, no auth. Refreshes ~daily on the upstream."""
    name = "Frankfurter (ECB)"
    URL = "https://api.frankfurter.dev/v1/latest"

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
    name = "open.er-api.com"
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
    name = "CBN (Central Bank of Nigeria)"
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
    """Manual override. Useful when you want to plug in CBN/XE/internal data."""
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
# Storage
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS spread_snapshots (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    fetched_at            TEXT    NOT NULL,
    fetched_ts_ms         INTEGER NOT NULL,
    quoted_rate           REAL,                 -- NGN per USDT, from Busha
    quoted_source         TEXT    NOT NULL,
    mid_market_rate       REAL,                 -- NGN per USD, from chosen provider
    mid_market_source     TEXT    NOT NULL,
    mid_market_age_sec    INTEGER,              -- how stale the mid was at this tick
    spread_abs            REAL,                 -- quoted - mid (NGN)
    spread_pct            REAL,                 -- as a percentage of mid
    spread_bps            REAL                  -- basis points
);

CREATE INDEX IF NOT EXISTS idx_spread_snapshots_ts
    ON spread_snapshots(fetched_ts_ms DESC);

CREATE VIEW IF NOT EXISTS v_latest_spread AS
SELECT * FROM spread_snapshots ORDER BY fetched_ts_ms DESC LIMIT 1;

CREATE VIEW IF NOT EXISTS v_spread_1min AS
SELECT
    strftime('%Y-%m-%dT%H:%M:00Z', fetched_at) AS minute_bucket,
    COUNT(*)             AS samples,
    AVG(quoted_rate)     AS avg_quoted_rate,
    AVG(mid_market_rate) AS avg_mid_market_rate,
    AVG(spread_pct)      AS avg_spread_pct,
    MIN(spread_pct)      AS min_spread_pct,
    MAX(spread_pct)      AS max_spread_pct
FROM spread_snapshots
WHERE quoted_rate IS NOT NULL AND mid_market_rate IS NOT NULL
GROUP BY minute_bucket
ORDER BY minute_bucket;
"""


def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# Background poller
# ---------------------------------------------------------------------------

class SpreadPoller(threading.Thread):
    daemon = True

    def __init__(
        self,
        db_conn: sqlite3.Connection,
        busha_base: str,
        busha_api_key: Optional[str],
        provider: MidMarketProvider,
        interval: float,
        mid_refresh: float,
    ):
        super().__init__(name="spread-poller")
        self.conn = db_conn
        self.lock = threading.Lock()
        self.busha_base = busha_base.rstrip("/")
        self.provider = provider
        self.interval = interval
        self.mid_refresh = mid_refresh
        self._stop_event = threading.Event()

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "busha-spread-tracker/1.0",
        })
        if busha_api_key:
            self.session.headers["Authorization"] = f"Bearer {busha_api_key}"

        self._mid_rate: Optional[float] = None
        self._mid_fetched_ts: Optional[float] = None

        self.polls = 0
        self.failures = 0
        self.last_error: Optional[str] = None

    def stop(self):
        self._stop_event.set()

    def _refresh_mid(self) -> None:
        try:
            rate = self.provider.fetch(self.session)
            self._mid_rate = rate
            self._mid_fetched_ts = time.time()
            logging.info("Mid-market refreshed: 1 USD = %.4f NGN (%s)",
                         rate, self.provider.name)
        except Exception as e:
            logging.warning("Mid-market refresh failed: %s", e)
            self.last_error = f"mid refresh: {e}"

    def _fetch_quoted(self) -> Optional[float]:
        r = self.session.get(
            f"{self.busha_base}/v1/pairs",
            params={"base": "USDT", "counter": "NGN"},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        body = r.json()
        for p in (body.get("data") or []):
            if (p.get("id") or "").upper() == "USDTNGN":
                bp = p.get("buy_price")
                if isinstance(bp, dict):
                    return float(bp.get("amount"))
        return None

    def _write_snapshot(self, quoted: Optional[float]) -> None:
        now = datetime.now(timezone.utc)
        fetched_at = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        fetched_ts_ms = int(now.timestamp() * 1000)
        mid = self._mid_rate
        mid_age = (
            int(time.time() - self._mid_fetched_ts)
            if self._mid_fetched_ts else None
        )

        spread_abs = (quoted - mid) if (quoted is not None and mid) else None
        spread_pct = (spread_abs / mid * 100.0) if (spread_abs is not None and mid) else None
        spread_bps = (spread_abs / mid * 10_000.0) if (spread_abs is not None and mid) else None

        with self.lock:
            self.conn.execute(
                """INSERT INTO spread_snapshots
                   (fetched_at, fetched_ts_ms, quoted_rate, quoted_source,
                    mid_market_rate, mid_market_source, mid_market_age_sec,
                    spread_abs, spread_pct, spread_bps)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (fetched_at, fetched_ts_ms, quoted, "Busha (USDT/NGN buy)",
                 mid, self.provider.name, mid_age,
                 spread_abs, spread_pct, spread_bps),
            )

    def run(self) -> None:
        logging.info("Poller starting. Busha cadence: %.2fs. Mid refresh: %.0fs (%s).",
                     self.interval, self.mid_refresh, self.provider.name)
        # First mid fetch upfront so the first row has a value.
        self._refresh_mid()

        next_busha_tick = time.monotonic()
        next_mid_tick = time.monotonic() + self.mid_refresh

        while not self._stop_event.is_set():
            now = time.monotonic()

            if now >= next_mid_tick:
                self._refresh_mid()
                next_mid_tick = now + self.mid_refresh

            try:
                quoted = self._fetch_quoted()
                self._write_snapshot(quoted)
                self.polls += 1
                if self.polls % 60 == 0:
                    logging.info("Polls=%d failures=%d", self.polls, self.failures)
            except Exception as e:
                self.failures += 1
                self.last_error = f"busha: {e}"
                # Still write a row so the timeline shows the gap, but with quoted=None.
                try:
                    self._write_snapshot(None)
                except Exception as inner:
                    logging.warning("Failed to record gap row: %s", inner)
                logging.warning("Busha poll failed (#%d): %s", self.failures, e)

            next_busha_tick += self.interval
            sleep_for = next_busha_tick - time.monotonic()
            if sleep_for > 0:
                self._stop_event.wait(min(sleep_for, 0.5))
                while (not self._stop_event.is_set()) and (time.monotonic() < next_busha_tick):
                    self._stop_event.wait(min(0.2, next_busha_tick - time.monotonic()))
            else:
                # behind schedule -> reset grid
                next_busha_tick = time.monotonic()


# ---------------------------------------------------------------------------
# Google Sheets publisher (optional)
# ---------------------------------------------------------------------------

class SheetsPublisher(threading.Thread):
    """
    Appends one row per push interval to a Google Sheet, giving you a public,
    shareable, append-only log of the spread.

    Setup (one time):
      1. Create a service account in Google Cloud Console (any project works).
      2. Enable the Google Sheets API on that project.
      3. Download the service account's JSON key file.
      4. Open your target Google Sheet, click Share, and add the service
         account's email (looks like xxx@yyy.iam.gserviceaccount.com) with
         Editor access.
      5. Copy the Sheet ID from the URL:
         docs.google.com/spreadsheets/d/<SHEET_ID>/edit

    Then set:
      GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
      SHEETS_ID=<SHEET_ID>
      SHEETS_WORKSHEET=Spread Log   (optional, default below)

    Anyone with view-access on the sheet can see live data. Make the sheet
    "Anyone with the link can view" for a public dashboard.

    Cadence: hourly by default (60 writes/min API quota means hourly is
    nowhere near any limit; daily would also be fine).
    """
    daemon = True

    HEADER = [
        "Timestamp (UTC)", "Quoted Rate (NGN/USDT)", "Mid-Market Rate (NGN/USD)",
        "Spread (NGN)", "Spread %", "Spread bps", "Mid Source",
    ]

    def __init__(
        self,
        db_path: str,
        sheet_id: str,
        worksheet_name: str,
        creds_path: str,
        push_interval: float,
    ):
        super().__init__(name="sheets-publisher")
        self.db_path = db_path
        self.sheet_id = sheet_id
        self.worksheet_name = worksheet_name
        self.creds_path = creds_path
        self.push_interval = push_interval
        self._stop_event = threading.Event()
        self._ws = None
        self.pushes = 0
        self.failures = 0
        self.last_error: Optional[str] = None

    def stop(self):
        self._stop_event.set()

    def _connect(self):
        """Lazy import — gspread is only needed if Sheets is configured."""
        import gspread
        from google.oauth2.service_account import Credentials  # type: ignore

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(self.creds_path, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(self.sheet_id)
        try:
            ws = sh.worksheet(self.worksheet_name)
        except Exception:
            ws = sh.add_worksheet(title=self.worksheet_name, rows=1000, cols=10)
            ws.append_row(self.HEADER, value_input_option="RAW")
        # If sheet is empty (or first row isn't ours), seed the header.
        first_row = ws.row_values(1) if ws.row_count > 0 else []
        if first_row != self.HEADER:
            try:
                ws.update("A1:G1", [self.HEADER])
            except Exception:
                pass
        self._ws = ws

    def _latest_snapshot(self) -> Optional[dict]:
        conn = sqlite3.connect(self.db_path, timeout=2.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA query_only = 1")
            row = conn.execute(
                "SELECT * FROM spread_snapshots WHERE quoted_rate IS NOT NULL "
                "AND mid_market_rate IS NOT NULL ORDER BY fetched_ts_ms DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def _push_once(self) -> bool:
        if self._ws is None:
            self._connect()
        snap = self._latest_snapshot()
        if not snap:
            logging.info("Sheets: no snapshot to push yet, skipping.")
            return False
        row = [
            snap.get("fetched_at"),
            snap.get("quoted_rate"),
            snap.get("mid_market_rate"),
            round(snap.get("spread_abs") or 0.0, 4),
            round(snap.get("spread_pct") or 0.0, 4),
            round(snap.get("spread_bps") or 0.0, 2),
            snap.get("mid_market_source"),
        ]
        self._ws.append_row(row, value_input_option="USER_ENTERED")
        self.pushes += 1
        return True

    def run(self) -> None:
        logging.info("Sheets publisher starting. Push every %.0fs to sheet %s.",
                     self.push_interval, self.sheet_id)
        # Push once on start so the user can verify the wiring works,
        # then settle into the hourly cadence.
        try:
            self._push_once()
            logging.info("Sheets: initial push ok (#%d)", self.pushes)
        except Exception as e:
            self.failures += 1
            self.last_error = str(e)
            logging.warning("Sheets: initial push failed: %s", e)

        while not self._stop_event.is_set():
            self._stop_event.wait(self.push_interval)
            if self._stop_event.is_set():
                break
            try:
                if self._push_once():
                    if self.pushes % 6 == 0:  # log every 6 pushes (~6h at default)
                        logging.info("Sheets: %d pushes, %d failures",
                                     self.pushes, self.failures)
            except Exception as e:
                self.failures += 1
                self.last_error = str(e)
                logging.warning("Sheets push failed (#%d): %s", self.failures, e)


def maybe_start_sheets(db_path: str, push_interval: float) -> Optional[SheetsPublisher]:
    """Start the Sheets publisher only if env vars are configured."""
    sheet_id = os.environ.get("SHEETS_ID")
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not sheet_id or not creds:
        logging.info(
            "Sheets publisher disabled (set SHEETS_ID and "
            "GOOGLE_APPLICATION_CREDENTIALS to enable)."
        )
        return None
    if not os.path.exists(creds):
        logging.warning("GOOGLE_APPLICATION_CREDENTIALS file not found: %s", creds)
        return None
    worksheet_name = os.environ.get("SHEETS_WORKSHEET", "Spread Log")
    pub = SheetsPublisher(db_path, sheet_id, worksheet_name, creds, push_interval)
    pub.start()
    return pub


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Busha USDT/NGN Spread Tracker", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

_poller: Optional[SpreadPoller] = None
_sheets: Optional["SheetsPublisher"] = None
_conn: Optional[sqlite3.Connection] = None


def _row_to_dict(r: sqlite3.Row) -> dict:
    return dict(r)


@contextmanager
def reader():
    """Short-lived read connection. WAL means we don't block the writer."""
    assert _conn is not None
    rconn = sqlite3.connect(_conn.execute("PRAGMA database_list").fetchone()[2], timeout=2.0)
    rconn.row_factory = sqlite3.Row
    try:
        rconn.execute("PRAGMA query_only = 1")
        yield rconn
    finally:
        rconn.close()


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/dashboard")


@app.get("/health")
def health():
    if _poller is None:
        raise HTTPException(503, "poller not initialised")
    with reader() as rc:
        row = rc.execute("SELECT MAX(fetched_ts_ms) AS t FROM spread_snapshots").fetchone()
    last_ts = row["t"] if row else None
    lag = (time.time() * 1000 - last_ts) / 1000.0 if last_ts else None
    sheets_state = None
    if _sheets is not None:
        sheets_state = {
            "enabled": True,
            "pushes": _sheets.pushes,
            "failures": _sheets.failures,
            "last_error": _sheets.last_error,
        }
    return {
        "status": "ok",
        "polls": _poller.polls,
        "failures": _poller.failures,
        "last_error": _poller.last_error,
        "last_snapshot_ms": last_ts,
        "lag_seconds": round(lag, 2) if lag is not None else None,
        "stale": (lag is not None and lag > 30),
        "sheets": sheets_state,
    }


@app.get("/api/latest")
def api_latest():
    with reader() as rc:
        row = rc.execute(
            "SELECT * FROM spread_snapshots ORDER BY fetched_ts_ms DESC LIMIT 1"
        ).fetchone()
    if row is None:
        raise HTTPException(404, "no snapshots yet")
    return _row_to_dict(row)


@app.get("/api/history")
def api_history(limit: int = Query(60, gt=0, le=5000)):
    with reader() as rc:
        rows = rc.execute(
            "SELECT * FROM spread_snapshots ORDER BY fetched_ts_ms DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return {"count": len(rows), "data": [_row_to_dict(r) for r in rows]}


# ---- Dashboard HTML ------------------------------------------------------

DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>USDT/NGN — Busha vs Mid-Market</title>
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
  .pos { color: var(--bad); }    /* positive spread = customer pays more = "bad" for them */
  .neg { color: var(--accent); }
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
           animation: pulse 1s ease-in-out infinite; }
  .pulse.stale { background: var(--bad); animation: none; }
  @keyframes pulse { 0%,100% {opacity: 1;} 50% {opacity: 0.4;} }
  .caveat { background: #fff8e1; border: 1px solid #f0e1a8; color: #6b4d00;
            border-radius: 8px; padding: 10px 14px; font-size: 12px; margin-bottom: 16px; }
</style>
</head>
<body>
  <h1>USDT/NGN — Busha vs Mid-Market</h1>
  <div class="sub"><span id="pulse" class="pulse"></span><span id="status">Connecting...</span></div>

  <div class="caveat">
    Mid-market is a free public USD/NGN feed (default: Frankfurter / ECB).
    USDT trades within ±0.1% of USD, so this is used as the USDT/NGN mid-market proxy.
    Note: official rates can differ meaningfully from parallel-market rates in NGN.
  </div>

  <div class="cards">
    <div class="card">
      <div class="label">Quoted Rate (Busha)</div>
      <div class="value"><span id="quoted">—</span> <small style="font-size:13px;color:var(--muted)">NGN</small></div>
      <div class="footnote">NGN per 1 USDT</div>
    </div>
    <div class="card">
      <div class="label">Mid-Market Rate</div>
      <div class="value"><span id="mid">—</span> <small style="font-size:13px;color:var(--muted)">NGN</small></div>
      <div class="footnote" id="mid_source">—</div>
    </div>
    <div class="card">
      <div class="label">Spread</div>
      <div class="value"><span id="spread_pct">—</span><small style="font-size:13px;color:var(--muted)"> %</small></div>
      <div class="footnote"><span id="spread_abs">—</span> NGN absolute</div>
    </div>
    <div class="card">
      <div class="label">Spread (bps)</div>
      <div class="value"><span id="spread_bps">—</span></div>
      <div class="footnote">basis points vs mid</div>
    </div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Timestamp (UTC)</th>
        <th>Quoted Rate</th>
        <th>Mid-Market Rate</th>
        <th>Spread (NGN)</th>
        <th>Spread %</th>
        <th>Spread bps</th>
      </tr>
    </thead>
    <tbody id="tbody">
      <tr><td colspan="6" style="text-align:center;color:var(--muted);padding:24px">Loading...</td></tr>
    </tbody>
  </table>

  <div class="meta" id="meta">—</div>

<script>
const fmt = (v, d=2) => v == null ? "—" : Number(v).toLocaleString(undefined, {minimumFractionDigits: d, maximumFractionDigits: d});
const fmtTime = ts => {
  if (!ts) return "—";
  const d = new Date(ts);
  return d.toISOString().replace("T", " ").slice(0, 19);
};
const sign = v => v == null ? "" : (v > 0 ? "pos" : "neg");

async function tick() {
  try {
    const [latestRes, histRes, healthRes] = await Promise.all([
      fetch("/api/latest").then(r => r.ok ? r.json() : null),
      fetch("/api/history?limit=30").then(r => r.json()),
      fetch("/health").then(r => r.json()),
    ]);

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
      document.getElementById("mid_source").textContent =
        (latestRes.mid_market_source || "—") +
        (latestRes.mid_market_age_sec != null ? ` · ${latestRes.mid_market_age_sec}s old` : "");
    }

    const rows = histRes.data || [];
    const tbody = document.getElementById("tbody");
    if (rows.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:24px">No data yet</td></tr>';
    } else {
      tbody.innerHTML = rows.map((r, i) => `
        <tr class="${i === 0 ? 'live' : ''}">
          <td>${fmtTime(r.fetched_at)}</td>
          <td>${fmt(r.quoted_rate, 4)}</td>
          <td>${fmt(r.mid_market_rate, 4)}</td>
          <td class="${sign(r.spread_abs)}">${fmt(r.spread_abs, 2)}</td>
          <td class="${sign(r.spread_pct)}">${fmt(r.spread_pct, 2)}%</td>
          <td class="${sign(r.spread_bps)}">${fmt(r.spread_bps, 0)}</td>
        </tr>
      `).join("");
    }

    const stale = healthRes.stale;
    document.getElementById("pulse").className = "pulse" + (stale ? " stale" : "");
    document.getElementById("status").textContent =
      `Live · ${healthRes.polls} polls · ${healthRes.failures} failures` +
      (healthRes.lag_seconds != null ? ` · last ${healthRes.lag_seconds}s ago` : "") +
      (stale ? " · STALE" : "");
    document.getElementById("meta").textContent =
      `Updated ${new Date().toISOString().slice(11, 19)} UTC · refreshes every 1s · ` +
      `data from /api/history`;
  } catch (e) {
    document.getElementById("status").textContent = "Connection error: " + e;
    document.getElementById("pulse").className = "pulse stale";
  }
}

tick();
setInterval(tick, 1000);
</script>
</body>
</html>
"""


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(DASHBOARD_HTML)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def start(args: argparse.Namespace) -> None:
    global _poller, _sheets, _conn

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    _conn = init_db(args.db)
    busha_base = BUSHA_PROD if args.env == "prod" else BUSHA_SANDBOX
    provider = make_provider()

    _poller = SpreadPoller(
        db_conn=_conn,
        busha_base=busha_base,
        busha_api_key=args.api_key,
        provider=provider,
        interval=args.interval,
        mid_refresh=args.mid_refresh,
    )
    _poller.start()

    _sheets = maybe_start_sheets(args.db, args.sheets_push)

    def shutdown(*_):
        logging.info("Shutting down...")
        if _sheets:
            _sheets.stop()
            _sheets.join(timeout=3)
        if _poller:
            _poller.stop()
            _poller.join(timeout=3)
        if _conn:
            _conn.close()

    signal.signal(signal.SIGINT, lambda *_: (shutdown(), sys.exit(0)))
    signal.signal(signal.SIGTERM, lambda *_: (shutdown(), sys.exit(0)))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Busha USDT/NGN spread tracker")
    p.add_argument("--env", choices=("prod", "sandbox"),
                   default=os.environ.get("BUSHA_ENV", "prod"))
    p.add_argument("--interval", type=float,
                   default=float(os.environ.get("POLL_INTERVAL_SEC", DEFAULT_INTERVAL)),
                   help="seconds between Busha polls (default: 5.0)")
    p.add_argument("--mid-refresh", type=float,
                   default=float(os.environ.get("MID_REFRESH_SEC", DEFAULT_MID_REFRESH)),
                   help="seconds between mid-market refreshes (default: 3600)")
    p.add_argument("--sheets-push", type=float,
                   default=float(os.environ.get("SHEETS_PUSH_SEC", DEFAULT_SHEETS_PUSH)),
                   help="seconds between Google Sheets pushes (default: 3600)")
    p.add_argument("--db", default=os.environ.get("DB_PATH", DEFAULT_DB_PATH))
    p.add_argument("--api-key", default=os.environ.get("BUSHA_API_KEY"))
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8000)))
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def main() -> None:
    import uvicorn
    args = parse_args()
    start(args)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
