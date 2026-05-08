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
        spread_bps          DOUBLE PRECISION
    )""",
    """CREATE INDEX IF NOT EXISTS idx_spread_snapshots_ts
        ON spread_snapshots(fetched_ts_ms DESC)""",
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
    def __init__(self, db, busha_base, busha_api_key, provider, markup_bps=DEFAULT_MARKUP_BPS):
        self.db = db
        self.busha_base = busha_base.rstrip("/")
        self.provider = provider
        self.markup_bps = markup_bps
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
        try:
            r = self.session.get(f"{self.busha_base}/v1/pairs",
                                  params={"base": "USDT", "counter": "NGN"}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            body = r.json()
            for p in (body.get("data") or []):
                if (p.get("id") or "").upper() == "USDTNGN":
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

        self.db.execute_write(
            """INSERT INTO spread_snapshots
               (fetched_at, fetched_ts_ms, busha_rate, markup_bps, markup_amount,
                quoted_rate, quoted_source, mid_market_rate, mid_market_source,
                mid_market_age_sec, spread_abs, spread_pct, spread_bps)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (fetched_at, fetched_ts_ms, busha_rate, self.markup_bps, markup_amount,
             display_rate, "PrimeVault Partner Network (USDT/NGN)",
             mid, self.provider.name, 0,
             spread_abs, spread_pct, spread_bps_val),
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
<title>USDT/NGN &mdash; PrimeVault Rates</title>
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
</style>
</head>
<body>
  <h1>USDT/NGN &mdash; PrimeVault Rates</h1>
  <div class="sub"><span id="pulse" class="pulse"></span><span id="status">Connecting...</span></div>

  <div class="caveat">
    Mid-market reference is USD/NGN from open.er-api.com. USDT trades within &plusmn;0.1% of USD. Rates update hourly.
  </div>

  <div class="cards">
    <div class="card">
      <div class="label">PrimeVault Quoted Rate</div>
      <div class="value"><span id="quoted">&mdash;</span> <small style="font-size:13px;color:var(--muted)">NGN</small></div>
      <div class="footnote">NGN per 1 USDT</div>
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

  <table>
    <thead>
      <tr>
        <th>Timestamp (UTC)</th>
        <th>PV Quoted Rate</th>
        <th>Mid-Market Rate</th>
        <th>Spread (NGN)</th>
        <th>Spread %</th>
        <th>Spread bps</th>
      </tr>
    </thead>
    <tbody id="tbody">
      <tr><td colspan="6" class="no-data">Loading&#x2026;</td></tr>
    </tbody>
  </table>

  <div class="meta" id="meta">—</div>

  <div class="chart-wrap" style="margin-top:24px">
    <h2>NGN/USDT Rates</h2>
    <canvas id="ratesChart" height="120"></canvas>
  </div>

  <div class="chart-wrap">
    <h2>Spread (bps)</h2>
    <canvas id="spreadChart" height="80"></canvas>
  </div>

<script>
const fmt = (v, d=2) => v == null ? "—" : Number(v).toLocaleString(undefined, {minimumFractionDigits: d, maximumFractionDigits: d});
const fmtTime = ts => {
  if (!ts) return "—";
  const s = String(ts);
  return s.replace("T", " ").slice(0, 19);
};
const sign = v => v == null ? "" : (v > 0 ? "pos" : "neg");

let _window = "24h";
let ratesChart = null;
let spreadChart = null;

function windowToFrom(w) {
  const now = new Date();
  if (w === "24h") {
    const d = new Date(now - 24*60*60*1000);
    return d.toISOString();
  }
  if (w === "7d") {
    const d = new Date(now - 7*24*60*60*1000);
    return d.toISOString();
  }
  if (w === "30d") {
    const d = new Date(now - 30*24*60*60*1000);
    return d.toISOString();
  }
  return null; // all time
}

function setWindow(w) {
  _window = w;
  document.querySelectorAll(".pill").forEach(p => {
    p.classList.toggle("active", p.textContent.trim() === w || (w === "all" && p.textContent.trim() === "All time"));
  });
  const from = windowToFrom(w);
  const rangeEl = document.getElementById("window_range");
  if (from) {
    rangeEl.textContent = fmtTime(from) + " → now";
  } else {
    rangeEl.textContent = "All available data";
  }
  tick();
}

function initCharts() {
  const rCtx = document.getElementById("ratesChart").getContext("2d");
  ratesChart = new Chart(rCtx, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "PV Quoted Rate",
          data: [],
          borderColor: "#0b6e3a",
          backgroundColor: "rgba(11,110,58,0.08)",
          borderWidth: 2,
          pointRadius: 2,
          tension: 0.3,
        },
        {
          label: "Mid-Market Rate",
          data: [],
          borderColor: "#b8740a",
          backgroundColor: "rgba(184,116,10,0.08)",
          borderWidth: 2,
          pointRadius: 2,
          tension: 0.3,
          borderDash: [4, 3],
        },
      ],
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "top" } },
      scales: {
        x: { ticks: { maxTicksLimit: 8, maxRotation: 0 } },
        y: { ticks: { callback: v => Number(v).toLocaleString() } },
      },
    },
  });

  const sCtx = document.getElementById("spreadChart").getContext("2d");
  spreadChart = new Chart(sCtx, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "Spread bps",
          data: [],
          borderColor: "#b3261e",
          backgroundColor: "rgba(179,38,30,0.08)",
          borderWidth: 2,
          pointRadius: 2,
          tension: 0.3,
          fill: true,
        },
      ],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { maxTicksLimit: 8, maxRotation: 0 } },
        y: { ticks: { callback: v => v + " bps" } },
      },
    },
  });
}

function updateCharts(rows) {
  // rows are newest-first; reverse for chronological display
  const sorted = [...rows].reverse();
  const labels = sorted.map(r => fmtTime(r.fetched_at));
  const quoted = sorted.map(r => r.quoted_rate);
  const mid = sorted.map(r => r.mid_market_rate);
  const bps = sorted.map(r => r.spread_bps);

  ratesChart.data.labels = labels;
  ratesChart.data.datasets[0].data = quoted;
  ratesChart.data.datasets[1].data = mid;
  ratesChart.update("none");

  spreadChart.data.labels = labels;
  spreadChart.data.datasets[0].data = bps;
  spreadChart.update("none");
}

async function tick() {
  try {
    const fromTs = windowToFrom(_window);
    let histUrl = "/api/history?limit=500";
    if (fromTs) histUrl += "&from=" + encodeURIComponent(fromTs);

    const [latestRes, histRes, healthRes] = await Promise.all([
      fetch("/api/latest").then(r => r.ok ? r.json() : null),
      fetch(histUrl).then(r => r.json()),
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
      document.getElementById("mid_source").textContent = "";
    }

    const rows = (histRes && histRes.data) ? histRes.data : [];
    const tbody = document.getElementById("tbody");
    if (rows.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" class="no-data">No data yet for this window. Data updates hourly.</td></tr>';
      updateCharts([]);
    } else {
      tbody.innerHTML = rows.slice(0, 30).map((r, i) => `
        <tr class="${i === 0 ? 'live' : ''}">
          <td>${fmtTime(r.fetched_at)}</td>
          <td>${fmt(r.quoted_rate, 4)}</td>
          <td>${fmt(r.mid_market_rate, 4)}</td>
          <td class="${sign(r.spread_abs)}">${fmt(r.spread_abs, 2)}</td>
          <td class="${sign(r.spread_pct)}">${fmt(r.spread_pct, 2)}%</td>
          <td class="${sign(r.spread_bps)}">${fmt(r.spread_bps, 0)}</td>
        </tr>
      `).join("");
      updateCharts(rows);
    }

    const stale = healthRes && healthRes.stale;
    document.getElementById("pulse").className = "pulse" + (stale ? " stale" : "");
    const snapCount = (healthRes && healthRes.snapshot_count != null) ? healthRes.snapshot_count : "?";
    const lastAt = (healthRes && healthRes.last_snapshot_at) ? healthRes.last_snapshot_at : null;
    document.getElementById("status").textContent =
      snapCount + " snapshots" +
      (lastAt ? " · last updated " + fmtTime(lastAt) : "") +
      (stale ? " · STALE" : "");
    document.getElementById("meta").textContent =
      "";
  } catch (e) {
    document.getElementById("status").textContent = "Connection error: " + e;
    document.getElementById("pulse").className = "pulse stale";
  }
}

initCharts();
tick();
setInterval(tick, 60000);
</script>
</body>
</html>
"""
