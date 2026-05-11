#!/usr/bin/env python3
"""Read-only FastAPI dashboard. No background threads."""
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from busha_spread_tracker import (
    DEFAULT_MARKUP_BPS, DASHBOARD_HTML_TEMPLATE,
    make_database, init_db, Database,
    BUSHA_PROD, BUSHA_SANDBOX, make_provider, SpreadPoller,
)

_db: Optional[Database] = None
_markup_bps: float = float(os.environ.get("PV_MARKUP_BPS", DEFAULT_MARKUP_BPS))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _db
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    _db = make_database()
    init_db(_db)
    yield
    if _db:
        _db.close()


app = FastAPI(title="PrimeVault USDT/NGN Rates", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return FileResponse("favicon.ico")


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/ngn_usdt")


@app.get("/ngn_usdt", response_class=HTMLResponse)
def dashboard():
    html = DASHBOARD_HTML_TEMPLATE.replace("__MARKUP_BPS__", str(int(_markup_bps)))
    return HTMLResponse(html)


@app.get("/health")
def health():
    assert _db is not None
    try:
        rows = _db.execute_read(
            "SELECT MAX(fetched_ts_ms) AS t, COUNT(*) AS n, MAX(fetched_at) AS last_at "
            "FROM spread_snapshots"
        )
    except Exception as e:
        raise HTTPException(503, f"DB error: {e}")
    row = rows[0] if rows else {}
    last_ts = row.get("t")
    lag = (time.time() * 1000 - last_ts) / 1000.0 if last_ts else None
    return {
        "status": "ok",
        "snapshot_count": row.get("n", 0),
        "last_snapshot_at": row.get("last_at"),
        "lag_seconds": round(lag, 0) if lag is not None else None,
        "stale": (lag is not None and lag > 7200),
    }


@app.get("/api/latest")
def api_latest():
    assert _db is not None
    rows = _db.execute_read("SELECT * FROM spread_snapshots WHERE quoted_rate IS NOT NULL ORDER BY fetched_ts_ms DESC LIMIT 1")
    if not rows:
        raise HTTPException(404, "no snapshots yet")
    return rows[0]


@app.get("/api/history")
def api_history(
    limit: int = Query(500, gt=0, le=5000),
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None),
):
    assert _db is not None
    conditions = ["quoted_rate IS NOT NULL"]
    params = []
    if from_:
        conditions.append("fetched_at >= %s")
        params.append(from_)
    if to:
        conditions.append("fetched_at <= %s")
        params.append(to)
    where = "WHERE " + " AND ".join(conditions)
    params.append(limit)
    rows = _db.execute_read(
        f"SELECT * FROM spread_snapshots {where} ORDER BY fetched_ts_ms DESC LIMIT %s",
        tuple(params),
    )
    return {"count": len(rows), "data": rows}


@app.get("/api/summary")
def api_summary(window: str = Query("24h")):
    assert _db is not None
    window_hours = {"24h": 24, "7d": 168, "30d": 720, "all": None}
    if window not in window_hours:
        raise HTTPException(400, f"window must be one of {list(window_hours)}")
    hours = window_hours[window]
    params = []
    base_where = "WHERE quoted_rate IS NOT NULL AND mid_market_rate IS NOT NULL"
    if hours:
        from_dt = datetime.now(timezone.utc) - timedelta(hours=hours)
        from_str = from_dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        base_where += " AND fetched_at >= %s"
        params.append(from_str)
    rows = _db.execute_read(
        f"""SELECT COUNT(*) AS samples,
               MIN(fetched_at) AS first_at, MAX(fetched_at) AS last_at,
               AVG(spread_pct) AS avg_spread_pct,
               MIN(spread_pct) AS min_spread_pct,
               MAX(spread_pct) AS max_spread_pct,
               AVG(spread_bps) AS avg_spread_bps,
               MIN(spread_bps) AS min_spread_bps,
               MAX(spread_bps) AS max_spread_bps,
               AVG(quoted_rate) AS avg_quoted_rate,
               AVG(mid_market_rate) AS avg_mid_rate
            FROM spread_snapshots {base_where}""",
        tuple(params),
    )
    result = rows[0] if rows else {}
    result["window"] = window
    return result


@app.get("/api/pairs")
def api_pairs():
    assert _db is not None
    rows = _db.execute_read(
        "SELECT fetched_at, quoted_rate, mid_market_rate, spread_bps "
        "FROM spread_snapshots WHERE quoted_rate IS NOT NULL ORDER BY fetched_ts_ms DESC LIMIT 1"
    )
    latest = rows[0] if rows else {}
    return {"pairs": [{"id": "USDTNGN", "base": "USDT", "counter": "NGN",
                        "latest_quoted_rate": latest.get("quoted_rate"),
                        "latest_mid_rate": latest.get("mid_market_rate"),
                        "latest_spread_bps": latest.get("spread_bps"),
                        "updated_at": latest.get("fetched_at")}]}


@app.get("/run-poll")
def run_poll(secret: str = Query(...)):
    poll_secret = os.environ.get("POLL_SECRET", "")
    if not poll_secret or secret != poll_secret:
        raise HTTPException(403, "invalid secret")
    assert _db is not None
    env = os.environ.get("BUSHA_ENV", "prod")
    busha_base = BUSHA_PROD if env == "prod" else BUSHA_SANDBOX
    api_key = os.environ.get("BUSHA_API_KEY")
    markup_bps = float(os.environ.get("PV_MARKUP_BPS", DEFAULT_MARKUP_BPS))
    mid_provider = make_provider()
    poller = SpreadPoller(
        db=_db,
        busha_base=busha_base,
        busha_api_key=api_key,
        provider=mid_provider,
        markup_bps=markup_bps,
    )
    try:
        snap = poller.poll_once()
        return {"ok": True, "snapshot": snap}
    except Exception as e:
        raise HTTPException(500, f"poll failed: {e}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
