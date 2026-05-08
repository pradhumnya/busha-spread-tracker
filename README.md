# PrimeVault USDT/NGN Rate Tracker

Tracks the USDT/NGN spread between the PrimeVault partner network rate and the ECB mid-market reference rate.

## Architecture

```
GitHub Actions (cron :05 every hour)
        |
        v
   poller.py  ──────────────────────────────────────► Supabase PostgreSQL
   (one-shot)                                              |
                                                          |
                                              Render Web Service
                                              server.py (FastAPI)
                                                          |
                                                          v
                                                   /dashboard (HTML)
                                                   /api/latest
                                                   /api/history
                                                   /health
```

**GitHub Actions** runs `poller.py` hourly. It fetches the USDT/NGN buy price from the Busha partner network and the ECB USD/NGN mid-market rate (via Frankfurter), applies the PrimeVault markup, and writes one row to Supabase Postgres.

**Render** serves `server.py` (FastAPI) as a read-only dashboard. It never polls — it only reads from the database. Free tier is fine.

---

## Deployment Runbook

### 1. Supabase setup

1. Create a project at [supabase.com](https://supabase.com)
2. Get the connection string: **Project Settings → Database → Connection string → URI tab → Transaction pooler**
3. That URI is your `DATABASE_URL` — it looks like `postgres://postgres.xxx:password@aws-0-xxx.pooler.supabase.com:6543/postgres`

The schema (tables, indexes, views) is created automatically on first run.

### 2. GitHub setup

1. Push this repo to GitHub
2. Add repository secrets (**Settings → Secrets and variables → Actions → New repository secret**):
   - `BUSHA_API_KEY` — your Busha API bearer token
   - `DATABASE_URL` — the Supabase connection URI from step 1
3. Manually trigger the workflow: **Actions → Hourly Rate Poll → Run workflow**
4. Confirm it completes green and a row appears in Supabase (`spread_snapshots` table)
5. The cron takes over automatically at :05 past every hour

### 3. Render setup

1. Go to [render.com](https://render.com) → **New → Blueprint**
2. Point it at your GitHub repo — Render reads `render.yaml` and provisions the web service
3. In the Render dashboard for the service, set the environment variable:
   - `DATABASE_URL` — same value as the GitHub secret
4. First deploy takes ~2 minutes; subsequent deploys are faster

### 4. UptimeRobot setup (keep free dyno warm)

1. Sign up at [uptimerobot.com](https://uptimerobot.com) (free tier is enough)
2. **New Monitor → HTTP(S)**
   - URL: `https://<your-render-slug>.onrender.com/health`
   - Interval: 5 minutes
3. This pings the service every 5 minutes so the free Render dyno never cold-starts for real visitors

### 5. Verify the chain

1. Open `/dashboard` — first visit shows empty charts with "No data yet for this window. Data updates hourly."
2. After 24 hours of hourly polling the 24h chart fills out
3. Check the **Actions** tab on GitHub to confirm hourly runs are green
4. `/health` returns `"stale": false` as long as the last snapshot is less than 2 hours old

---

## Environment variables

| Variable | Where | Required | Description |
|---|---|---|---|
| `DATABASE_URL` | GitHub secret + Render | Yes | Supabase Postgres connection URI |
| `BUSHA_API_KEY` | GitHub secret | Yes | Busha API bearer token |
| `PV_MARKUP_BPS` | GH Actions env + Render | No (default: 15) | Markup bps added over partner rate |
| `MID_PROVIDER` | GH Actions env | No (default: frankfurter) | `frankfurter`, `open_er_api`, `cbn`, or `static` |
| `SHEETS_ID` | GitHub secret (optional) | No | Google Sheet ID for append log |
| `GOOGLE_APPLICATION_CREDENTIALS` | GitHub secret (optional) | No | Path to service account JSON |

---

## API endpoints

| Endpoint | Description |
|---|---|
| `GET /dashboard` | HTML dashboard with charts and table |
| `GET /health` | Liveness + staleness check |
| `GET /api/latest` | Most recent snapshot as JSON |
| `GET /api/history?limit=N&from=ISO&to=ISO` | Historical snapshots |
| `GET /api/summary?window=24h\|7d\|30d\|all` | Aggregate stats for window |
| `GET /api/pairs` | Current rate in exchange-pair format |
