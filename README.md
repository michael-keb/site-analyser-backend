# Site Analyser — Backend

FastAPI backend for the StoreConnect Site Analyser.

## Deploy to Render

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/michael-keb/site-analyser-backend)

After deploy, set env vars in Render Dashboard: `FIRECRAWL_API_KEY`, `OPENAI_API_KEY`, `ALLOWED_ORIGINS` (add your frontend URL, e.g. `https://site-analyser-frontend.onrender.com`), and optionally `PIPEDRIVE_API_KEY` (creates a Pipedrive deal on each submission). Set `PIPEDRIVE_DOMAIN` if using a company subdomain (e.g. `yourcompany` for yourcompany.pipedrive.com). Wraps the Firecrawl crawl pipeline and streams real-time SSE progress events to the frontend.

## Setup

```bash
cd backend_siteanalyser
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Copy the env file and add your Firecrawl API key:

```bash
cp .env.example .env.local
# edit .env.local and set FIRECRAWL_API_KEY=fc-your-key
```

## Run

```bash
uvicorn main:app --reload --port 8000
```

The API will be available at `http://localhost:8000`.

Then start the frontend separately:

```bash
cd ../Frontend_siteanalyser
npm run dev    # starts on http://localhost:3000
```

## API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/check?url=` | Check for cached report |
| GET | `/api/analyse?url=` | SSE stream — run full analysis |
| POST | `/api/cancel` | Cancel an in-flight job `{ jobId }` |
| GET | `/api/report/:id` | Serve generated HTML report |
| GET | `/api/report/:id/pdf` | Server-side PDF (requires pyppeteer) |
| POST | `/api/share` | Generate shareable token `{ reportId }` |
| GET | `/report/:token` | Resolve share token → redirect |
| GET | `/health` | Health check |

## SSE Event Types

The `/api/analyse` endpoint streams these events:

| Event | Payload |
|-------|---------|
| `step` | `{ step, state: "active"\|"done"\|"error", detail?, jobId? }` |
| `crawl-progress` | `{ crawled, total }` |
| `warning` | `{ message }` |
| `complete` | `{ reportId }` |
| `error-event` | `{ code, message, partial?, reportId? }` |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FIRECRAWL_API_KEY` | — | Required. Get from firecrawl.dev |
| `MAX_PAGES` | `15` | Max pages crawled per analysis |
| `ALLOWED_ORIGINS` | `http://localhost:3000` | CORS origins (comma-separated) |
| `PIPEDRIVE_API_KEY` | — | Optional. Creates a Pipedrive deal when a submission includes contact info |
| `PIPEDRIVE_DOMAIN` | — | Optional. Company subdomain (e.g. `yourcompany` for yourcompany.pipedrive.com) |

## Reports

Generated reports are stored in `backend_siteanalyser/reports/` as self-contained HTML files. Share tokens are persisted in `reports/_tokens.json` and survive server restarts.
