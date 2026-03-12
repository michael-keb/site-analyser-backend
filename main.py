"""
main.py — FastAPI backend for the StoreConnect Site Analyser.

Endpoints:
  GET  /api/discover       — discover all URLs on a site (Firecrawl map)
  GET  /api/check          — check for a cached report
  GET  /api/analyse        — SSE stream: run full analysis pipeline
  POST /api/cancel         — cancel an in-flight job
  GET  /api/report/{id}    — serve the generated HTML report
  GET  /api/report/{id}/pdf — server-side PDF (Puppeteer; falls back to 404)
  POST /api/share          — generate a shareable token URL
  GET  /report/{token}     — resolve a share token and redirect to report

Run:
  uvicorn main:app --reload --port 8000

Frontend must be served separately (e.g. npx serve ../Frontend_siteanalyser -l 3000).
For local dev, CORS is open to localhost:3000. Set ALLOWED_ORIGINS in .env.local
for production.
"""

import os
import uuid
import asyncio
from urllib.parse import urlparse

from pydantic import BaseModel

from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from sse_starlette.sse import EventSourceResponse

from store import ReportStore
from analyser import run_analysis, discover_urls
from email_service import send_report_email
from pipedrive_service import create_lead as pipedrive_create_lead

# ── Load env ──────────────────────────────────────────────────────────────────
# Load all env files — each call with override=True means last one wins.
# Priority: root .env.local < backend .env.local < backend .env (highest)

_here = os.path.dirname(os.path.abspath(__file__))

load_dotenv(os.path.join(_here, "..", ".env.local"), override=True)
load_dotenv(os.path.join(_here, ".env.local"), override=True)
load_dotenv(os.path.join(_here, ".env"), override=True)

FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
PIPEDRIVE_API_KEY = (
    os.environ.get("PIPEDRIVE_API_KEY") or
    str(os.environ.get("Pipedrive", "")).strip('"\'')
)
MAX_PAGES = int(os.environ.get("MAX_PAGES", "15"))
ALLOWED_ORIGINS = os.environ.get(
    "ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000"
).split(",")

import logging
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)
_logger.info(f"FIRECRAWL_API_KEY: {'OK (fc-***)' if FIRECRAWL_API_KEY.startswith('fc-') else 'MISSING or invalid'}")
_logger.info(f"OPENAI_API_KEY: {'OK (sk-***)' if OPENAI_API_KEY.startswith('sk-') else 'not set — using fallback analysis'}")
_logger.info(f"PIPEDRIVE_API_KEY: {'OK' if PIPEDRIVE_API_KEY else 'not set — leads not created'}")

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(title="StoreConnect Site Analyser API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

report_store = ReportStore()

# In-memory job cancellation flags: job_id → asyncio.Event
_cancel_events: dict[str, asyncio.Event] = {}


class CreateLeadPayload(BaseModel):
    name: str = ""
    email: str = ""
    phone: str = ""
    url: str = ""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _validate_url(raw: str) -> str:
    """Normalise and validate a URL string. Raises HTTPException on failure."""
    url = raw.strip()
    if not url:
        raise HTTPException(status_code=400, detail="URL is required.")
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            raise ValueError
        # Remove trailing slash from root path
        if parsed.path == "/":
            url = url.rstrip("/")
        return url
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid URL.")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/api/discover")
async def discover(url: str = Query(..., description="Root URL to map")):
    """
    Phase 1: Use Firecrawl map() to discover all URLs on a site.
    Returns the list immediately — no SSE, no crawl credits consumed per page.
    Frontend caches this list then passes it to /api/analyse.
    """
    if not FIRECRAWL_API_KEY:
        raise HTTPException(status_code=503, detail="API key not configured.")

    try:
        normalised = _validate_url(url)
    except HTTPException:
        raise

    loop = asyncio.get_event_loop()
    urls = await loop.run_in_executor(
        None, discover_urls, normalised, FIRECRAWL_API_KEY, MAX_PAGES
    )
    return JSONResponse({"urls": urls, "total": len(urls)})


@app.post("/api/create-lead")
async def create_lead(payload: CreateLeadPayload):
    """
    Create a Pipedrive lead before starting analysis.
    Body: { name, email, phone, url }
    Returns { ok: true } on success, { ok: false, error: "..." } on failure.
    Frontend must call this and wait for success before proceeding to analysis.
    """
    if not PIPEDRIVE_API_KEY:
        return JSONResponse({"ok": False, "error": "Pipedrive not configured"})
    url = payload.url or ""
    try:
        normalised = _validate_url(url)
    except HTTPException:
        return JSONResponse({"ok": False, "error": "Invalid URL"})
    contact = {
        "name": (payload.name or "").strip(),
        "email": (payload.email or "").strip(),
        "phone": (payload.phone or "").strip(),
    }
    if not contact.get("name") and not contact.get("email"):
        return JSONResponse({"ok": False, "error": "Name or email required"})
    result = pipedrive_create_lead(PIPEDRIVE_API_KEY, contact, normalised)
    if result:
        return JSONResponse({"ok": True})
    return JSONResponse({"ok": False, "error": "Failed to create lead"})


@app.get("/api/check")
async def check_cached(url: str = Query(..., description="Site URL to check")):
    """Return cached report metadata if one exists for this URL."""
    try:
        normalised = _validate_url(url)
    except HTTPException:
        return JSONResponse({"cached": False})

    result = report_store.check_cached(normalised)
    if result:
        return JSONResponse(result)
    return JSONResponse({"cached": False})


@app.get("/api/analyse")
async def analyse(
    request: Request,
    url: str = Query(...),
    urls: str = Query(None, description="JSON-encoded list of URLs from /api/discover"),
    name: str = Query(None, description="Contact name"),
    email: str = Query(None, description="Contact email"),
    phone: str = Query(None, description="Contact phone"),
):
    """
    SSE stream: Phase 2 — scrape each URL one-by-one and emit per-page progress.
    If urls param provided (JSON array from /api/discover), skips discovery.
    """
    if not FIRECRAWL_API_KEY:
        async def _no_key():
            yield {
                "event": "error-event",
                "data": '{"code":"quota","message":"Analysis unavailable right now — API key not configured."}',
            }
        return EventSourceResponse(_no_key())

    try:
        normalised = _validate_url(url)
    except HTTPException as exc:
        import json as _json
        _detail = _json.dumps({"code": "invalid_url", "message": exc.detail})

        async def _bad_url():
            yield {"event": "error-event", "data": _detail}

        return EventSourceResponse(_bad_url())

    # Parse pre-discovered URL list if provided
    url_list = None
    if urls:
        import json as _json
        try:
            url_list = _json.loads(urls)
            if not isinstance(url_list, list):
                url_list = None
        except Exception:
            url_list = None

    contact = {"name": name or "", "email": email or "", "phone": phone or ""} if (name or email or phone) else None

    # Pipedrive lead is created by frontend via /api/create-lead before calling this

    job_id = uuid.uuid4().hex
    cancel_event = asyncio.Event()
    _cancel_events[job_id] = cancel_event

    async def event_generator():
        try:
            async for chunk in run_analysis(
                url=normalised,
                api_key=FIRECRAWL_API_KEY,
                report_store=report_store,
                job_id=job_id,
                max_pages=MAX_PAGES,
                url_list=url_list,
                openai_key=OPENAI_API_KEY,
                contact=contact,
            ):
                if cancel_event.is_set():
                    break
                lines = chunk.strip().split("\n")
                event_name = "message"
                data = ""
                for line in lines:
                    if line.startswith("event: "):
                        event_name = line[7:]
                    elif line.startswith("data: "):
                        data = line[6:]
                yield {"event": event_name, "data": data}

                # When analysis completes, send PDF to user's email in background
                if event_name == "complete" and data and contact and contact.get("email"):
                    import json as _json
                    try:
                        payload = _json.loads(data)
                        report_id = payload.get("reportId")
                        if report_id:
                            asyncio.create_task(send_report_email(report_id, contact, report_store))
                    except Exception as e:
                        _logger.warning("Could not queue report email: %s", e)
        except asyncio.CancelledError:
            pass
        finally:
            _cancel_events.pop(job_id, None)

    return EventSourceResponse(event_generator())


@app.post("/api/cancel")
async def cancel_job(payload: dict):
    """Signal a running analysis job to stop."""
    job_id = payload.get("jobId")
    if job_id and job_id in _cancel_events:
        _cancel_events[job_id].set()
    return JSONResponse({"ok": True})


@app.get("/api/report/{report_id}", response_class=HTMLResponse)
async def serve_report(report_id: str):
    """Serve the generated HTML report by ID."""
    html = report_store.get_html(report_id)
    if html is None:
        raise HTTPException(status_code=404, detail="Report not found.")
    return HTMLResponse(content=html)


@app.get("/api/report/{report_id}/pdf")
async def report_pdf(report_id: str):
    """
    Server-side PDF generation via Puppeteer (pyppeteer).
    Falls back to 501 if pyppeteer is not installed — the frontend
    will gracefully degrade to browser print.
    """
    try:
        import pyppeteer  # noqa: F401
    except ImportError:
        raise HTTPException(
            status_code=501,
            detail="Server-side PDF unavailable — use browser print.",
        )

    html = report_store.get_html(report_id)
    if html is None:
        raise HTTPException(status_code=404, detail="Report not found.")

    try:
        import pyppeteer
        browser = await pyppeteer.launch(args=["--no-sandbox"])
        page = await browser.newPage()
        await page.setContent(html, waitUntil="networkidle0")
        pdf_bytes = await page.pdf({
            "format": "A4",
            "printBackground": True,
            "margin": {"top": "1cm", "bottom": "1cm", "left": "1cm", "right": "1cm"},
        })
        await browser.close()

        meta = report_store.get_meta(report_id)
        domain = (meta or {}).get("domain", "report")
        from datetime import datetime
        date_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"{domain}-frontend-report-{date_str}.pdf"

        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"PDF generation failed: {exc}")


@app.post("/api/share")
async def create_share(payload: dict, request: Request):
    """Generate a shareable token URL for a report."""
    report_id = payload.get("reportId")
    if not report_id:
        raise HTTPException(status_code=400, detail="reportId is required.")

    try:
        token_data = report_store.create_share_token(report_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Report not found.")

    base = str(request.base_url).rstrip("/")
    share_url = f"{base}/report/{token_data['token']}"

    return JSONResponse({
        "url": share_url,
        "token": token_data["token"],
        "expiresAt": token_data["expires_at"],
    })


@app.get("/report/{token}")
async def resolve_share(token: str):
    """
    Resolve a share token and redirect to the report view.
    Returns the expired.html page if the token is invalid or expired.
    """
    report_id = report_store.resolve_share_token(token)

    if report_id is None:
        # Serve expired page
        expired_path = os.path.join(
            os.path.dirname(__file__), "..", "Frontend_siteanalyser", "expired.html"
        )
        if os.path.exists(expired_path):
            with open(expired_path, encoding="utf-8") as f:
                return HTMLResponse(content=f.read(), status_code=410)
        return HTMLResponse(
            content="<h1>This report has expired.</h1>",
            status_code=410,
        )

    # Redirect to the API report endpoint (served in the frontend iframe)
    return RedirectResponse(url=f"/api/report/{report_id}", status_code=302)


@app.get("/health")
async def health():
    return {"status": "ok", "api_key_set": bool(FIRECRAWL_API_KEY)}
