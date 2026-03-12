"""
email_service.py — Send report PDF to user after analysis completes.

Uses SMTP (configurable via env). If SMTP is not configured, logs a warning
and skips sending. PDF is generated via pyppeteer (same as /api/report/{id}/pdf).
"""

import os
import smtplib
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

_logger = logging.getLogger(__name__)


def _get_smtp_config():
    """Return SMTP config from env, or None if not configured."""
    host = os.environ.get("SMTP_HOST", "").strip()
    if not host:
        return None
    return {
        "host": host,
        "port": int(os.environ.get("SMTP_PORT", "587")),
        "user": os.environ.get("SMTP_USER", "").strip() or None,
        "password": os.environ.get("SMTP_PASSWORD", "").strip() or None,
        "from_addr": os.environ.get("SMTP_FROM", "").strip() or "noreply@example.com",
        "use_tls": os.environ.get("SMTP_USE_TLS", "true").lower() in ("true", "1", "yes"),
    }


async def _generate_pdf(report_store, report_id: str) -> bytes | None:
    """Generate PDF bytes from report HTML using pyppeteer."""
    try:
        import pyppeteer
    except ImportError:
        _logger.warning("pyppeteer not installed — cannot generate PDF for email")
        return None

    html = report_store.get_html(report_id)
    if not html:
        _logger.error("Report HTML not found for %s", report_id)
        return None

    try:
        browser = await pyppeteer.launch(args=["--no-sandbox"])
        page = await browser.newPage()
        await page.setContent(html, waitUntil="networkidle0")
        pdf_bytes = await page.pdf({
            "format": "A4",
            "printBackground": True,
            "margin": {"top": "1cm", "bottom": "1cm", "left": "1cm", "right": "1cm"},
        })
        await browser.close()
        return pdf_bytes
    except Exception as e:
        _logger.exception("PDF generation failed for %s: %s", report_id, e)
        return None


def _send_email_sync(to_email: str, subject: str, body: str, pdf_bytes: bytes, filename: str, config: dict) -> bool:
    """Send email with PDF attachment. Blocking."""
    try:
        msg = MIMEMultipart()
        msg["From"] = config["from_addr"]
        msg["To"] = to_email
        msg["Subject"] = subject

        msg.attach(MIMEText(body, "plain"))

        part = MIMEApplication(pdf_bytes, _subtype="pdf")
        part.add_header("Content-Disposition", "attachment", filename=filename)
        msg.attach(part)

        with smtplib.SMTP(config["host"], config["port"]) as smtp:
            if config["use_tls"]:
                smtp.starttls()
            if config["user"] and config["password"]:
                smtp.login(config["user"], config["password"])
            smtp.send_message(msg)

        _logger.info("Report PDF sent to %s", to_email)
        return True
    except Exception as e:
        _logger.exception("Failed to send report email to %s: %s", to_email, e)
        return False


async def send_report_email(report_id: str, contact: dict, report_store) -> bool:
    """
    Generate report PDF and email it to the contact.
    Returns True if sent successfully, False otherwise.
    """
    email = (contact or {}).get("email", "").strip()
    if not email:
        _logger.debug("No email in contact — skipping report email")
        return False

    config = _get_smtp_config()
    if not config:
        _logger.warning("SMTP not configured — report PDF not sent. Set SMTP_HOST in .env")
        return False

    pdf_bytes = await _generate_pdf(report_store, report_id)
    if not pdf_bytes:
        return False

    meta = report_store.get_meta(report_id)
    domain = (meta or {}).get("domain", "report")
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{domain}-frontend-report-{date_str}.pdf"

    name = (contact or {}).get("name", "").strip() or "there"
    subject = f"Your StoreConnect Frontend Report — {domain}"
    body = f"""Hi {name},

Your StoreConnect frontend capability report for {domain} is ready.

Please find the PDF attached.

Best regards,
Praxis — StoreConnect Frontend Custodian
"""

    import asyncio
    return await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: _send_email_sync(email, subject, body, pdf_bytes, filename, config),
    )
