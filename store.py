"""
store.py — Report persistence and share-token management.

Reports are stored as HTML files under:
  reports/<report_id>/report.html
  reports/<report_id>/meta.json

Share tokens map a short token → report_id with an expiry timestamp.
In-memory index is rebuilt from disk on startup, so restarts don't lose tokens.
"""

import os
import json
import uuid
import hashlib
import time
from datetime import datetime, timezone, timedelta
from threading import Lock
from urllib.parse import urlparse


REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports")
SHARE_EXPIRY_DAYS = 30


class ReportStore:
    def __init__(self, reports_dir: str = REPORTS_DIR):
        self.reports_dir = reports_dir
        os.makedirs(reports_dir, exist_ok=True)
        self._lock = Lock()
        # token → { report_id, expires_at (ISO) }
        self._tokens: dict[str, dict] = {}
        self._rebuild_token_index()

    # ── Save ──────────────────────────────────────────────────────────────────

    def save(self, url: str, html: str, signals: dict, ratings: dict, contact: dict | None = None) -> str:
        """
        Persist a report and return its report_id.
        If an identical URL was analysed within the last 24 hours, returns
        the existing report_id (cache).
        """
        report_id = self._make_id(url)
        report_dir = os.path.join(self.reports_dir, report_id)
        os.makedirs(report_dir, exist_ok=True)

        with open(os.path.join(report_dir, "report.html"), "w", encoding="utf-8") as f:
            f.write(html)

        meta = {
            "report_id": report_id,
            "url": url,
            "domain": urlparse(url).netloc,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "ratings": ratings,
            "contact": contact or {},
            "signals_summary": {
                "pages_analyzed": signals.get("pages_analyzed", 0),
                "avg_tags": signals.get("avg_tags", 0),
                "avg_scripts": signals.get("avg_scripts", 0),
                "js_frameworks": signals.get("js_frameworks", []),
            },
        }
        with open(os.path.join(report_dir, "meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

        return report_id

    # ── Load ─────────────────────────────────────────────────────────────────

    def get_html(self, report_id: str) -> str | None:
        path = os.path.join(self.reports_dir, report_id, "report.html")
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as f:
            return f.read()

    def get_meta(self, report_id: str) -> dict | None:
        path = os.path.join(self.reports_dir, report_id, "meta.json")
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    # ── Cache check ───────────────────────────────────────────────────────────

    def check_cached(self, url: str, max_age_hours: int = 24) -> dict | None:
        """
        Return { reportId, created_at } if a report for this URL exists and
        is younger than max_age_hours, otherwise None.
        """
        report_id = self._make_id(url)
        meta = self.get_meta(report_id)
        if not meta:
            return None

        created = datetime.fromisoformat(meta["created_at"])
        age = datetime.now(timezone.utc) - created
        if age.total_seconds() > max_age_hours * 3600:
            return None

        return {
            "cached": True,
            "reportId": report_id,
            "created_at": meta["created_at"],
        }

    # ── Share tokens ──────────────────────────────────────────────────────────

    def create_share_token(self, report_id: str) -> dict:
        """
        Generate a short share token for a report.
        Returns { token, url_path, expires_at }.
        """
        if self.get_meta(report_id) is None:
            raise ValueError(f"Report {report_id} not found")

        token = uuid.uuid4().hex[:12]
        expires_at = (datetime.now(timezone.utc) + timedelta(days=SHARE_EXPIRY_DAYS)).isoformat()

        with self._lock:
            self._tokens[token] = {
                "report_id": report_id,
                "expires_at": expires_at,
            }
            self._persist_tokens()

        return {
            "token": token,
            "expires_at": expires_at,
        }

    def resolve_share_token(self, token: str) -> str | None:
        """
        Return the report_id for a valid, non-expired token, or None.
        """
        with self._lock:
            entry = self._tokens.get(token)
            if not entry:
                return None
            expires = datetime.fromisoformat(entry["expires_at"])
            if datetime.now(timezone.utc) > expires:
                # Clean up expired token
                del self._tokens[token]
                self._persist_tokens()
                return None
            return entry["report_id"]

    # ── Internals ─────────────────────────────────────────────────────────────

    def _make_id(self, url: str) -> str:
        """Deterministic report ID based on normalised URL."""
        normalised = url.rstrip("/").lower()
        return hashlib.sha256(normalised.encode()).hexdigest()[:16]

    def _tokens_path(self) -> str:
        return os.path.join(self.reports_dir, "_tokens.json")

    def _persist_tokens(self):
        with open(self._tokens_path(), "w") as f:
            json.dump(self._tokens, f, indent=2)

    def _rebuild_token_index(self):
        path = self._tokens_path()
        if os.path.exists(path):
            try:
                with open(path) as f:
                    self._tokens = json.load(f)
            except (json.JSONDecodeError, OSError):
                self._tokens = {}
        # Purge expired tokens on startup
        now = datetime.now(timezone.utc)
        self._tokens = {
            t: v for t, v in self._tokens.items()
            if datetime.fromisoformat(v["expires_at"]) > now
        }
        self._persist_tokens()
