"""
Pipedrive integration — create a lead when a Site Analyser submission is received.
Uses pipetools pattern: create Person first, then Lead.
"""

import logging
from typing import Optional

import requests

BASE_URL = "https://api.pipedrive.com/v1"
_logger = logging.getLogger(__name__)


def create_lead(
    api_token: str,
    contact: dict,
    site_url: str,
) -> Optional[dict]:
    """
    Create a Pipedrive lead from a Site Analyser submission.
    Creates a Person first, then a Lead linked to that person.

    contact: {name, email, phone}
    site_url: the StoreConnect URL they submitted for analysis

    Returns the lead data on success, None on failure.
    """
    if not api_token or not contact:
        return None

    name = (contact.get("name") or "").strip()
    email = (contact.get("email") or "").strip()
    phone = (contact.get("phone") or "").strip()

    if not name and not email:
        _logger.warning("Pipedrive: need at least name or email to create lead")
        return None

    # 1. Create Person (Pipedrive v1: email/phone as arrays of {value, label})
    person_data = {"name": name or email or "Site Analyser submission"}
    if email:
        person_data["email"] = [{"value": email, "label": "work"}]
    if phone:
        person_data["phone"] = [{"value": phone, "label": "work"}]

    try:
        r = requests.post(
            f"{BASE_URL}/persons",
            params={"api_token": api_token},
            json=person_data,
            timeout=10,
        )
        resp = r.json()
        if not resp.get("success") or "data" not in resp:
            _logger.warning("Pipedrive create person failed: %s", resp)
            return None

        person_id = resp["data"]["id"]
    except Exception as e:
        _logger.warning("Pipedrive create person error: %s", e)
        return None

    # 2. Create Lead (linked to person)
    title = f"Site Analyser: {site_url}" if site_url else "Site Analyser submission"
    lead_data = {
        "title": title[:255],
        "person_id": person_id,
    }

    try:
        r = requests.post(
            f"{BASE_URL}/leads",
            params={"api_token": api_token},
            json=lead_data,
            timeout=10,
        )
        resp = r.json()
        if not resp.get("success") or "data" not in resp:
            _logger.warning("Pipedrive create lead failed: %s", resp)
            return None

        _logger.info("Pipedrive lead created: person_id=%s, lead_id=%s", person_id, resp["data"].get("id"))
        return resp["data"]
    except Exception as e:
        _logger.warning("Pipedrive create lead error: %s", e)
        return None
