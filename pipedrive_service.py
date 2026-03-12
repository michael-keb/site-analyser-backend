"""
Pipedrive integration — create a deal when a Site Analyser submission is received.
Uses template fields: Person (name*, email, phone), Organization (name* = "Site Analyser"), Deal (title, value).
"""

import logging
import os
from typing import Optional, Tuple

import requests

# Use company domain if set (e.g. "yourcompany" for yourcompany.pipedrive.com), else api.pipedrive.com
_domain = os.environ.get("PIPEDRIVE_DOMAIN", "").strip()
BASE_URL = f"https://{_domain}.pipedrive.com/api/v1" if _domain else "https://api.pipedrive.com/v1"
ORG_NAME = "Site Analyser"
_logger = logging.getLogger(__name__)
if _domain:
    _logger.info("Pipedrive using company domain: %s.pipedrive.com", _domain)


def _get_or_create_org(api_token: str) -> Tuple[Optional[int], Optional[str]]:
    """Get or create organization 'Site Analyser'. Returns (org_id, None) or (None, error)."""
    params = {"api_token": api_token, "term": ORG_NAME, "item_types": "organization", "exact_match": "true"}
    try:
        r = requests.get(f"{BASE_URL}/itemSearch", params=params, timeout=10)
        resp = r.json()
        items = resp.get("data", {}).get("items") or resp.get("data", {}).get("organization") or []
        if items:
            org = items[0] if isinstance(items[0], dict) else items[0].get("item", items[0])
            org_id = org.get("id") if isinstance(org, dict) else org
            if org_id:
                _logger.info("Pipedrive: using existing org '%s' id=%s", ORG_NAME, org_id)
                return int(org_id), None
    except Exception as e:
        _logger.debug("Pipedrive org search: %s", e)

    try:
        r = requests.post(
            f"{BASE_URL}/organizations",
            params={"api_token": api_token},
            json={"name": ORG_NAME},
            timeout=10,
        )
        resp = r.json()
        if resp.get("success") and resp.get("data"):
            org_id = resp["data"]["id"]
            _logger.info("Pipedrive: created org '%s' id=%s", ORG_NAME, org_id)
            return org_id, None
        err = resp.get("error", resp.get("error_info", str(resp)))
        return None, _extract_error(err)
    except requests.RequestException as e:
        return None, str(e) or "Request failed"
    except Exception as e:
        return None, str(e)


def create_deal(
    api_token: str,
    contact: dict,
    site_url: str,
) -> Tuple[Optional[dict], Optional[str]]:
    """
    Create a Pipedrive deal from a Site Analyser submission.
    Template: Person (name*, email, phone), Organization (name* = "Site Analyser"), Deal (title, value).

    contact: {name, email, phone}
    site_url: the StoreConnect URL they submitted for analysis

    Returns (deal_data, None) on success, (None, error_message) on failure.
    """
    if not api_token or not contact:
        return None, "Missing API token or contact"

    name = (contact.get("name") or "").strip()
    email = (contact.get("email") or "").strip()
    phone = (contact.get("phone") or "").strip()

    if not name and not email:
        _logger.warning("Pipedrive: need at least name or email to create deal")
        return None, "Name or email required"

    # 1. Get or create Organization "Site Analyser" (mandatory)
    org_id, err = _get_or_create_org(api_token)
    if org_id is None:
        return None, err or "Failed to get or create organization"

    # 2. Create Person (mandatory: name; suggested: email, phone)
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
            err = resp.get("error", resp.get("error_info", str(resp)))
            _logger.warning("Pipedrive create person failed: %s", resp)
            return None, _extract_error(err)

        person_id = resp["data"]["id"]
    except requests.RequestException as e:
        _logger.warning("Pipedrive create person request error: %s", e)
        return None, str(e) or "Request failed"
    except Exception as e:
        _logger.warning("Pipedrive create person error: %s", e)
        return None, str(e)

    # 3. Create Deal (title, person_id, org_id, value)
    title = f"Site Analyser: {site_url}" if site_url else "Site Analyser submission"
    deal_data = {
        "title": title[:255],
        "person_id": person_id,
        "org_id": org_id,
        "value": 0,
    }

    try:
        r = requests.post(
            f"{BASE_URL}/deals",
            params={"api_token": api_token},
            json=deal_data,
            timeout=10,
        )
        resp = r.json()
        if not resp.get("success") or "data" not in resp:
            err = resp.get("error", resp.get("error_info", str(resp)))
            _logger.warning("Pipedrive create deal failed: %s", resp)
            return None, _extract_error(err)

        _logger.info("Pipedrive deal created: person_id=%s, org_id=%s, deal_id=%s", person_id, org_id, resp["data"].get("id"))
        return resp["data"], None
    except requests.RequestException as e:
        _logger.warning("Pipedrive create deal request error: %s", e)
        return None, str(e) or "Request failed"
    except Exception as e:
        _logger.warning("Pipedrive create deal error: %s", e)
        return None, str(e)


def _extract_error(err) -> str:
    """Extract human-readable error from Pipedrive response."""
    if isinstance(err, str):
        return err
    if isinstance(err, dict):
        return err.get("message") or err.get("error") or str(err)
    return str(err)
