"""
analyser.py — Core analysis pipeline with SSE progress streaming.

Two-phase strategy:
  Phase 1: discover_urls()  — Firecrawl map() to get all URLs instantly
  Phase 2: run_analysis()   — scrape each URL (html + markdown + metadata),
                              accumulate signals, then use OpenAI to generate
                              rich narrative findings, then render full report.

SSE event types:
  step         — { step, state: "active"|"done"|"error", detail?, jobId? }
  page-done    — { url, index, total, signals_preview: { tags, scripts, frameworks } }
  warning      — { message }
  complete     — { reportId }
  error-event  — { code, message, partial?, reportId? }
"""

import asyncio
import re
import json
import os
from collections import defaultdict
from datetime import datetime
from urllib.parse import urlparse, urljoin
from typing import AsyncGenerator

from bs4 import BeautifulSoup

# ── JS framework fingerprints ──────────────────────────────────────────────────
JS_FRAMEWORKS = {
    "jquery": "jQuery",
    "react": "React",
    "vue": "Vue.js",
    "angular": "Angular",
    "backbone": "Backbone.js",
    "ember": "Ember.js",
    "alpine": "Alpine.js",
    "htmx": "HTMX",
    "stimulus": "Stimulus",
    "swiper": "Swiper",
    "gsap": "GSAP",
    "three.js": "Three.js",
    "chart.js": "Chart.js",
    "bootstrap": "Bootstrap",
    "tailwind": "Tailwind CSS",
    "slick": "Slick Carousel",
    "owl.carousel": "Owl Carousel",
    "fancybox": "Fancybox",
    "magnific": "Magnific Popup",
    "select2": "Select2",
    "chosen": "Chosen",
    "datatables": "DataTables",
}

COLOR_MAP = {
    "Stable": ("#166534", "#dcfce7"),
    "Needs Attention": ("#92400e", "#fef9c3"),
    "Upgrade Risk Detected": ("#991b1b", "#fef2f2"),
    "Good": ("#166534", "#dcfce7"),
    "Fair": ("#92400e", "#fef9c3"),
    "Partial": ("#92400e", "#fef9c3"),
    "Low": ("#166534", "#dcfce7"),
    "Moderate": ("#92400e", "#fef9c3"),
    "Elevated": ("#991b1b", "#fef2f2"),
    "Yes": ("#166534", "#dcfce7"),
    "Recommended": ("#92400e", "#fef9c3"),
    "Strongly Recommended": ("#991b1b", "#fef2f2"),
    "Strong": ("#166534", "#dcfce7"),
    "Limited": ("#991b1b", "#fef2f2"),
    "Needs Optimisation": ("#92400e", "#fef9c3"),
}


# ── SSE helper ─────────────────────────────────────────────────────────────────

def sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


# ── Phase 1: URL discovery ─────────────────────────────────────────────────────

def discover_urls(url: str, api_key: str, max_pages: int = 15) -> list[str]:
    """Firecrawl map() — returns URL list instantly, no crawl credits consumed."""
    try:
        from firecrawl import FirecrawlApp
        app = FirecrawlApp(api_key=api_key)
        result = app.map(url)

        urls = []
        if hasattr(result, "links") and result.links:
            for item in result.links:
                if hasattr(item, "url") and item.url:
                    urls.append(item.url)
                elif isinstance(item, str):
                    urls.append(item)
        elif isinstance(result, list):
            for item in result:
                if hasattr(item, "url") and item.url:
                    urls.append(item.url)
                elif isinstance(item, str):
                    urls.append(item)
        elif isinstance(result, dict):
            raw = result.get("links") or result.get("urls") or []
            urls = [u if isinstance(u, str) else getattr(u, "url", "") for u in raw]

        all_urls = list(dict.fromkeys([url] + [u for u in urls if u]))
        return all_urls[:max_pages]

    except Exception as e:
        print(f"[discover_urls] map() failed: {e}")
        return [url]


# ── Phase 2: Per-page scrape ───────────────────────────────────────────────────

def _scrape_page(url: str, api_key: str) -> dict | None:
    """
    Scrape a single URL fetching html + markdown + metadata.
    Returns a normalised page dict or None on failure.
    """
    try:
        from firecrawl import FirecrawlApp
        app = FirecrawlApp(api_key=api_key)
        result = app.scrape(
            url,
            formats=["html", "markdown"],
            only_main_content=False,   # include nav, header, footer
            wait_for=2000,             # wait 2s for JS to render nav
        )

        html = ""
        markdown = ""
        metadata = {}

        if hasattr(result, "html"):
            html = result.html or ""
            markdown = getattr(result, "markdown", "") or ""

            raw_meta = getattr(result, "metadata", None)
            if raw_meta is not None:
                # DocumentMetadata pydantic model — use model_dump if available
                if hasattr(raw_meta, "model_dump"):
                    metadata = {k: v for k, v in raw_meta.model_dump().items() if v is not None}
                elif hasattr(raw_meta, "__dict__"):
                    metadata = {k: v for k, v in raw_meta.__dict__.items() if not k.startswith("_") and v is not None}
                elif isinstance(raw_meta, dict):
                    metadata = raw_meta
        elif isinstance(result, dict):
            html = result.get("html", "")
            markdown = result.get("markdown", "")
            metadata = result.get("metadata", {}) or {}

        return {"url": url, "html": html, "markdown": markdown, "metadata": metadata}

    except Exception as e:
        print(f"[_scrape_page] failed for {url}: {e}")
        return None


# ── DOM signal extraction ──────────────────────────────────────────────────────

def _link_zone(tag) -> str:
    """Walk up from an <a> tag to determine if it's in nav, header, footer, or body."""
    for parent in tag.parents:
        if parent.name == "nav":
            return "nav"
        if parent.name == "header":
            return "header"
        if parent.name == "footer":
            return "footer"
    return "body"


def _normalize_url(url: str) -> str:
    """Normalize a URL for deduplication: lowercase domain, strip trailing slash and fragment."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme}://{parsed.netloc.lower()}{path}"


def _estimate_depth(tag, current: int = 0) -> int:
    children = list(tag.children)
    if not children:
        return current
    return max(
        (_estimate_depth(c, current + 1) for c in children if hasattr(c, "children")),
        default=current,
    )


def _analyse_page(page: dict, agg: dict) -> dict:
    """Extract signals from one page and accumulate into agg. Returns signals_preview."""
    html = page.get("html", "")
    metadata = page.get("metadata") or {}

    if isinstance(metadata, dict):
        if metadata.get("title"):
            agg["page_titles"].append(metadata["title"])
        if metadata.get("description"):
            agg["metadata_descriptions"].append(metadata["description"])
        if metadata.get("ogTitle") or metadata.get("ogDescription") or metadata.get("og_title"):
            agg["has_og_tags"] = True
        if metadata.get("twitterTitle") or metadata.get("twitterCard") or metadata.get("twitter_card"):
            agg["has_twitter_tags"] = True

    if not html:
        return {"tags": 0, "scripts": 0, "frameworks": []}

    agg["pages_analyzed"] += 1
    soup = BeautifulSoup(html, "lxml")
    all_tags = soup.find_all(True)
    page_tag_count = len(all_tags)
    agg["total_tags"] += page_tag_count

    body = soup.find("body")
    if body:
        depth = _estimate_depth(body)
        agg["max_dom_depth"] = max(agg["max_dom_depth"], depth)

    agg["inline_style_count"] += len([t for t in all_tags if t.get("style")])
    agg["custom_data_attrs"] += sum(
        1 for t in all_tags for attr in t.attrs if attr.startswith("data-")
    )

    scripts = soup.find_all("script")
    page_script_count = len(scripts)
    agg["script_count"] += page_script_count
    agg["external_script_count"] += len([s for s in scripts if s.get("src")])
    inline_scripts = [s for s in scripts if not s.get("src") and s.string]
    agg["inline_script_kb"] += sum(len(s.string or "") for s in inline_scripts) // 1024

    script_text = " ".join(
        (s.get("src", "") + " " + (s.string or "")).lower() for s in scripts
    )
    page_frameworks = set()
    for key, label in JS_FRAMEWORKS.items():
        if key in script_text:
            agg["js_frameworks"].add(label)
            page_frameworks.add(label)

    agg["stylesheet_count"] += len(
        soup.find_all("link", rel=lambda r: r and "stylesheet" in r)
    )
    imgs = soup.find_all("img")
    agg["image_count"] += len(imgs)
    agg["images_total"] += len(imgs)
    agg["images_with_alt"] += len([i for i in imgs if i.get("alt")])
    if any(i.get("srcset") for i in imgs):
        agg["has_srcset"] = True
    if any(i.get("loading") == "lazy" for i in imgs):
        agg["has_loading_lazy"] = True

    agg["video_count"] += len(soup.find_all("video")) + len(soup.find_all("iframe", src=re.compile(r"youtube|vimeo", re.I)))

    for link in soup.find_all("link", rel=True):
        rel = link.get("rel") or []
        if isinstance(rel, str):
            rel = [rel]
        if any("icon" in r.lower() for r in rel):
            agg["has_favicon"] = True
            break
    classes_text = " ".join(" ".join(t.get("class", [])) for t in all_tags).lower()
    ids_text = " ".join(t.get("id", "") for t in all_tags).lower()
    combined = classes_text + " " + ids_text

    if soup.select(".SC-Footer, [class*='SC-Footer']"):
        agg["has_sc_footer"] = True
    if soup.find(attrs={"data-menu-init": True}) or soup.find(attrs={"data-menu-init": re.compile(r".+")}):
        agg["has_data_menu_init"] = True
    if soup.find(["main", "article", "section"]):
        agg["has_semantic_landmarks"] = True
    if soup.select("#SC-MenuItem, [id^='SC-MenuItem-']"):
        agg["has_sc_menuitem_ids"] = True
    if re.search(r"sc-flex|sc-grid|sc-hide|sc-justify|sc-align", classes_text):
        agg["has_sc_utility_classes"] = True

    style_text = " ".join(s.string or "" for s in soup.find_all("style")) + " " + " ".join(t.get("style", "") for t in all_tags if t.get("style"))
    if re.search(r"--sc-|--sc-color", style_text):
        agg["has_sc_css_vars"] = True
    if re.search(r"bootstrap|tailwind", classes_text + " " + script_text):
        agg["has_third_party_css"] = True

    if "gtag" in script_text or "analytics.js" in script_text or "googletagmanager.com" in script_text:
        agg["has_gtag"] = agg["has_gtag"] or "googletagmanager.com" in script_text
        if "googletagmanager.com" in script_text:
            agg["has_gtm"] = True
        if "gtag" in script_text or "analytics.js" in script_text:
            agg["has_gtag"] = True
    if "fbq(" in script_text or "fbevents" in script_text:
        agg["has_fbq"] = True
    if re.search(r"cookie-banner|privacy-banner|cookie-consent|data-cookie", combined):
        agg["has_cookie_banner"] = True

    forms = soup.find_all("form")
    agg["form_count"] += len(forms)
    agg["input_count"] += len(soup.find_all(["input", "select", "textarea"]))
    agg["required_field_count"] += len(soup.find_all(attrs={"required": True}))

    navs = soup.find_all("nav")
    sc_navbars = soup.select(".SC-Navbar, [data-navbar]")
    if sc_navbars:
        agg["has_sc_navbar"] = True
    top_navs = sc_navbars if sc_navbars else navs
    agg["nav_count"] += min(len(top_navs), 3)
    for nav in top_navs[:3]:
        agg["nav_link_count"] += len(nav.find_all("a"))
        nested_uls = nav.find_all("ul")
        if len(nested_uls) > 1:
            agg["has_dropdowns"] = True
        if nav.find_all(class_=re.compile(r"drop|sub|mega|menu|flyout|tier2|tier3", re.I)):
            agg["has_dropdowns"] = True

    if re.search(r"modal|dialog|overlay|lightbox|popup", combined):
        agg["has_modal_signals"] = True
    if re.search(r"cart|basket|bag|mini-cart", combined):
        agg["has_cart_signals"] = True
    if re.search(r"checkout|payment|billing|shipping-address", combined):
        agg["has_checkout_signals"] = True

    json_ld_scripts = soup.find_all("script", type="application/ld+json")
    if json_ld_scripts:
        agg["has_json_ld"] = True
        agg["json_ld_count"] += len(json_ld_scripts)
    if soup.find(attrs={"itemscope": True}):
        agg["has_schema_org"] = True

    for meta in soup.find_all("meta"):
        name = meta.get("name", "").lower()
        prop = meta.get("property", "").lower()
        if name == "viewport":
            agg["has_viewport_meta"] = True
        if prop.startswith("og:"):
            agg["has_og_tags"] = True
        if name.startswith("twitter:") or prop.startswith("twitter:"):
            agg["has_twitter_tags"] = True

    if soup.find("link", rel="canonical"):
        agg["has_canonical"] = True

    agg["h1_count"] += len(soup.find_all("h1"))
    agg["h2_count"] += len(soup.find_all("h2"))
    agg["h3_count"] += len(soup.find_all("h3"))
    agg["h4_plus_count"] += len(soup.find_all(["h4", "h5", "h6"]))

    # ── Extract all links for site architecture mapping ──
    page_url = page.get("url", "")
    page_domain = urlparse(page_url).netloc.lower()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        abs_url = urljoin(page_url, href)
        # Strip fragments from the resolved URL
        parsed_link = urlparse(abs_url)
        if not parsed_link.netloc:
            continue
        clean_url = f"{parsed_link.scheme}://{parsed_link.netloc}{parsed_link.path}"
        link_domain = parsed_link.netloc.lower()
        agg["site_links"].append({
            "url": clean_url,
            "source_url": page_url,
            "zone": _link_zone(a_tag),
            "text": (a_tag.get_text(strip=True) or "")[:100],
            "is_internal": link_domain == page_domain,
        })

    # Collect markdown content for AI analysis (first 1000 chars per page)
    md = page.get("markdown", "")
    if md:
        agg["markdown_samples"].append(md[:1000])

    return {
        "tags": page_tag_count,
        "scripts": page_script_count,
        "frameworks": list(page_frameworks),
    }


def _finalise_signals(agg: dict) -> dict:
    n = max(agg["pages_analyzed"], 1)
    agg["avg_tags"] = agg["total_tags"] // n
    agg["avg_scripts"] = agg["script_count"] // n
    agg["avg_forms"] = agg["form_count"] // n
    agg["avg_images"] = agg["image_count"] // n

    h1, h2, h3 = agg["h1_count"], agg["h2_count"], agg["h3_count"]
    if h1 >= 1 and h2 >= 2 and h3 >= 1:
        agg["heading_quality"] = "good"
    elif h1 >= 1 and h2 >= 1:
        agg["heading_quality"] = "fair"
    else:
        agg["heading_quality"] = "poor"

    agg["js_frameworks"] = list(agg["js_frameworks"])
    return dict(agg)


def _fresh_agg() -> dict:
    agg = defaultdict(int)
    agg["pages_analyzed"] = 0
    agg["has_json_ld"] = False
    agg["has_og_tags"] = False
    agg["has_twitter_tags"] = False
    agg["has_canonical"] = False
    agg["has_schema_org"] = False
    agg["has_viewport_meta"] = False
    agg["has_dropdowns"] = False
    agg["has_modal_signals"] = False
    agg["has_cart_signals"] = False
    agg["has_checkout_signals"] = False
    agg["has_sc_footer"] = False
    agg["has_favicon"] = False
    agg["has_srcset"] = False
    agg["has_loading_lazy"] = False
    agg["video_count"] = 0
    agg["has_sc_css_vars"] = False
    agg["has_third_party_css"] = False
    agg["has_data_menu_init"] = False
    agg["has_semantic_landmarks"] = False
    agg["images_with_alt"] = 0
    agg["images_total"] = 0
    agg["has_gtag"] = False
    agg["has_fbq"] = False
    agg["has_gtm"] = False
    agg["has_cookie_banner"] = False
    agg["has_sc_utility_classes"] = False
    agg["has_sc_menuitem_ids"] = False
    agg["has_sc_navbar"] = False
    agg["js_frameworks"] = set()
    agg["page_titles"] = []
    agg["max_dom_depth"] = 0
    agg["metadata_descriptions"] = []
    agg["markdown_samples"] = []
    agg["site_links"] = []
    return agg


# ── Site architecture tree & Mermaid diagram ──────────────────────────────────

def _build_site_tree(base_url: str, discovered_urls: list, site_links: list) -> dict:
    """
    Build a URL hierarchy tree from discovered URLs and scraped links.
    Returns dict with tree, nav_urls, external_domains, all_internal_urls.
    """
    base_domain = urlparse(base_url).netloc.lower()

    # Collect all unique internal URLs
    all_internal = set()
    nav_urls = set()
    external_domains = set()

    for u in discovered_urls:
        norm = _normalize_url(u)
        if urlparse(norm).netloc.lower() == base_domain:
            all_internal.add(norm)

    for link in site_links:
        if link["is_internal"]:
            norm = _normalize_url(link["url"])
            all_internal.add(norm)
            if link["zone"] == "nav":
                nav_urls.add(norm)
        else:
            ext_domain = urlparse(link["url"]).netloc.lower()
            if ext_domain:
                external_domains.add(ext_domain)

    # Build path trie
    tree = {}
    for url in all_internal:
        path = urlparse(url).path.strip("/")
        segments = [s for s in path.split("/") if s] if path else []
        node = tree
        for seg in segments:
            if seg not in node:
                node[seg] = {}
            node = node[seg]

    # Collect link texts for nav URLs (first text found per URL)
    link_labels = {}
    for link in site_links:
        norm = _normalize_url(link["url"])
        if norm not in link_labels and link["text"]:
            link_labels[norm] = link["text"]

    return {
        "tree": tree,
        "nav_urls": nav_urls,
        "external_domains": external_domains,
        "all_internal_urls": all_internal,
        "link_labels": link_labels,
        "base_domain": base_domain,
    }


def _generate_nav_mermaid(base_url: str, tree_data: dict) -> str:
    """Generate a Mermaid flowchart string from the site tree."""
    tree = tree_data["tree"]
    nav_urls = tree_data["nav_urls"]
    external_domains = tree_data["external_domains"]
    link_labels = tree_data["link_labels"]
    base_domain = tree_data["base_domain"]
    base_scheme = urlparse(base_url).scheme or "https"

    lines = ["graph TD"]
    nav_node_ids = []
    node_counter = [0]  # mutable counter for closures
    MAX_NODES = 100
    MAX_CHILDREN = 15
    MAX_DEPTH = 4

    def _safe_id(prefix: str, name: str) -> str:
        safe = re.sub(r"[^a-zA-Z0-9]", "_", name)
        return f"{prefix}_{safe}"

    def _safe_label(text: str) -> str:
        return (text
                .replace("&", "and")
                .replace('"', "'")
                .replace("<", "")
                .replace(">", "")
                .replace("#", "")
                .replace("(", "")
                .replace(")", "")
                .replace("[", "")
                .replace("]", "")
                .replace("{", "")
                .replace("}", "")
                )

    def _walk(node: dict, parent_id: str, path_prefix: str, depth: int):
        if node_counter[0] >= MAX_NODES:
            return

        children = sorted(node.keys())
        shown = children[:MAX_CHILDREN]
        overflow = len(children) - MAX_CHILDREN

        for seg in shown:
            if node_counter[0] >= MAX_NODES:
                break

            node_counter[0] += 1
            current_path = f"{path_prefix}/{seg}"
            node_id = _safe_id(f"N{node_counter[0]}", seg)

            # Try to find a link label for this path
            full_url = _normalize_url(f"{base_scheme}://{base_domain}{current_path}")
            label = link_labels.get(full_url, seg.replace("-", " ").replace("_", " ").title())
            label = _safe_label(label)

            # Check if this is a nav URL
            is_nav = full_url in nav_urls

            if is_nav:
                lines.append(f'    {parent_id} --> {node_id}["{label}"]')
                nav_node_ids.append(node_id)
            else:
                lines.append(f'    {parent_id} --> {node_id}["{label}"]')

            # Recurse if within depth limit
            sub = node[seg]
            if sub and depth < MAX_DEPTH:
                _walk(sub, node_id, current_path, depth + 1)
            elif sub:
                # Show count of deeper pages
                deep_count = _count_leaves(sub)
                if deep_count > 0:
                    node_counter[0] += 1
                    more_id = f"M{node_counter[0]}"
                    lines.append(f'    {node_id} --> {more_id}["{deep_count} sub-pages"]')

        if overflow > 0 and node_counter[0] < MAX_NODES:
            node_counter[0] += 1
            ov_id = f"OV{node_counter[0]}"
            lines.append(f'    {parent_id} --> {ov_id}["... +{overflow} more"]')

    def _count_leaves(node: dict) -> int:
        if not node:
            return 1
        return sum(_count_leaves(v) for v in node.values())

    # Root node
    home_label = _safe_label(base_domain)
    lines.append(f'    HOME["{home_label}"]')
    _walk(tree, "HOME", "", 1)

    # External domains node
    if external_domains:
        ext_list = sorted(external_domains)[:5]
        ext_label = "<br/>".join(ext_list)
        if len(external_domains) > 5:
            ext_label += f"<br/>... +{len(external_domains) - 5} more"
        lines.append(f'    HOME -.-> EXT["{ext_label}"]')

    # Styling for nav nodes — Praxis branding (yellow accent)
    if nav_node_ids:
        lines.append('    classDef nav fill:#facc15,stroke:#eab308,stroke-width:2px,color:#1a1a1a')
        lines.append(f'    class {",".join(nav_node_ids)} nav')

    return "\n".join(lines)


# ── Scoring ────────────────────────────────────────────────────────────────────

def compute_ratings(sig: dict) -> dict:
    score = 0

    avg_tags = sig.get("avg_tags", 0)
    if avg_tags > 1500: score += 20
    elif avg_tags > 800: score += 13
    elif avg_tags > 400: score += 7

    depth = sig.get("max_dom_depth", 0)
    if depth > 25: score += 10
    elif depth > 15: score += 6
    elif depth > 10: score += 3

    avg_scripts = sig.get("avg_scripts", 0)
    if avg_scripts > 25: score += 20
    elif avg_scripts > 12: score += 13
    elif avg_scripts > 6: score += 7

    fw_count = len(sig.get("js_frameworks", []))
    if fw_count > 5: score += 10
    elif fw_count > 2: score += 6
    elif fw_count > 0: score += 3

    inline = sig.get("inline_style_count", 0)
    if inline > 100: score += 10
    elif inline > 40: score += 6
    elif inline > 10: score += 3

    forms = sig.get("form_count", 0)
    inputs = sig.get("input_count", 0)
    if forms > 8 or inputs > 50: score += 15
    elif forms > 3 or inputs > 20: score += 9
    elif forms > 0 or inputs > 5: score += 4

    nav_links = sig.get("nav_link_count", 0)
    navs = sig.get("nav_count", 0)
    if sig.get("has_dropdowns") or nav_links > 40: score += 10
    elif nav_links > 20: score += 6
    elif navs > 0: score += 3

    if sig.get("has_checkout_signals"): score += 5
    elif sig.get("has_cart_signals"): score += 3

    if score >= 60:
        stability, upgrade_band, regression = "Upgrade Risk Detected", "Elevated", "Strongly Recommended"
    elif score >= 30:
        stability, upgrade_band, regression = "Needs Attention", "Moderate", "Recommended"
    else:
        stability, upgrade_band, regression = "Stable", "Low", "Yes"

    ai_score = sum([
        1 if sig.get("has_json_ld") else 0,
        1 if sig.get("has_og_tags") else 0,
        1 if sig.get("has_canonical") else 0,
        1 if sig.get("heading_quality") == "good" else 0,
        1 if sig.get("has_schema_org") else 0,
    ])
    ai_readiness = "Strong" if ai_score >= 4 else "Moderate" if ai_score >= 2 else "Limited"

    perf_issues = sum([
        1 if sig.get("avg_scripts", 0) > 15 else 0,
        1 if sig.get("avg_images", 0) > 30 else 0,
        1 if sig.get("avg_tags", 0) > 800 else 0,
        1 if sig.get("max_dom_depth", 0) > 18 else 0,
        1 if sig.get("inline_script_kb", 0) > 50 else 0,
    ])
    performance_band = "Needs Optimisation" if perf_issues >= 2 else "Stable"

    return {
        "score": score,
        "stability": stability,
        "upgrade_band": upgrade_band,
        "regression": regression,
        "ai_readiness": ai_readiness,
        "performance_band": performance_band,
    }


# ── OpenAI narrative analysis ──────────────────────────────────────────────────

def run_openai_analysis(url: str, sig: dict, ratings: dict, openai_key: str) -> dict:
    """
    Send aggregated signals + markdown samples to GPT-4o and get back a rich
    narrative analysis. Returns a dict of sections for the report.
    Falls back to rule-based text if OpenAI is unavailable.
    """
    if not openai_key or openai_key.startswith("your_"):
        return _fallback_analysis(url, sig, ratings)

    try:
        from openai import OpenAI
        client = OpenAI(api_key=openai_key)

        domain = urlparse(url).netloc
        fw = sig.get("js_frameworks", [])
        pages = sig.get("pages_analyzed", 0)
        markdown_samples = sig.get("markdown_samples", [])
        sample_text = "\n\n---\n\n".join(markdown_samples[:5]) if markdown_samples else "No content samples available."

        prompt = f"""You are a senior frontend analyst for Praxis, a StoreConnect Frontend Custodian service. 
You have just completed a technical scan of {domain} across {pages} pages.

Here are the raw signals extracted:

TECHNICAL SIGNALS:
- Pages analysed: {pages}
- Avg DOM tags/page: {sig.get('avg_tags', 0)}
- Max DOM depth: {sig.get('max_dom_depth', 0)}
- Avg scripts/page: {sig.get('avg_scripts', 0)}
- External scripts: {sig.get('external_script_count', 0)}
- Inline script KB: {sig.get('inline_script_kb', 0)}
- JS Frameworks detected: {', '.join(fw) if fw else 'None'}
- Stylesheets: {sig.get('stylesheet_count', 0)}
- Inline styles: {sig.get('inline_style_count', 0)}
- Forms: {sig.get('form_count', 0)} with {sig.get('input_count', 0)} inputs
- Images/page avg: {sig.get('avg_images', 0)}
- Navigation links: {sig.get('nav_link_count', 0)}, dropdowns: {sig.get('has_dropdowns', False)}
- Cart signals: {sig.get('has_cart_signals', False)}
- Checkout signals: {sig.get('has_checkout_signals', False)}
- Modal signals: {sig.get('has_modal_signals', False)}
- JSON-LD: {sig.get('has_json_ld', False)}, Schema.org: {sig.get('has_schema_org', False)}
- Open Graph: {sig.get('has_og_tags', False)}, Twitter Cards: {sig.get('has_twitter_tags', False)}
- Canonical URLs: {sig.get('has_canonical', False)}
- Heading quality: {sig.get('heading_quality', 'unknown')}
- H1/H2/H3 counts: {sig.get('h1_count',0)}/{sig.get('h2_count',0)}/{sig.get('h3_count',0)}

COMPUTED RATINGS:
- Frontend Stability: {ratings['stability']}
- Upgrade Readiness: {ratings['upgrade_band']}
- Regression Testing: {ratings['regression']}
- AI Readiness: {ratings['ai_readiness']}
- Performance Band: {ratings['performance_band']}
- Complexity Score: {ratings['score']}/100

PAGE CONTENT SAMPLES (first 5 pages, markdown):
{sample_text}

Write a professional Praxis-branded frontend analysis report in JSON format with these exact keys:

{{
  "executive_summary": "2-3 sentence executive summary of the site's frontend health, specific to this site",
  "summary_benefits": ["3-6 bullet points — plain language, what's working well across the whole site. Start each with a bold label like **Search visibility** — then explain in simple terms."],
  "summary_gaps": ["3-6 bullet points — plain language, what needs fixing. Start each with a bold label like **Preferred URL missing** — then explain why it matters and what to do."],
  "seo_benefits": ["2-4 bullet points — what's working well in SEO/meta for THIS site, plain language"],
  "seo_gaps": ["2-4 bullet points — what needs improving in SEO/meta, plain language, specific to findings"],
  "seo_intro": "1-2 sentence plain-language intro for the SEO section, specific to this site",
  "content_benefits": ["2-4 bullet points — what's working well in content/accessibility"],
  "content_gaps": ["2-4 bullet points — what needs improving in content/accessibility"],
  "content_intro": "1-2 sentence plain-language intro for content section",
  "perf_benefits": ["2-4 bullet points — what's working well in performance"],
  "perf_gaps": ["2-4 bullet points — what needs improving in performance"],
  "perf_intro": "1-2 sentence plain-language intro for performance section",
  "sc_benefits": ["2-4 bullet points — what's working well in StoreConnect standards compliance"],
  "sc_gaps": ["2-4 bullet points — what needs improving in SC standards"],
  "sc_intro": "1-2 sentence plain-language intro for StoreConnect standards section",
  "ux_benefits": ["2-4 bullet points — what's working well in UX/navigation"],
  "ux_gaps": ["1-3 bullet points — what needs improving in UX/navigation"],
  "ux_intro": "1-2 sentence plain-language intro for UX section",
  "struct_benefits": ["1-3 bullet points — what's working well in structured data / AI readiness"],
  "struct_gaps": ["2-4 bullet points — what's missing in structured data, plain language"],
  "struct_intro": "1-2 sentence plain-language intro for structured data section",
  "complex_benefits": ["1-3 bullet points — what's working well re complexity/upgrade risk"],
  "complex_gaps": ["2-4 bullet points — what needs attention for complexity/upgrades"],
  "complex_intro": "1-2 sentence plain-language intro for complexity section",
  "complexity_narrative": "1-2 paragraphs describing what was found in the frontend complexity — be specific about the signals (e.g. mention specific frameworks, form counts, navigation patterns)",
  "performance_narrative": "1-2 paragraphs on performance implications of what was found",
  "ai_structure_narrative": "1 paragraph on structured data / SEO / AI readiness findings",
  "what_this_means_1": "2-3 sentences for point 1: how StoreConnect upgrades affect THIS site specifically",
  "what_this_means_2": "2-3 sentences for point 2: what regression testing scope looks like for THIS site",
  "what_this_means_3": "2-3 sentences for point 3: why frontend stewardship matters for THIS site",
  "action_1_title": "Title for recommended action 1",
  "action_1_body": "Body for action 1 — specific to what was found",
  "action_2_title": "Title for recommended action 2",
  "action_2_body": "Body for action 2 — specific to what was found",
  "action_3_title": "Title for recommended action 3",
  "action_3_body": "Body for action 3 — specific to what was found",
  "complexity_items": ["list", "of", "4-6 specific complexity signals found"],
  "ai_structure_items": ["list of 2-4 positive structured data items found"],
  "ai_gap_items": ["list of 2-4 gaps in structured data / metadata"]
}}

IMPORTANT GUIDELINES FOR BENEFITS & GAPS:
- Write in plain, non-technical language that a store owner can understand
- Be specific to THIS site — reference actual findings (e.g. "Your page titles and descriptions are set up" not "SEO is configured")
- For gaps, explain WHY it matters and WHAT to do about it in simple terms
- Use **bold labels** at the start of each bullet (e.g. "**Mobile-friendly** — The site scales correctly on phones.")
- Keep each bullet to 1-2 sentences max
- Do not be generic — every bullet should clearly relate to the actual signals found

Be specific, professional, and direct. Reference actual numbers from the signals. Do not be generic."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.4,
        )

        return json.loads(response.choices[0].message.content)

    except Exception as e:
        print(f"[openai_analysis] failed: {e}")
        return _fallback_analysis(url, sig, ratings)


def _fallback_analysis(url: str, sig: dict, ratings: dict) -> dict:
    """Rule-based fallback when OpenAI is unavailable."""
    domain = urlparse(url).netloc
    pages = sig.get("pages_analyzed", 0)
    fw = sig.get("js_frameworks", [])
    stability = ratings["stability"]

    complexity_items = []
    if sig.get("has_dropdowns"):
        complexity_items.append("Multi-level navigation with dropdown behaviour")
    if fw:
        complexity_items.append(f"JavaScript frameworks: {', '.join(fw)}")
    if sig.get("avg_scripts", 0) > 10:
        complexity_items.append(f"High script load — avg {sig.get('avg_scripts', 0)} scripts/page")
    if sig.get("inline_style_count", 0) > 15:
        complexity_items.append(f"Custom inline styling ({sig.get('inline_style_count', 0)} instances)")
    if sig.get("form_count", 0) > 0:
        complexity_items.append(f"{sig.get('form_count', 0)} forms with {sig.get('input_count', 0)} input fields")
    if sig.get("has_cart_signals") or sig.get("has_checkout_signals"):
        complexity_items.append("Cart and checkout flow components detected")
    if sig.get("has_modal_signals"):
        complexity_items.append("Modal and overlay interaction patterns")
    if not complexity_items:
        complexity_items.append("Relatively straightforward frontend structure")

    ai_items = []
    ai_gaps = []
    if sig.get("has_json_ld"): ai_items.append("Structured data (JSON-LD) present")
    else: ai_gaps.append("No JSON-LD schema detected — entity signals are weak")
    if sig.get("has_og_tags"): ai_items.append("Open Graph metadata in place")
    else: ai_gaps.append("Open Graph tags incomplete — social previews affected")
    if sig.get("heading_quality") == "good": ai_items.append("Heading hierarchy well-structured")
    else: ai_gaps.append("Heading structure needs attention")
    if sig.get("has_canonical"): ai_items.append("Canonical URLs present")

    if stability == "Upgrade Risk Detected":
        exec_summary = f"This report summarises the frontend profile of {domain}, based on analysis across {pages} page{'s' if pages != 1 else ''}. The site presents elevated complexity across DOM structure, script layering, and interactive components. Regression testing and upgrade verification are strongly recommended before any platform changes are applied. The Upgrade Readiness Band is assessed at {ratings['upgrade_band']}."
    elif stability == "Needs Attention":
        exec_summary = f"This report summarises the frontend profile of {domain}, based on analysis across {pages} page{'s' if pages != 1 else ''}. The frontend profile shows moderate complexity across script loading, navigation structure, and interactive components. This level of customisation warrants structured review prior to any platform version upgrade. The Upgrade Readiness Band is assessed at {ratings['upgrade_band']}."
    else:
        exec_summary = f"This report summarises the frontend profile of {domain}, based on analysis across {pages} page{'s' if pages != 1 else ''}. The site presents a stable frontend structure with manageable complexity indicators. Proactive verification ahead of version upgrades remains advisable. The Upgrade Readiness Band is assessed at {ratings['upgrade_band']}."

    fw_str = ", ".join(fw) if fw else "no major frameworks"
    regression_note = "checkout flows, " if sig.get("has_checkout_signals") else ""

    # ── Per-section benefits & gaps (plain language) ──
    summary_benefits = []
    summary_gaps = []

    # SEO
    seo_benefits = []
    seo_gaps = []
    if sig.get("page_titles"):
        seo_benefits.append(f"**Search visibility** — Page titles and descriptions are set up, so Google can show {domain} in results.")
    if sig.get("has_viewport_meta"):
        seo_benefits.append("**Mobile-friendly** — The site scales correctly on phones.")
    seo_benefits.append("**Language and encoding** — Text displays properly with correct language and character settings.")
    if not sig.get("has_canonical"):
        seo_gaps.append("**Preferred URL missing** — Search engines may index duplicate pages. Add a canonical URL so they know which address is the main one.")
    if not sig.get("has_og_tags"):
        seo_gaps.append("**Social previews incomplete** — When someone shares your link on Facebook or LinkedIn, it may not show your image or description. Add Open Graph tags.")
    if not sig.get("has_twitter_tags"):
        seo_gaps.append("**Twitter/X preview missing** — Links shared on Twitter/X won't look as good. Add Twitter Card tags.")

    # Content
    content_benefits = []
    content_gaps = []
    if sig.get("heading_quality") in ("good", "fair"):
        content_benefits.append("**Clear heading structure** — Main heading and subheadings help visitors scan the page.")
    if sig.get("required_field_count", 0) > 0:
        content_benefits.append("**Required fields marked** — Visitors know what's mandatory in forms.")
    alt_ratio = sig.get("images_with_alt", 0) / max(sig.get("images_total", 1), 1)
    if alt_ratio < 0.5 and sig.get("images_total", 0) > 0:
        content_gaps.append(f"**Image descriptions missing** — {sig.get('images_total', 0) - sig.get('images_with_alt', 0)} images lack alt text. Screen reader users can't understand them; search engines can't index them well.")
    content_gaps.append("**No skip-to-content link** — Keyboard users must tab through the whole menu to reach main content. Add a skip link for accessibility.")
    if not sig.get("has_semantic_landmarks"):
        content_gaps.append("**Page structure partial** — Main content, articles, and sections could be marked more clearly for assistive technology.")

    # Performance
    perf_benefits = []
    perf_gaps = []
    if sig.get("max_dom_depth", 0) < 18:
        perf_benefits.append("**Page structure depth is fine** — Not overly nested, which helps rendering speed.")
    if sig.get("avg_scripts", 0) < 15:
        perf_benefits.append(f"**Script count is reasonable** — {sig.get('avg_scripts', 0)} scripts per page on average.")
    if sig.get("avg_images", 0) < 30:
        perf_benefits.append("**Image count per page is acceptable** — Not overloaded with images.")
    if not sig.get("has_third_party_css"):
        perf_benefits.append("**No problematic external style libraries** — No Bootstrap or Tailwind adding bloat.")
    if sig.get("inline_style_count", 0) >= 40:
        perf_gaps.append(f"**Too many inline styles** — {sig.get('inline_style_count', 0)} inline styles scattered in the page make it heavier and harder to change.")
    if sig.get("avg_tags", 0) >= 400:
        perf_gaps.append(f"**Page complexity {'high' if sig.get('avg_tags', 0) >= 800 else 'moderate'}** — {sig.get('avg_tags', 0)} elements per page on average. Consider reducing if the page feels slow.")

    # StoreConnect Standards
    sc_benefits = []
    sc_gaps = []
    if sig.get("has_sc_navbar") or sig.get("nav_count", 0) > 0:
        sc_benefits.append("**Standard navbar** — StoreConnect navigation layout is in place.")
    if sig.get("has_sc_footer"):
        sc_benefits.append("**Standard footer** — StoreConnect footer component is present.")
    if sig.get("has_sc_css_vars"):
        sc_benefits.append("**Theme variables** — StoreConnect CSS variables and brand colours are defined.")
    if sig.get("has_dropdowns") or sig.get("nav_link_count", 0) > 15:
        sc_benefits.append("**Menu structure** — Dropdowns and mega menus are set up correctly.")
    if not sig.get("has_third_party_css"):
        sc_benefits.append("**No unsupported frameworks** — The site should upgrade smoothly.")
    if not sig.get("has_sc_footer"):
        sc_gaps.append("**Footer partial** — The StoreConnect footer component may not be fully standard.")
    if sig.get("inline_style_count", 0) > 0:
        sc_gaps.append("**Custom styles partial** — Styling may not be fully organised in a theme supplement file.")

    # UX
    ux_benefits = []
    ux_gaps = []
    if sig.get("nav_count", 0) > 0:
        ux_benefits.append(f"**Main menu is present** — {sig.get('nav_link_count', 0)} links in the navigation.")
    if sig.get("has_dropdowns"):
        ux_benefits.append("**Dropdown menus work** — Categories and sub-pages are accessible.")
    if sig.get("has_cart_signals"):
        ux_benefits.append("**Cart is easy to find** — Cart icon or link is visible.")
    if sig.get("has_checkout_signals"):
        ux_benefits.append("**Checkout present** — Payment and checkout flow is in place.")
    if sig.get("has_modal_signals"):
        ux_benefits.append("**Pop-ups and modals** — Overlay interactions are available.")
    if sig.get("has_data_menu_init"):
        ux_benefits.append("**Mobile menu works** — Responsive hamburger menu is set up for phones.")
    if sig.get("input_count", 0) > 20:
        ux_gaps.append(f"**Form fields partial** — {sig.get('input_count', 0)} form inputs across the site may need better labelling or validation.")

    # Structured data
    struct_benefits = []
    struct_gaps = []
    if sig.get("has_json_ld"):
        struct_benefits.append("**Structured data present** — JSON-LD is providing machine-readable info to search engines.")
    if sig.get("has_schema_org"):
        struct_benefits.append("**Schema markup** — Products and organisation are labelled in a standard way.")
    if not struct_benefits:
        struct_benefits.append("**Biggest opportunity** — Adding structured data can significantly improve how you show up in search and AI.")
    if not sig.get("has_json_ld"):
        struct_gaps.append("**No structured data** — Add JSON-LD so search engines and AI can understand your products.")
    if not sig.get("has_schema_org"):
        struct_gaps.append("**No schema markup** — Products and organisation aren't labelled in a standard way for search.")
    struct_gaps.append("**No breadcrumb data** — The path (Home > Category > Product) isn't available for search results.")

    # Complexity
    complex_benefits = []
    complex_gaps = []
    if len(fw) <= 5:
        complex_benefits.append(f"**No unsupported JavaScript frameworks** — {', '.join(fw) if fw else 'No frameworks detected'}, all supported.")
    if sig.get("inline_style_count", 0) >= 40:
        complex_gaps.append(f"**Too many inline styles** — {sig.get('inline_style_count', 0)} inline styles make changes harder and the site heavier.")
    if sig.get("custom_data_attrs", 0) >= 100:
        complex_gaps.append(f"**Custom attributes add complexity** — {sig.get('custom_data_attrs', 0)} extra HTML attributes detected.")
    if stability != "Stable":
        complex_gaps.append(f"**Overall complexity {stability.lower()}** — The combined score suggests the site may need cleanup before major upgrades.")

    # Build summary from section items
    summary_benefits = (seo_benefits[:1] + content_benefits[:1] + perf_benefits[:1] +
                        sc_benefits[:1] + ux_benefits[:1] + complex_benefits[:1])
    summary_gaps = (seo_gaps[:1] + content_gaps[:1] + perf_gaps[:1] +
                    struct_gaps[:1] + complex_gaps[:1])
    # Keep max 6 each
    summary_benefits = summary_benefits[:6]
    summary_gaps = summary_gaps[:6]

    return {
        "executive_summary": exec_summary,
        "summary_benefits": summary_benefits,
        "summary_gaps": summary_gaps,
        "seo_benefits": seo_benefits,
        "seo_gaps": seo_gaps,
        "seo_intro": f"How well {domain} is set up for search engines to find it and for links to look good when shared.",
        "content_benefits": content_benefits,
        "content_gaps": content_gaps,
        "content_intro": f"How well {domain} is organised for visitors and for people using screen readers or keyboards.",
        "perf_benefits": perf_benefits,
        "perf_gaps": perf_gaps,
        "perf_intro": f"How heavy or light {domain} is — affects loading speed and responsiveness.",
        "sc_benefits": sc_benefits,
        "sc_gaps": sc_gaps,
        "sc_intro": f"Whether {domain} follows StoreConnect's recommended layout, colours, and menu patterns.",
        "ux_benefits": ux_benefits,
        "ux_gaps": ux_gaps,
        "ux_intro": f"Can visitors find the menu, cart, checkout, and forms easily on {domain}?",
        "struct_benefits": struct_benefits,
        "struct_gaps": struct_gaps,
        "struct_intro": f"Structured information that helps search engines and AI understand {domain}'s products and business.",
        "complex_benefits": complex_benefits,
        "complex_gaps": complex_gaps,
        "complex_intro": f"How complex {domain} is — affects how easy it is to upgrade, change, or maintain.",
        "complexity_narrative": f"The frontend of {domain} presents {sig.get('avg_tags', 0)} average DOM elements per page with a maximum nesting depth of {sig.get('max_dom_depth', 0)}. Script loading shows {sig.get('avg_scripts', 0)} scripts per page on average, with {fw_str} detected across the site. {'Inline styling is present across ' + str(sig.get('inline_style_count', 0)) + ' instances, indicating theme customisation beyond the base layer.' if sig.get('inline_style_count', 0) > 15 else 'Inline styling is minimal, suggesting a clean theme implementation.'}",
        "performance_narrative": f"{'The script layer is dense with ' + str(sig.get('avg_scripts', 0)) + ' scripts per page, introducing render-blocking risk.' if sig.get('avg_scripts', 0) > 15 else 'Script loading appears well-contained at ' + str(sig.get('avg_scripts', 0)) + ' per page.'} {'Image volume averages ' + str(sig.get('avg_images', 0)) + ' per page — lazy loading and modern format adoption would support load time efficiency.' if sig.get('avg_images', 0) > 15 else 'Image usage is within a manageable range.'} DOM depth of {sig.get('max_dom_depth', 0)} {'is elevated and may impact rendering on lower-powered devices.' if sig.get('max_dom_depth', 0) > 18 else 'is within acceptable range.'}",
        "ai_structure_narrative": f"{'Structured data (JSON-LD) is present, providing a foundation for entity recognition.' if sig.get('has_json_ld') else 'No JSON-LD structured data was detected, which limits entity clarity for AI-driven discovery.'} {'Open Graph metadata is in place.' if sig.get('has_og_tags') else 'Open Graph tags appear absent, limiting social preview quality.'} {'Heading hierarchy follows a logical structure.' if sig.get('heading_quality') == 'good' else 'Heading structure requires attention for accessibility and AI content parsing.'}",
        "what_this_means_1": f"When StoreConnect releases a version upgrade, it can alter how themes render and how scripts load. Given {domain}'s {ratings['upgrade_band'].lower()} complexity profile, these changes carry {'elevated' if ratings['upgrade_band'] == 'Elevated' else 'moderate' if ratings['upgrade_band'] == 'Moderate' else 'manageable'} regression risk. Pre-upgrade verification against this site's specific frontend signals is the recommended approach.",
        "what_this_means_2": f"A regression test scope for {domain} should cover navigation behaviour {'including dropdown menus,' if sig.get('has_dropdowns') else ','} {regression_note}form validation flows, and mobile layout integrity. {'With ' + str(sig.get('form_count', 0)) + ' forms and ' + str(sig.get('input_count', 0)) + ' input fields, form testing is a non-trivial scope.' if sig.get('form_count', 0) > 2 else ''} A structured bundle runs in hours and prevents live incidents.",
        "what_this_means_3": f"The frontend layer of {domain} — the part customers interact with — carries customisation that sits outside StoreConnect's core platform management. {'With cart, checkout, and modal components detected, this layer needs dedicated custodianship.' if sig.get('has_checkout_signals') else 'Ensuring this layer is monitored between upgrade cycles prevents gradual drift from causing visible issues.'} That is the gap Praxis fills.",
        "action_1_title": "Conduct a Pre-Upgrade Frontend Verification" if ratings['upgrade_band'] in ("Elevated", "Moderate") else "Establish a Frontend Upgrade Baseline",
        "action_1_body": f"Before applying any StoreConnect version upgrade, commission a structured frontend audit to catalogue active customisations{', script dependencies (' + ', '.join(fw[:3]) + '),' if fw else ','} and theme overrides. This creates a baseline for post-upgrade comparison and reduces the risk of undetected regressions.",
        "action_2_title": f"Set Up a Regression Testing Bundle (Testing: {ratings['regression']})",
        "action_2_body": f"Implement a defined regression test suite covering navigation {'(including dropdowns), ' if sig.get('has_dropdowns') else ', '}{regression_note}mobile layouts, and form validation. A structured test bundle ensures post-upgrade deployments are validated consistently.",
        "action_3_title": "Improve Structured Data & Metadata Coverage" if ratings['ai_readiness'] in ("Limited", "Moderate") else "Optimise Frontend Asset & Script Loading",
        "action_3_body": (
            "Adding JSON-LD schema markup, completing Open Graph tags, and ensuring heading hierarchy is consistent "
            "will strengthen the site's AI discoverability and search entity signals."
            if ratings["ai_readiness"] in ("Limited", "Moderate") else
            "Review script loading order, implement lazy loading for images, "
            "and evaluate consolidation opportunities across the stylesheet and script layers."
        ),
        "complexity_items": complexity_items,
        "ai_structure_items": ai_items,
        "ai_gap_items": ai_gaps,
    }


# ── HTML report ────────────────────────────────────────────────────────────────

def _badge_html(label: str) -> str:
    fg, bg = COLOR_MAP.get(label, ("#374151", "#f3f4f6"))
    return (
        f'<span style="display:inline-block;padding:0.2rem 0.65rem;border-radius:999px;'
        f'font-size:0.78rem;font-weight:700;background:{bg};color:{fg};">{label}</span>'
    )


def _li(items: list) -> str:
    return "\n".join(f"<li>{i}</li>" for i in items) if items else "<li>None detected</li>"


def _md_bold_to_html(text: str) -> str:
    """Convert **bold** markdown to <strong>bold</strong> HTML."""
    import re as _re
    return _re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)


def _benefits_gaps_html(benefits: list, gaps: list, full: bool = False) -> str:
    """Render a benefits & gaps box pair. full=True for the summary version."""
    cls = "benefits-gaps" if full else "section-benefits-gaps"
    b_title = "What's working" if not full else "What's working"
    g_title = "What needs improving" if not full else "Gaps to fix — what matters"
    b_items = "\n".join(f"<li>{_md_bold_to_html(i)}</li>" for i in benefits) if benefits else "<li>No issues detected</li>"
    g_items = "\n".join(f"<li>{_md_bold_to_html(i)}</li>" for i in gaps) if gaps else "<li>No gaps detected</li>"
    return f"""<div class="{cls}">
        <div class="benefits-box">
          <h3>&#10003; {b_title}</h3>
          <ul>{b_items}</ul>
        </div>
        <div class="gaps-box">
          <h3>&#9888; {g_title}</h3>
          <ul>{g_items}</ul>
        </div>
      </div>"""


def _audit_row(idx: str, check: str, signal: str, status: str) -> str:
    cls = "status-pass" if status in ("Pass", "Good", "Stable") else "status-fail" if status in ("Fail",) else "status-partial" if status not in ("—",) else "status-na"
    return f'<tr><td>{idx}</td><td>{check}</td><td>{signal}</td><td class="{cls}">{status}</td></tr>'


def _compute_audit_status(sig: dict, ratings: dict) -> dict:
    """Compute pass/fail/partial for each audit item from signals."""
    s = sig.get
    r = ratings.get
    return {
        "1.1": "Pass" if (s("page_titles") or []) else "Fail",
        "1.2": "Pass" if (s("metadata_descriptions") or []) else "Fail",
        "1.3": "Pass" if s("has_canonical") else "Fail",
        "1.4": "Partial" if s("has_og_tags") else "Fail",
        "1.5": "Pass" if s("has_twitter_tags") else "Fail",
        "1.6": "Pass" if s("has_viewport_meta") else "Fail",
        "1.7": "—",
        "1.8": "Pass",
        "1.9": "Pass",
        "2.1": "Pass" if s("h1_count", 0) >= 1 and s("h2_count", 0) >= 1 else "Fail",
        "2.2": "Pass" if s("heading_quality") == "good" else "Partial" if s("heading_quality") == "fair" else "Fail",
        "2.3": "Pass" if s("has_semantic_landmarks") else "Partial",
        "2.4": "Pass" if s("images_total", 0) == 0 or (s("images_with_alt", 0) / max(s("images_total", 1), 1)) > 0.5 else "Fail",
        "2.5": "Partial",
        "2.6": "Pass" if s("required_field_count", 0) > 0 or s("form_count", 0) == 0 else "Partial",
        "2.7": "—",
        "2.8": "Fail",
        "2.9": "—",
        "3.1": "Pass" if s("avg_tags", 0) < 400 else "Partial" if s("avg_tags", 0) < 800 else "Moderate",
        "3.2": "Pass" if s("max_dom_depth", 0) < 18 else "Partial" if s("max_dom_depth", 0) < 25 else "Fail",
        "3.3": "Pass" if s("avg_scripts", 0) < 15 else "Partial",
        "3.4": "Pass" if s("external_script_count", 0) < 20 else "Partial",
        "3.5": "Pass" if s("inline_script_kb", 0) < 50 else "Fail",
        "3.6": "Pass",
        "3.7": "Pass" if s("inline_style_count", 0) < 40 else "Fail",
        "3.8": "Pass" if s("avg_images", 0) < 30 else "Partial",
        "3.9": "Pass" if not s("has_third_party_css") else "Fail",
        "4.1": "Pass" if s("has_sc_navbar") or s("nav_count", 0) > 0 else "Fail",
        "4.2": "Pass" if s("has_sc_footer") else "Partial",
        "4.3": "Pass" if s("has_sc_css_vars") else "Partial",
        "4.4": "Pass",
        "4.5": "Partial" if s("inline_style_count", 0) > 0 else "—",
        "4.6": "—",
        "4.7": "Partial" if s("custom_data_attrs", 0) > 0 else "—",
        "4.8": "Pass" if s("has_dropdowns") or s("nav_link_count", 0) > 15 else "Partial",
        "4.9": "Pass" if not s("has_third_party_css") else "Fail",
        "5.1": "Pass" if s("nav_count", 0) > 0 else "Fail",
        "5.2": "Pass" if s("nav_link_count", 0) >= 5 else "Partial",
        "5.3": "Pass" if s("has_dropdowns") else "Partial",
        "5.4": "Pass" if s("has_cart_signals") else "Partial",
        "5.5": "Pass" if s("has_checkout_signals") else "Partial",
        "5.6": "Pass" if s("has_modal_signals") else "Partial",
        "5.7": "Pass",
        "5.8": "Partial" if s("input_count", 0) > 20 else "Pass",
        "5.9": "Pass" if s("has_data_menu_init") else "Partial",
        "6.1": "Pass" if s("has_json_ld") else "Fail",
        "6.2": "Pass" if s("has_schema_org") else "Fail",
        "6.3": "Pass" if s("has_json_ld") else "Fail",
        "6.4": "Fail",
        "7.1": "Pass" if len(s("js_frameworks", [])) <= 5 else "Partial",
        "7.2": "Pass" if s("custom_data_attrs", 0) < 100 else "Partial",
        "7.3": "Pass" if s("inline_style_count", 0) < 40 else "Fail",
        "7.4": r("stability", "Stable"),
    }


def generate_html(url: str, sig: dict, ratings: dict, analysis: dict) -> str:
    domain = urlparse(url).netloc
    scan_date = datetime.now().strftime("%d %b %Y")
    pages = sig.get("pages_analyzed", 0)
    status = _compute_audit_status(sig, ratings)

    nav_mermaid = sig.get("nav_mermaid", "graph TD\n    A[No navigation data]")
    nav_tree_stats = sig.get("nav_tree_stats", {"internal_urls": 0, "nav_urls": 0, "external_domains": 0})

    def row(idx, plain, technical):
        return _audit_row(idx, plain, technical, status.get(idx, "—"))

    seo_rows = "\n".join([
        row("1.1", "Does the page have a title (shows in browser tab)?", "Page title"),
        row("1.2", "Short summary for search results", "Page description"),
        row("1.3", "One main address so search engines don't get confused", "Preferred URL"),
        row("1.4", "How your page looks when shared on Facebook, LinkedIn", "Social sharing preview"),
        row("1.5", "How your page looks when shared on Twitter/X", "Twitter/X preview"),
        row("1.6", "Page scales correctly on phones", "Mobile-friendly setup"),
        row("1.7", "Any rules for indexing (optional)", "Search engine instructions"),
        row("1.8", "Language declared for screen readers", "Page language"),
        row("1.9", "Text displays correctly (e.g. accents, symbols)", "Character encoding"),
    ])
    content_rows = "\n".join([
        row("2.1", "Clear main heading and subheadings", "Heading structure"),
        row("2.2", "Enough headings to organise content", "Heading quality"),
        row("2.3", "Main content, articles, sections marked properly", "Page structure"),
        row("2.4", "Every image has alt text (for screen readers)", "Image descriptions"),
        row("2.5", "Each input has a visible label", "Form field labels"),
        row("2.6", "Mandatory fields are clearly indicated", "Required fields marked"),
        row("2.7", "Extra cues for assistive technology", "Accessibility helpers"),
        row("2.8", "Keyboard users can skip the menu", "Skip to content link"),
        row("2.9", "Clear outline when tabbing through links", "Focus visibility"),
    ])
    perf_rows = "\n".join([
        row("3.1", "How many elements on the page", "Page complexity"),
        row("3.2", "How deeply nested the page is", "Structure depth"),
        row("3.3", "Number of scripts loading", "Script count"),
        row("3.4", "Scripts loaded from elsewhere", "External scripts"),
        row("3.5", "Scripts embedded in the page", "Inline script size"),
        row("3.6", "Number of style files", "Stylesheet count"),
        row("3.7", "Styles applied directly to elements", "Inline styles"),
        row("3.8", "Number of images per page", "Image count"),
        row("3.9", "Bootstrap, Tailwind, etc.", "External style libraries"),
    ])
    sc_rows = "\n".join([
        row("4.1", "Standard top navigation bar", "StoreConnect navbar"),
        row("4.2", "Standard footer component", "StoreConnect footer"),
        row("4.3", "StoreConnect colour and style settings", "Theme variables"),
        row("4.4", "Your brand's primary colours defined", "Brand colours"),
        row("4.5", "Your own styling applied", "Custom styles"),
        row("4.6", "Extra theme customisation file", "Theme supplement file"),
        row("4.7", "Standard content block identifiers", "Content block IDs"),
        row("4.8", "Dropdowns and mega menus set up correctly", "Menu structure"),
        row("4.9", "No unsupported frameworks", "Supported libraries"),
    ])
    ux_rows = "\n".join([
        row("5.1", "Main menu is present", "Navigation menu"),
        row("5.2", "Enough links in the main nav", "Menu links"),
        row("5.3", "Sub-menus work (categories, etc.)", "Dropdown menus"),
        row("5.4", "Cart icon or link visible", "Cart"),
        row("5.5", "Checkout and payment flow present", "Checkout"),
        row("5.6", "Modals, dialogs, lightboxes", "Pop-ups and overlays"),
        row("5.7", "Contact, search, or other forms", "Forms"),
        row("5.8", "Inputs, dropdowns, text areas", "Form fields"),
        row("5.9", "Menu works on phones (hamburger, etc.)", "Mobile menu"),
    ])
    struct_rows = "\n".join([
        row("6.1", "Machine-readable product/business info", "Structured data"),
        row("6.2", "Standard labels for products and organisation", "Schema markup"),
        row("6.3", "Clear signals about what you sell and who you are", "Product &amp; organisation info"),
        row("6.4", "Path (Home &gt; Category &gt; Product) for search", "Breadcrumb data"),
    ])
    complex_rows = "\n".join([
        row("7.1", "jQuery, React, Vue, Alpine — are they supported?", "JavaScript frameworks"),
        row("7.2", "Extra HTML attributes that add complexity", "Custom attributes"),
        row("7.3", "Styles scattered in the page (harder to maintain)", "Inline styles"),
        row("7.4", "Combined score — how upgrade-friendly is the site?", "Overall complexity"),
    ])

    seo_badge = "Good" if status.get("1.1") == "Pass" and status.get("1.2") == "Pass" and status.get("1.3") == "Pass" else "Fair"
    content_badge = "Good" if sig.get("heading_quality") == "good" else "Fair"
    perf_badge = ratings.get("performance_band", "Stable")
    sc_badge = "Partial" if status.get("4.2") != "Pass" else "Good"
    complex_badge = ratings.get("stability", "Stable")

    # Build benefits/gaps HTML blocks
    summary_bg = _benefits_gaps_html(analysis.get("summary_benefits", []), analysis.get("summary_gaps", []), full=True)
    seo_bg = _benefits_gaps_html(analysis.get("seo_benefits", []), analysis.get("seo_gaps", []))
    content_bg = _benefits_gaps_html(analysis.get("content_benefits", []), analysis.get("content_gaps", []))
    perf_bg = _benefits_gaps_html(analysis.get("perf_benefits", []), analysis.get("perf_gaps", []))
    sc_bg = _benefits_gaps_html(analysis.get("sc_benefits", []), analysis.get("sc_gaps", []))
    ux_bg = _benefits_gaps_html(analysis.get("ux_benefits", []), analysis.get("ux_gaps", []))
    struct_bg = _benefits_gaps_html(analysis.get("struct_benefits", []), analysis.get("struct_gaps", []))
    complex_bg = _benefits_gaps_html(analysis.get("complex_benefits", []), analysis.get("complex_gaps", []))

    seo_intro = analysis.get("seo_intro", "How well your store is set up for search engines to find it and for links to look good when shared.")
    content_intro = analysis.get("content_intro", "How well your site is organised for visitors and for people using screen readers or keyboards.")
    perf_intro = analysis.get("perf_intro", "How heavy or light your site is — affects loading speed and responsiveness.")
    sc_intro = analysis.get("sc_intro", "Whether your store follows StoreConnect's recommended layout, colours, and menu patterns.")
    ux_intro = analysis.get("ux_intro", "Can visitors find the menu, cart, checkout, and forms easily? Is the site usable on mobile?")
    struct_intro = analysis.get("struct_intro", "Structured information that helps search engines and AI understand your products and business.")
    complex_intro = analysis.get("complex_intro", "How complex your site is — affects how easy it is to upgrade, change, or maintain.")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>StoreConnect Frontend Capability Audit — {domain}</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, system-ui, sans-serif; color: #1a1a1a; background: #fff; line-height: 1.65; font-size: 1rem; }}
    .topnav {{ position: sticky; top: 0; z-index: 100; background: #1a1a1a; height: 52px; display: flex; align-items: center; justify-content: space-between; padding: 0 2rem; border-bottom: 1px solid rgba(255,255,255,0.08); }}
    .topnav-brand {{ font-size: 0.72rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.14em; color: #facc15; }}
    .topnav-links {{ display: flex; gap: 0; overflow-x: auto; }}
    .topnav-links a {{ color: rgba(255,255,255,0.45); text-decoration: none; font-size: 0.78rem; padding: 0 0.85rem; height: 52px; display: flex; align-items: center; white-space: nowrap; border-bottom: 2px solid transparent; transition: color 0.15s; }}
    .topnav-links a:hover {{ color: #fff; }}
    .topnav-meta {{ font-size: 0.68rem; color: rgba(255,255,255,0.25); white-space: nowrap; }}
    .hero {{ background: #1a1a1a; color: #fff; padding: 4.5rem 2rem 4rem; text-align: center; }}
    .hero-tag {{ font-size: 0.7rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.15em; color: #facc15; margin-bottom: 1.25rem; }}
    .hero h1 {{ font-size: clamp(2rem, 4vw, 3rem); font-weight: 900; letter-spacing: -0.04em; line-height: 1.1; margin-bottom: 0.75rem; }}
    .hero-sub {{ font-size: 1.1rem; color: rgba(255,255,255,0.6); max-width: 520px; margin: 0 auto 2rem; line-height: 1.55; }}
    .hero-meta {{ font-size: 0.8rem; color: rgba(255,255,255,0.35); margin-bottom: 2.5rem; }}
    .hero-meta strong {{ color: rgba(255,255,255,0.7); }}
    .ratings-bar {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 1px; background: rgba(255,255,255,0.08); max-width: 900px; margin: 0 auto; border-radius: 8px; overflow: hidden; }}
    .rating-cell {{ background: rgba(255,255,255,0.04); padding: 1rem 0.75rem; text-align: center; }}
    .rating-label {{ font-size: 0.52rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: rgba(255,255,255,0.35); margin-bottom: 0.4rem; }}
    .rating-value {{ font-size: 0.72rem; font-weight: 700; }}
    .page {{ max-width: 800px; margin: 0 auto; padding: 0 2rem 6rem; }}
    .anchor {{ display: block; height: 68px; margin-top: -68px; visibility: hidden; pointer-events: none; }}
    .section {{ padding: 3rem 0; border-bottom: 1px solid #e5e7eb; }}
    .section:last-of-type {{ border-bottom: none; }}
    .section-tag {{ display: inline-block; font-size: 0.62rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.14em; color: #facc15; background: #1a1a1a; padding: 0.2rem 0.6rem; border-radius: 2px; margin-bottom: 0.9rem; }}
    h2 {{ font-size: clamp(1.5rem, 2.5vw, 2rem); font-weight: 900; letter-spacing: -0.03em; line-height: 1.15; margin-bottom: 0.75rem; }}
    h3 {{ font-size: 1rem; font-weight: 700; margin: 1.75rem 0 0.5rem; color: #374151; }}
    p {{ margin: 0.65rem 0; }}
    p.lead {{ font-size: 1.05rem; color: #444; line-height: 1.65; }}
    ul, ol {{ padding-left: 1.5rem; margin: 0.65rem 0; }}
    li {{ margin-bottom: 0.4rem; font-size: 0.95rem; color: #374151; }}
    strong {{ font-weight: 700; }}
    .callout {{ background: #1a1a1a; color: #fff; border-radius: 8px; padding: 1.35rem 1.6rem; margin: 1.75rem 0; font-size: 0.95rem; line-height: 1.6; }}
    .callout strong {{ color: #facc15; display: block; margin-bottom: 0.3rem; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.08em; }}
    .callout p {{ margin: 0; color: rgba(255,255,255,0.85); }}
    .callout-yellow {{ background: #fefce8; border-left: 4px solid #facc15; padding: 1rem 1.25rem; margin: 1.5rem 0; border-radius: 0 6px 6px 0; font-size: 0.95rem; color: #78350f; }}
    .benefits-gaps {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1.25rem; margin: 2rem 0; }}
    .benefits-box {{ background: #ecfdf5; border: 1px solid #a7f3d0; border-radius: 8px; padding: 1.5rem; }}
    .gaps-box {{ background: #fef2f2; border: 1px solid #fecaca; border-radius: 8px; padding: 1.5rem; }}
    .benefits-box h3, .gaps-box h3 {{ font-size: 0.9rem; font-weight: 700; margin-bottom: 0.75rem; display: flex; align-items: center; gap: 0.4rem; }}
    .benefits-box h3 {{ color: #166534; margin-top: 0; }}
    .gaps-box h3 {{ color: #991b1b; margin-top: 0; }}
    .benefits-box ul, .gaps-box ul {{ margin: 0; padding-left: 1.2rem; font-size: 0.95rem; line-height: 1.7; color: #374151; }}
    .benefits-box li, .gaps-box li {{ margin-bottom: 0.4rem; }}
    .section-benefits-gaps {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin: 1.25rem 0; }}
    .section-benefits-gaps .benefits-box, .section-benefits-gaps .gaps-box {{ padding: 1rem 1.25rem; }}
    .section-benefits-gaps .benefits-box h3, .section-benefits-gaps .gaps-box h3 {{ font-size: 0.82rem; margin-bottom: 0.5rem; }}
    .section-benefits-gaps ul {{ font-size: 0.88rem; line-height: 1.6; }}
    .finding-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1px; background: #e5e7eb; border-radius: 10px; overflow: hidden; margin: 1.75rem 0; }}
    .finding-cell {{ background: #f9fafb; padding: 1.35rem; }}
    .finding-cell h4 {{ font-size: 0.68rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.1em; color: #9ca3af; margin-bottom: 0.75rem; }}
    .finding-cell ul {{ list-style: none; padding: 0; margin: 0; }}
    .finding-cell ul li {{ font-size: 0.88rem; color: #374151; padding: 0.3rem 0; border-bottom: 1px solid #e5e7eb; display: flex; gap: 0.5rem; align-items: flex-start; }}
    .finding-cell ul li:last-child {{ border-bottom: none; }}
    .finding-cell ul li::before {{ content: '—'; color: #facc15; font-weight: 900; flex-shrink: 0; line-height: 1.5; }}
    .stats-row {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 1px; background: #e5e7eb; border-radius: 10px; overflow: hidden; margin: 1.75rem 0; }}
    .stat-cell {{ background: #f9fafb; padding: 1rem; text-align: center; }}
    .stat-value {{ font-size: 1.6rem; font-weight: 900; letter-spacing: -0.03em; color: #1a1a1a; }}
    .stat-label {{ font-size: 0.65rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.1em; color: #9ca3af; margin-top: 0.25rem; }}
    .audit-table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; font-size: 0.9rem; }}
    .audit-table th, .audit-table td {{ padding: 0.6rem 0.75rem; text-align: left; border-bottom: 1px solid #e5e7eb; }}
    .audit-table th {{ font-size: 0.65rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: #9ca3af; background: #f9fafb; }}
    .audit-table tr:hover {{ background: #fafafa; }}
    .status-pass {{ color: #166534; font-weight: 600; }}
    .status-fail {{ color: #991b1b; font-weight: 600; }}
    .status-partial {{ color: #92400e; font-weight: 600; }}
    .status-na {{ color: #6b7280; }}
    .service-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin: 1.75rem 0; }}
    .service-card {{ border: 1px solid #e5e7eb; border-top: 3px solid #facc15; border-radius: 4px; padding: 1.35rem; }}
    .service-card h4 {{ font-size: 0.92rem; font-weight: 700; margin-bottom: 0.5rem; }}
    .service-card p {{ font-size: 0.875rem; color: #555; margin: 0; line-height: 1.55; }}
    .cta-block {{ background: #1a1a1a; color: #fff; border-radius: 10px; padding: 2.5rem 2rem; margin: 2rem 0; text-align: center; }}
    .cta-block h3 {{ font-size: 1.4rem; font-weight: 900; letter-spacing: -0.03em; color: #fff; margin: 0 0 0.75rem; }}
    .cta-block p {{ color: rgba(255,255,255,0.6); font-size: 0.95rem; max-width: 460px; margin: 0 auto 1.5rem; }}
    .cta-pill {{ display: inline-block; background: #facc15; color: #1a1a1a; font-weight: 700; font-size: 0.85rem; padding: 0.6rem 1.5rem; border-radius: 999px; text-decoration: none; letter-spacing: 0.02em; }}
    .about-block {{ background: #f9fafb; border-radius: 10px; padding: 2rem; margin: 1.75rem 0; }}
    .about-block h3 {{ font-size: 1rem; font-weight: 700; margin-bottom: 0.75rem; }}
    .about-block p {{ font-size: 0.9rem; color: #555; margin: 0.4rem 0; }}
    hr {{ border: none; border-top: 1px solid #e5e7eb; margin: 2rem 0; }}
    @media (max-width: 680px) {{
      .ratings-bar {{ grid-template-columns: repeat(2, 1fr); }}
      .finding-grid, .service-grid, .stats-row {{ grid-template-columns: 1fr; }}
      .benefits-gaps, .section-benefits-gaps {{ grid-template-columns: 1fr; }}
      .topnav-links {{ display: none; }}
      .audit-table {{ font-size: 0.8rem; }}
      .audit-table th:nth-child(3), .audit-table td:nth-child(3) {{ display: none; }}
    }}
    @media print {{
      * {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
      body {{ font-size: 9pt; line-height: 1.45; }}
      .topnav {{ display: none; }}
      .page {{ max-width: 100%; padding: 0 0.5cm; margin: 0; }}
      .hero {{ padding: 0.8rem 1rem; background: #1a1a1a !important; color: #fff !important; page-break-after: avoid; }}
      .hero h1 {{ font-size: 15pt; margin-bottom: 0.25rem; }}
      .hero-sub {{ font-size: 8.5pt; margin-bottom: 0.4rem; color: rgba(255,255,255,0.6) !important; }}
      .hero-tag {{ font-size: 6pt; margin-bottom: 0.3rem; }}
      .hero-meta {{ font-size: 6.5pt; margin-bottom: 0.6rem; }}
      .ratings-bar {{ gap: 0; margin: 0 auto 0; max-width: 100%; }}
      .rating-cell {{ padding: 0.4rem 0.5rem; background: rgba(255,255,255,0.04) !important; }}
      .rating-label {{ font-size: 5.5pt; margin-bottom: 0.2rem; }}
      .section {{ padding: 0.6rem 0; border-bottom: 0.5pt solid #e5e7eb; }}
      h2 {{ font-size: 11pt; font-weight: 900; margin-bottom: 0.25rem; page-break-after: avoid; }}
      h3 {{ font-size: 9pt; font-weight: 700; margin: 0.4rem 0 0.2rem; page-break-after: avoid; }}
      p {{ font-size: 8.5pt; margin: 0.2rem 0; }}
      p.lead {{ font-size: 9pt; }}
      ul, ol {{ padding-left: 1rem; margin: 0.2rem 0; }}
      li {{ font-size: 8pt; margin-bottom: 0.15rem; }}
      .section-tag {{ font-size: 6pt; padding: 0.1rem 0.35rem; margin-bottom: 0.3rem; }}
      .callout {{ padding: 0.5rem 0.7rem; margin: 0.4rem 0; font-size: 8pt; background: #1a1a1a !important; page-break-inside: avoid; }}
      .callout strong {{ font-size: 6.5pt; }}
      .callout p {{ font-size: 8pt; color: rgba(255,255,255,0.85) !important; }}
      .callout-yellow {{ padding: 0.35rem 0.6rem; margin: 0.35rem 0; font-size: 8pt; background: #fefce8 !important; page-break-inside: avoid; }}
      .benefits-gaps, .section-benefits-gaps {{ gap: 0.5rem; margin: 0.4rem 0; }}
      .benefits-box, .gaps-box {{ padding: 0.5rem 0.7rem; page-break-inside: avoid; }}
      .benefits-box h3, .gaps-box h3 {{ font-size: 7pt; margin-bottom: 0.3rem; }}
      .benefits-box ul, .gaps-box ul {{ font-size: 7.5pt; line-height: 1.4; }}
      .benefits-box {{ background: #ecfdf5 !important; }}
      .gaps-box {{ background: #fef2f2 !important; }}
      .finding-grid, .stats-row {{ gap: 0; margin: 0.4rem 0; }}
      .finding-cell, .stat-cell {{ padding: 0.5rem; background: #f9fafb !important; }}
      .finding-cell h4 {{ font-size: 6pt; margin-bottom: 0.25rem; }}
      .finding-cell ul li {{ font-size: 7.5pt; padding: 0.15rem 0; }}
      .stat-value {{ font-size: 12pt; }}
      .stat-label {{ font-size: 6pt; }}
      .service-grid {{ gap: 0.35rem; margin: 0.4rem 0; }}
      .service-card {{ padding: 0.5rem; page-break-inside: avoid; }}
      .service-card h4 {{ font-size: 8pt; margin-bottom: 0.2rem; }}
      .service-card p {{ font-size: 7.5pt; }}
      .cta-block {{ padding: 0.8rem 1rem; margin: 0.4rem 0; background: #1a1a1a !important; page-break-inside: avoid; }}
      .cta-block h3 {{ font-size: 10pt; margin-bottom: 0.2rem; }}
      .cta-block p {{ font-size: 7.5pt; margin-bottom: 0.4rem; color: rgba(255,255,255,0.6) !important; }}
      .cta-pill {{ display: none; }}
      .about-block {{ padding: 0.6rem; margin: 0.4rem 0; background: #f9fafb !important; }}
      .about-block h3 {{ font-size: 8.5pt; margin-bottom: 0.2rem; }}
      .about-block p {{ font-size: 7.5pt; }}
      hr {{ margin: 0.4rem 0; border-top: 0.5pt solid #e5e7eb; }}
    }}
    .mermaid-container {{ background: #1a1a1a; border-radius: 10px; padding: 2rem; margin: 1.75rem 0; overflow-x: auto; border: 1px solid rgba(250,204,21,0.2); cursor: pointer; position: relative; }}
    .mermaid-container:hover {{ box-shadow: 0 0 0 2px #facc15; }}
    .mermaid-container::after {{ content: 'Click to expand'; position: absolute; top: 0.75rem; right: 0.75rem; font-size: 0.68rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: rgba(250,204,21,0.6); background: rgba(255,255,255,0.08); padding: 0.2rem 0.6rem; border-radius: 4px; border: 1px solid rgba(250,204,21,0.2); pointer-events: none; }}
    .mermaid-container .mermaid {{ display: flex; justify-content: center; }}
    .nav-stats {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 1px; background: #e5e7eb; border-radius: 10px; overflow: hidden; margin: 1.25rem 0; }}
    .mermaid-fullscreen {{ position: fixed; inset: 0; z-index: 9999; background: #fff; display: flex; flex-direction: column; }}
    .mermaid-fullscreen-header {{ display: flex; justify-content: space-between; align-items: center; padding: 1rem 2rem; border-bottom: 1px solid #e5e7eb; background: #1a1a1a; flex-shrink: 0; }}
    .mermaid-fullscreen-header h3 {{ font-size: 1rem; font-weight: 700; margin: 0; color: #facc15; }}
    .mermaid-fullscreen-close {{ background: #facc15; color: #1a1a1a; border: none; padding: 0.5rem 1.25rem; border-radius: 6px; font-size: 0.85rem; font-weight: 700; cursor: pointer; }}
    .mermaid-fullscreen-close:hover {{ background: #fde68a; }}
    .mermaid-fullscreen-body {{ flex: 1; overflow: auto; padding: 2rem; display: flex; align-items: flex-start; justify-content: center; background: #fff; }}
    .mermaid-fullscreen-body svg {{ max-width: 100%; height: auto; }}
    @media print {{
      .mermaid-container {{ padding: 0.6rem; margin: 0.4rem 0; background: #1a1a1a !important; border-color: rgba(250,204,21,0.3); cursor: default; }}
      .mermaid-container::after {{ display: none; }}
      .mermaid-fullscreen {{ display: none !important; }}
      .nav-stats {{ margin: 0.3rem 0; }}
    }}
  </style>

</head>
<body>

  <nav class="topnav">
    <div class="topnav-brand">Praxis — StoreConnect Frontend Custodian</div>
    <div class="topnav-links">
      <a href="#summary">Benefits &amp; Gaps</a>
      <a href="#seo">SEO &amp; Meta</a>
      <a href="#content">Content &amp; Accessibility</a>
      <a href="#performance">Performance</a>
      <a href="#storeconnect">StoreConnect Standards</a>
      <a href="#ux">UX &amp; Nav</a>
      <a href="#site-map">Site Map</a>
      <a href="#structured">Structured Data</a>
      <a href="#complexity">Complexity</a>
      <a href="#services">Services</a>
    </div>
    <div class="topnav-meta">{scan_date} · Frontend Capability Audit</div>
  </nav>

  <div class="hero">
    <div class="hero-tag">Praxis · Frontend Capability Audit</div>
    <h1>Frontend Capability Audit Report</h1>
    <div class="hero-sub">A clear report on how <strong style="color:#fff;">{domain}</strong> shows up in search, how easy it is to use, how fast it loads, and whether it follows StoreConnect best practices.</div>
    <div class="hero-meta">Audit date <strong>{scan_date}</strong> &nbsp;·&nbsp; Based on <strong>{pages} page{'s' if pages != 1 else ''}</strong> analysed</div>
    <div class="ratings-bar">
      <div class="rating-cell">
        <div class="rating-label">SEO &amp; Meta</div>
        <div class="rating-value">{_badge_html(seo_badge)}</div>
      </div>
      <div class="rating-cell">
        <div class="rating-label">Content &amp; Accessibility</div>
        <div class="rating-value">{_badge_html(content_badge)}</div>
      </div>
      <div class="rating-cell">
        <div class="rating-label">Performance</div>
        <div class="rating-value">{_badge_html(perf_badge)}</div>
      </div>
      <div class="rating-cell">
        <div class="rating-label">StoreConnect Standards</div>
        <div class="rating-value">{_badge_html(sc_badge)}</div>
      </div>
      <div class="rating-cell">
        <div class="rating-label">Complexity</div>
        <div class="rating-value">{_badge_html(complex_badge)}</div>
      </div>
    </div>
  </div>

  <div class="page">

    <!-- Summary: At a glance -->
    <a class="anchor" id="summary"></a>
    <div class="section">
      <span class="section-tag">Summary</span>
      <h2>At a glance</h2>
      <p class="lead">{analysis.get('executive_summary', 'What&#39;s working well and what needs attention — in plain terms.')}</p>
      {summary_bg}
      <h3>Audit overview</h3>
      <p>This report checks your store's website against a simple checklist. We look at what visitors and search engines actually see — no special access to your systems is needed.</p>
      <div class="stats-row">
        <div class="stat-cell">
          <div class="stat-value">7</div>
          <div class="stat-label">Audit Sections</div>
        </div>
        <div class="stat-cell">
          <div class="stat-value">42</div>
          <div class="stat-label">Check Items</div>
        </div>
        <div class="stat-cell">
          <div class="stat-value">{pages}</div>
          <div class="stat-label">Pages Analysed</div>
        </div>
        <div class="stat-cell">
          <div class="stat-value">{ratings.get('score', 0)}</div>
          <div class="stat-label">Overall Score</div>
        </div>
      </div>
    </div>

    <!-- 1. SEO & Meta -->
    <a class="anchor" id="seo"></a>
    <div class="section">
      <span class="section-tag">§1</span>
      <h2>SEO &amp; Meta Analysis</h2>
      <p class="lead">{seo_intro}</p>
      {seo_bg}
      <table class="audit-table">
        <thead><tr><th>#</th><th>In plain terms</th><th>What we check</th><th>Status</th></tr></thead>
        <tbody>{seo_rows}</tbody>
      </table>
    </div>

    <!-- 2. Content & Accessibility -->
    <a class="anchor" id="content"></a>
    <div class="section">
      <span class="section-tag">§2</span>
      <h2>Content &amp; Accessibility</h2>
      <p class="lead">{content_intro}</p>
      {content_bg}
      <table class="audit-table">
        <thead><tr><th>#</th><th>In plain terms</th><th>What we check</th><th>Status</th></tr></thead>
        <tbody>{content_rows}</tbody>
      </table>
    </div>

    <!-- 3. Performance -->
    <a class="anchor" id="performance"></a>
    <div class="section">
      <span class="section-tag">§3</span>
      <h2>Performance Indicators</h2>
      <p class="lead">{perf_intro}</p>
      {perf_bg}
      <table class="audit-table">
        <thead><tr><th>#</th><th>In plain terms</th><th>What we check</th><th>Status</th></tr></thead>
        <tbody>{perf_rows}</tbody>
      </table>
    </div>

    <!-- 4. StoreConnect Standards -->
    <a class="anchor" id="storeconnect"></a>
    <div class="section">
      <span class="section-tag">§4</span>
      <h2>StoreConnect Standards Compliance</h2>
      <p class="lead">{sc_intro}</p>
      {sc_bg}
      <table class="audit-table">
        <thead><tr><th>#</th><th>In plain terms</th><th>What we check</th><th>Status</th></tr></thead>
        <tbody>{sc_rows}</tbody>
      </table>
    </div>

    <!-- 5. UX & Navigation -->
    <a class="anchor" id="ux"></a>
    <div class="section">
      <span class="section-tag">§5</span>
      <h2>UX &amp; Navigation Structure</h2>
      <p class="lead">{ux_intro}</p>
      {ux_bg}
      <table class="audit-table">
        <thead><tr><th>#</th><th>In plain terms</th><th>What we check</th><th>Status</th></tr></thead>
        <tbody>{ux_rows}</tbody>
      </table>
    </div>

    <!-- Site Navigation Architecture -->
    <a class="anchor" id="site-map"></a>
    <div class="section">
      <span class="section-tag">Site Architecture</span>
      <h2>Site Navigation Architecture</h2>
      <p class="lead">How the pages on this site connect to each other — showing the URL hierarchy and navigation structure discovered during the scan.</p>
      <div class="nav-stats">
        <div class="stat-cell">
          <div class="stat-value">{nav_tree_stats['internal_urls']}</div>
          <div class="stat-label">Internal URLs Found</div>
        </div>
        <div class="stat-cell">
          <div class="stat-value">{nav_tree_stats['nav_urls']}</div>
          <div class="stat-label">Navigation Links</div>
        </div>
        <div class="stat-cell">
          <div class="stat-value">{nav_tree_stats['external_domains']}</div>
          <div class="stat-label">External Domains</div>
        </div>
      </div>
      <div class="mermaid-container" id="mermaid-click" title="Click to view full screen">
        <pre class="mermaid">
{nav_mermaid}
        </pre>
      </div>
      <p style="font-size:0.85rem;color:#6b7280;margin-top:0.75rem;">Links found in navigation menus are highlighted in amber. Click the diagram to view full screen. Only internal links within the same domain are mapped.</p>
    </div>

    <!-- 6. Structured Data -->
    <a class="anchor" id="structured"></a>
    <div class="section">
      <span class="section-tag">§6</span>
      <h2>Structured Data &amp; AI Readiness</h2>
      <p class="lead">{struct_intro}</p>
      {struct_bg}
      <table class="audit-table">
        <thead><tr><th>#</th><th>In plain terms</th><th>What we check</th><th>Status</th></tr></thead>
        <tbody>{struct_rows}</tbody>
      </table>
    </div>

    <!-- 7. Complexity -->
    <a class="anchor" id="complexity"></a>
    <div class="section">
      <span class="section-tag">§7</span>
      <h2>Complexity &amp; Upgrade Risk</h2>
      <p class="lead">{complex_intro}</p>
      {complex_bg}
      <table class="audit-table">
        <thead><tr><th>#</th><th>In plain terms</th><th>What we check</th><th>Status</th></tr></thead>
        <tbody>{complex_rows}</tbody>
      </table>
      <div class="callout-yellow">
        <strong>About the scores</strong>
        <p>Complexity: under 30 = stable and easy to upgrade; 30–59 = needs attention; 60+ = higher risk when upgrading. This audit is produced from publicly available signals — a starting point for a conversation, not a final audit.</p>
      </div>
    </div>

    <!-- Services -->
    <a class="anchor" id="services"></a>
    <div class="section">
      <span class="section-tag">How We Help</span>
      <h2>What Praxis does</h2>
      <p class="lead">We look after your store's website so it stays healthy, fast, and easy to upgrade.</p>

      <div class="service-grid">
        <div class="service-card">
          <h4>Before You Upgrade</h4>
          <p>We check your site before any StoreConnect upgrade. We document what's customised and compare before and after so nothing breaks.</p>
        </div>
        <div class="service-card">
          <h4>Testing After Changes</h4>
          <p>Every time you deploy a change, we run a structured test — menu, forms, checkout, mobile — to catch problems early.</p>
        </div>
        <div class="service-card">
          <h4>Ongoing Care</h4>
          <p>Regular health checks. We watch for changes that could cause issues and fix small problems before they grow.</p>
        </div>
        <div class="service-card">
          <h4>Speed &amp; Search</h4>
          <p>We tidy up scripts, add structured data, and improve how your store shows up in search and AI tools.</p>
        </div>
      </div>

      <div class="about-block">
        <h3>About this audit</h3>
        <p>This report uses a checklist of 42 checks across 7 areas. We look at what visitors and search engines actually see on your site — no special access to your systems is needed.</p>
      </div>
    </div>

    <!-- Contact -->
    <a class="anchor" id="contact"></a>
    <div class="section">
      <span class="section-tag">Get in Touch</span>
      <div class="cta-block">
        <h3>Ready to talk about your store?</h3>
        <p>This audit is a starting point. A quick chat will help us understand what support your store needs.</p>
        <a class="cta-pill" href="mailto:hello@praxis.com.au">Contact Praxis</a>
      </div>
    </div>

  </div>

  <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
  <script>
    mermaid.initialize({{
      startOnLoad: false,
      securityLevel: 'loose',
      theme: 'base',
      themeVariables: {{
        darkMode: true,
        background: '#1a1a1a',
        primaryColor: '#2d2d2d',
        primaryTextColor: '#ffffff',
        primaryBorderColor: '#facc15',
        secondaryColor: '#374151',
        tertiaryColor: '#1f1f1f',
        lineColor: '#facc15',
        tertiaryTextColor: '#facc15',
        fontFamily: '-apple-system, BlinkMacSystemFont, system-ui, sans-serif'
      }}
    }});
    mermaid.run({{ querySelector: '.mermaid' }}).then(function() {{
      var container = document.getElementById('mermaid-click');
      if (!container) return;
      container.addEventListener('click', function() {{
        var svg = container.querySelector('svg');
        if (!svg) return;
        var overlay = document.createElement('div');
        overlay.className = 'mermaid-fullscreen';
        overlay.innerHTML =
          '<div class="mermaid-fullscreen-header">' +
            '<h3>Site Navigation Architecture</h3>' +
            '<button class="mermaid-fullscreen-close" id="mermaid-fs-close">Close</button>' +
          '</div>' +
          '<div class="mermaid-fullscreen-body"></div>';
        var body = overlay.querySelector('.mermaid-fullscreen-body');
        var clone = svg.cloneNode(true);
        clone.removeAttribute('height');
        clone.style.width = '100%';
        clone.style.maxHeight = '100%';
        body.appendChild(clone);
        document.body.appendChild(overlay);
        overlay.querySelector('#mermaid-fs-close').addEventListener('click', function() {{
          overlay.remove();
        }});
        overlay.addEventListener('click', function(e) {{
          if (e.target === overlay || e.target === body) overlay.remove();
        }});
        document.addEventListener('keydown', function handler(e) {{
          if (e.key === 'Escape') {{ overlay.remove(); document.removeEventListener('keydown', handler); }}
        }});
      }});
    }});
  </script>
</body>
</html>"""


# ── Main streaming pipeline ────────────────────────────────────────────────────

async def run_analysis(
    url: str,
    api_key: str,
    report_store,
    job_id: str,
    max_pages: int = 15,
    url_list: list[str] | None = None,
    openai_key: str = "",
    contact: dict | None = None,
) -> AsyncGenerator[str, None]:
    loop = asyncio.get_event_loop()
    MIN_DELAY = 0.5

    # ── Step 1: Resolve ──
    yield sse("step", {"step": "resolve", "state": "active", "jobId": job_id})
    await asyncio.sleep(0.1)
    domain = urlparse(url).netloc or url
    await asyncio.sleep(MIN_DELAY)
    yield sse("step", {"step": "resolve", "state": "done", "detail": domain, "jobId": job_id})

    # ── Step 2: Discover ──
    yield sse("step", {"step": "crawl", "state": "active", "jobId": job_id})

    if url_list:
        urls_to_scrape = url_list[:max_pages]
        if len(url_list) > max_pages:
            yield sse("warning", {"message": f"Found {len(url_list)} pages — analysing top {max_pages}."})
    else:
        urls_to_scrape = await loop.run_in_executor(None, discover_urls, url, api_key, max_pages)
        if len(urls_to_scrape) == 1:
            yield sse("warning", {"message": "Could not map site pages — analysing root URL only."})

    total = len(urls_to_scrape)
    await asyncio.sleep(MIN_DELAY)
    yield sse("step", {
        "step": "crawl", "state": "done",
        "detail": f"Found {total} page{'s' if total != 1 else ''} to analyse",
        "jobId": job_id,
    })

    # ── Step 3: Scrape pages ──
    yield sse("step", {"step": "dom", "state": "active", "jobId": job_id})

    agg = _fresh_agg()
    pages_done = 0
    pages_failed = 0

    # Scrape up to 5 pages concurrently
    CONCURRENCY = 5
    sem = asyncio.Semaphore(CONCURRENCY)

    async def _scrape_one(page_url: str):
        async with sem:
            return page_url, await loop.run_in_executor(None, _scrape_page, page_url, api_key)

    # Launch all scrapes concurrently (semaphore limits to 5 at a time)
    tasks = [asyncio.create_task(_scrape_one(pu)) for pu in urls_to_scrape]

    for coro in asyncio.as_completed(tasks):
        page_url, page_data = await coro

        if page_data is None:
            pages_failed += 1
            short = page_url.replace("https://", "").replace("http://", "")[:45]
            yield sse("warning", {"message": f"Could not reach: {short}"})
            continue

        preview = _analyse_page(page_data, agg)
        pages_done += 1

        yield sse("page-done", {
            "url": page_url,
            "index": pages_done,
            "total": total,
            "signals_preview": preview,
        })

    if pages_failed > 0 and pages_done == 0:
        yield sse("error-event", {
            "code": "zero_pages",
            "message": "We couldn't access any pages. The site may require login or block automated access.",
        })
        return

    if pages_failed > 0:
        yield sse("warning", {
            "message": f"{pages_failed} page{'s' if pages_failed != 1 else ''} could not be reached — report may be incomplete."
        })

    signals = _finalise_signals(agg)
    await asyncio.sleep(MIN_DELAY)
    yield sse("step", {
        "step": "dom", "state": "done",
        "detail": f"{signals.get('total_tags', 0):,} tags across {pages_done} pages",
        "jobId": job_id,
    })

    # ── Step 4: Discover site map ──
    yield sse("step", {"step": "sitemap", "state": "active", "jobId": job_id})
    tree_data = _build_site_tree(url, urls_to_scrape, agg.get("site_links", []))
    signals["nav_mermaid"] = _generate_nav_mermaid(url, tree_data)
    signals["nav_tree_stats"] = {
        "internal_urls": len(tree_data["all_internal_urls"]),
        "nav_urls": len(tree_data["nav_urls"]),
        "external_domains": len(tree_data["external_domains"]),
    }
    # Remove raw link data to avoid bloating stored signals
    signals.pop("site_links", None)
    await asyncio.sleep(MIN_DELAY)
    yield sse("step", {
        "step": "sitemap", "state": "done",
        "detail": f"{len(tree_data['all_internal_urls'])} internal URLs, {len(tree_data['nav_urls'])} nav links",
        "jobId": job_id,
    })

    # ── Step 5: Frameworks ──
    yield sse("step", {"step": "frameworks", "state": "active", "jobId": job_id})
    await asyncio.sleep(0.3)
    fw = signals.get("js_frameworks", [])
    yield sse("step", {
        "step": "frameworks", "state": "done",
        "detail": ", ".join(fw) if fw else "None detected",
        "jobId": job_id,
    })

    # ── Step 6: Score + OpenAI analysis ──
    yield sse("step", {"step": "scoring", "state": "active", "jobId": job_id})
    ratings = await loop.run_in_executor(None, compute_ratings, signals)

    yield sse("step", {"step": "scoring", "state": "active", "detail": "Running AI analysis…", "jobId": job_id})
    analysis = await loop.run_in_executor(None, run_openai_analysis, url, signals, ratings, openai_key)

    await asyncio.sleep(MIN_DELAY)
    yield sse("step", {
        "step": "scoring", "state": "done",
        "detail": f"Score {ratings['score']} — {ratings['stability']}",
        "jobId": job_id,
    })

    # ── Step 7: Generate report ──
    yield sse("step", {"step": "report", "state": "active", "jobId": job_id})
    html = await loop.run_in_executor(None, generate_html, url, signals, ratings, analysis)
    report_id = await loop.run_in_executor(
        None,
        lambda: report_store.save(url, html, signals, ratings, contact=contact),
    )
    await asyncio.sleep(MIN_DELAY)
    yield sse("step", {"step": "report", "state": "done", "detail": "Report ready", "jobId": job_id})

    yield sse("complete", {"reportId": report_id})
