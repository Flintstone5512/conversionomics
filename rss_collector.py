import logging
from datetime import datetime, timezone
from typing import Any

import feedparser

log = logging.getLogger(__name__)


def _entry_to_item(entry: Any, feed_cfg: dict) -> dict:
    """Normalize a feedparser entry into a flat dict."""
    link = entry.get("link") or entry.get("id") or ""
    title = entry.get("title", "").strip()
    summary = entry.get("summary", "") or entry.get("description", "")

    # Strip HTML tags from summary
    import re
    summary = re.sub(r"<[^>]+>", " ", summary).strip()

    published = entry.get("published_parsed") or entry.get("updated_parsed")
    if published:
        pub_dt = datetime(*published[:6], tzinfo=timezone.utc).isoformat()
    else:
        pub_dt = datetime.now(timezone.utc).isoformat()

    return {
        "title": title,
        "summary": summary[:500],
        "source_link": link,
        "platform": feed_cfg.get("platform", "Unknown"),
        "feed_label": feed_cfg.get("label", ""),
        "published_at": pub_dt,
    }


def fetch_feed(feed_cfg: dict) -> list[dict]:
    url = feed_cfg["url"]
    if "_REPLACE_" in url:
        log.warning("Skipping placeholder feed URL: %s", url)
        return []
    try:
        parsed = feedparser.parse(url)
        if parsed.get("bozo") and not parsed.get("entries"):
            log.warning("Malformed feed or fetch error for %s: %s", url, parsed.get("bozo_exception"))
            return []
        items = [_entry_to_item(e, feed_cfg) for e in parsed.entries]
        log.info("Fetched %d items from '%s'", len(items), feed_cfg.get("label", url))
        return items
    except Exception as exc:
        log.error("Failed to fetch feed %s: %s", url, exc)
        return []


def collect_all_feeds(feeds: list[dict]) -> list[dict]:
    all_items: list[dict] = []
    for feed_cfg in feeds:
        all_items.extend(fetch_feed(feed_cfg))
    log.info("Total raw items collected: %d", len(all_items))
    return all_items
