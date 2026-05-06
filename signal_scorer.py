import re
import logging
from urllib.parse import quote_plus

log = logging.getLogger(__name__)

_BRAND_PATTERNS = [
    re.compile(r'"([A-Z][^"]{2,40})"'),
    re.compile(r"'([A-Z][^']{2,40})'"),
    re.compile(r"@([A-Za-z0-9_]{3,25})"),
    re.compile(r"\b([A-Z][a-zA-Z]+(?:\s[A-Z][a-zA-Z]+)?)\b"),
]

_SKIP_WORDS = {"The", "This", "That", "Our", "My", "Their", "We", "How", "Why",
               "What", "When", "New", "All", "Top", "Best", "Now"}


def _extract_brand(title: str) -> str:
    for pat in _BRAND_PATTERNS:
        match = pat.search(title)
        if match:
            candidate = match.group(1).strip()
            if candidate not in _SKIP_WORDS and len(candidate) >= 3:
                return candidate
    words = [w for w in title.split() if len(w) > 3 and w[0].isupper()]
    return " ".join(words[:2]) if words else title[:40]


def _tiktok_url(query: str) -> str:
    return f"https://www.tiktok.com/search?q={quote_plus(query)}"


def _score_revenue(text: str, revenue_keywords: dict, play_count: int = 0) -> str:
    lower = text.lower()
    for level in ("high", "medium", "low"):
        for kw in revenue_keywords.get(level, []):
            if kw.lower() in lower:
                return level.capitalize()
    # TikTok items: use view count as a proxy for revenue signal
    if play_count >= 1_000_000:
        return "High"
    if play_count >= 500_000:
        return "Medium"
    return "Low"


def _passes_filter(text: str, required_keywords: list[str]) -> bool:
    lower = text.lower()
    return any(kw.lower() in lower for kw in required_keywords)


def score_and_filter(items: list[dict], keywords: dict) -> list[dict]:
    required = keywords.get("required") or []
    revenue_kws = keywords.get("revenue") or {}
    results = []
    skipped = 0

    for item in items:
        # TikTok items collected via tiktok_collector are already filtered
        # by hashtag + minimum view count — skip keyword check for them
        if not item.get("pre_filtered"):
            combined = f"{item['title']} {item['summary']}"
            if not _passes_filter(combined, required):
                skipped += 1
                continue

        play_count = item.get("play_count", 0)
        combined = f"{item['title']} {item['summary']}"
        brand = _extract_brand(item["title"])
        revenue_signal = _score_revenue(combined, revenue_kws, play_count)
        tiktok_link = _tiktok_url(brand)

        results.append({
            **item,
            "brand_name": brand,
            "product_name": "",
            "revenue_signal": revenue_signal,
            "tiktok_search_link": tiktok_link,
            "status": "Inbox",
        })

    log.info("Items after keyword filter: %d kept, %d skipped", len(results), skipped)
    return results
