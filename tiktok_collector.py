"""
TikTok collector using Apify's clockworks/tiktok-scraper actor.
Pulls videos from hashtag pages AND keyword searches, filtered by view count.

Keyword templates:
  Use [brand] and [product] as placeholders in search_keywords.
  They are expanded against the brands/products lists in config.yaml.
  e.g. "[brand] TikTok" + brands: [SKIMS, Gymshark] -> "SKIMS TikTok", "Gymshark TikTok"
  Keywords with no placeholder run as-is (e.g. "TikTok made me buy").

Setup:
  1. pip install apify-client
  2. Sign up at https://apify.com (free tier: $5/mo of platform credits)
  3. Grab your API token: https://console.apify.com/account/integrations
  4. Set APIFY_TOKEN in your .env file

Pricing: clockworks/tiktok-scraper bills ~$0.30 per 1,000 results.
"""

import logging
import os

log = logging.getLogger(__name__)

ACTOR_ID = "clockworks/tiktok-scraper"


def _expand_keywords(templates: list[str], brands: list[str], products: list[str]) -> list[str]:
    """Expand [brand] / [product] placeholders into concrete search queries."""
    templates = templates or []
    brands = brands or []
    products = products or []
    expanded = []
    for tpl in templates:
        has_brand = "[brand]" in tpl
        has_product = "[product]" in tpl

        if has_brand and brands:
            for brand in brands:
                expanded.append(tpl.replace("[brand]", brand))
        elif has_product and products:
            for product in products:
                expanded.append(tpl.replace("[product]", product))
        elif not has_brand and not has_product:
            expanded.append(tpl)
        else:
            log.warning("Skipping template '%s' — no %s defined in config",
                        tpl, "brands" if has_brand else "products")

    seen = set()
    return [kw for kw in expanded if not (kw in seen or seen.add(kw))]


def _build_record(item: dict, label: str) -> dict:
    play_count = item.get("playCount") or 0
    like_count = item.get("diggCount") or 0
    share_count = item.get("shareCount") or 0

    web_url = item.get("webVideoUrl") or ""
    if not web_url:
        author = (item.get("authorMeta") or {}).get("name") or "unknown"
        vid_id = item.get("id") or ""
        if vid_id:
            web_url = f"https://www.tiktok.com/@{author}/video/{vid_id}"

    return {
        "title": (item.get("text") or "")[:200],
        "summary": f"{play_count:,} views · {like_count:,} likes · {share_count:,} shares",
        "source_link": web_url,
        "platform": "TikTok",
        "feed_label": label,
        "published_at": item.get("createTimeISO") or "",
        "play_count": play_count,
        "pre_filtered": True,  # skip RSS keyword filter — already filtered by query + views
    }


def _label_for_hashtag_item(item: dict, input_hashtags: list[str]) -> str:
    """Best-effort: map a video back to the input hashtag that surfaced it."""
    video_hashtags = item.get("hashtags") or []
    found = {(h.get("name") or "").lower() for h in video_hashtags if isinstance(h, dict)}
    for tag in input_hashtags:
        if tag.lower() in found:
            return f"TikTok #{tag}"
    return "TikTok hashtag"


def _label_for_search_item(item: dict, input_keywords: list[str]) -> str:
    """Best-effort: map a video back to the search query that surfaced it."""
    explicit = item.get("searchQuery") or item.get("input")
    if isinstance(explicit, str) and explicit:
        return f'TikTok search: "{explicit}"'
    text = (item.get("text") or "").lower()
    for kw in input_keywords:
        if kw.lower() in text:
            return f'TikTok search: "{kw}"'
    return "TikTok search"


def _run_actor(client, run_input: dict, source_label: str) -> list[dict]:
    """Run the Apify actor and return all dataset items."""
    try:
        run = client.actor(ACTOR_ID).call(run_input=run_input)
        dataset_id = run["defaultDatasetId"]
        return list(client.dataset(dataset_id).iterate_items())
    except Exception as exc:
        log.error("Apify run failed (%s): %s", source_label, exc)
        return []


def collect_tiktok(tiktok_cfg: dict) -> list[dict]:
    token = os.environ.get("APIFY_TOKEN", "")
    if not token:
        log.warning("APIFY_TOKEN not set — skipping TikTok. See tiktok_collector.py docstring.")
        return []

    try:
        from apify_client import ApifyClient
    except ImportError:
        log.error("apify-client not installed. Run: pip install apify-client")
        return []

    min_views = tiktok_cfg.get("min_views") or 500_000
    fetch_count = tiktok_cfg.get("fetch_count") or 50
    hashtags = tiktok_cfg.get("hashtags") or []
    brands = tiktok_cfg.get("brands") or []
    products = tiktok_cfg.get("products") or []
    keyword_templates = tiktok_cfg.get("search_keywords") or []

    resolved_keywords = _expand_keywords(keyword_templates, brands, products)
    log.info("TikTok: %d hashtags, %d search queries (from %d templates, %d brands, %d products)",
             len(hashtags), len(resolved_keywords), len(keyword_templates), len(brands), len(products))

    client = ApifyClient(token)
    common_input = {
        "resultsPerPage": fetch_count,
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
        "shouldDownloadSubtitles": False,
    }

    all_results: list[dict] = []

    if hashtags:
        items = _run_actor(client, {**common_input, "hashtags": hashtags},
                           f"hashtags ({len(hashtags)})")
        kept = 0
        for item in items:
            if (item.get("playCount") or 0) < min_views:
                continue
            all_results.append(_build_record(item, _label_for_hashtag_item(item, hashtags)))
            kept += 1
        log.info("TikTok hashtags -> %d/%d above %s views", kept, len(items), f"{min_views:,}")

    if resolved_keywords:
        items = _run_actor(client, {**common_input, "searchQueries": resolved_keywords},
                           f"search ({len(resolved_keywords)})")
        kept = 0
        for item in items:
            if (item.get("playCount") or 0) < min_views:
                continue
            all_results.append(_build_record(item, _label_for_search_item(item, resolved_keywords)))
            kept += 1
        log.info("TikTok search -> %d/%d above %s views", kept, len(items), f"{min_views:,}")

    log.info("TikTok total (above %s views): %d", f"{min_views:,}", len(all_results))
    return all_results
