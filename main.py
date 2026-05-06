"""
EcomRocket Signal Pipeline
--------------------------
Fetches signals from newsletters, Google Alerts, and TikTok, then upserts
new DTC/Shopify brand leads into Airtable with TikTok search links attached.

Usage:
  python main.py                # run once and exit
  python main.py --schedule     # run on the interval defined in config.yaml
  python main.py --dry-run      # print results without writing to Airtable
  python main.py --list-alerts  # print every Google Alert query to create
"""

import argparse
import logging
import schedule
import time

from config import load_config
from rss_collector import collect_all_feeds
from tiktok_collector import collect_tiktok
from signal_scorer import score_and_filter
from airtable_client import AirtableClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _expand_alert_queries(cfg: dict) -> list[str]:
    """Return the full list of Google Alert queries the user should create."""
    tracking = cfg.get("tracking") or {}
    brands = tracking.get("brands") or []
    products = tracking.get("products") or []
    alert_cfg = cfg.get("google_alerts") or {}

    queries = list(alert_cfg.get("literal") or [])

    for tpl in alert_cfg.get("brand_templates") or []:
        if brands:
            queries.extend(tpl.replace("[brand]", b) for b in brands)

    for tpl in alert_cfg.get("product_templates") or []:
        if products:
            queries.extend(tpl.replace("[product]", p) for p in products)

    return queries


def list_alerts(cfg: dict) -> None:
    queries = _expand_alert_queries(cfg)
    if not queries:
        print("No Google Alert queries to create yet.")
        print("Add brands/products to the tracking: section in config.yaml first.")
        return

    print(f"\n{'='*55}")
    print(f"  Google Alert queries to create ({len(queries)} total)")
    print(f"  google.com/alerts → type query → RSS feed → Create")
    print(f"{'='*55}")
    for q in queries:
        print(f'  {q}')
    print(f"{'='*55}")
    print("  After creating each alert, paste its RSS URL into")
    print("  the feeds: section of config.yaml.\n")


def run_pipeline(cfg: dict, dry_run: bool = False) -> None:
    log.info("=== Pipeline start ===")

    tracking = cfg.get("tracking") or {}
    tiktok_cfg = {**(cfg.get("tiktok") or {}), **tracking}

    rss_items = collect_all_feeds(cfg.get("feeds") or [])
    tiktok_items = collect_tiktok(tiktok_cfg)
    items = rss_items + tiktok_items
    scored = score_and_filter(items, cfg.get("keywords") or {})

    if not scored:
        log.info("No items passed the filter.")
        return

    if dry_run:
        log.info("[DRY RUN] Would insert %d items:", len(scored))
        for i in scored:
            log.info("  [%s] %s — %s", i["revenue_signal"], i["brand_name"], i["source_link"])
        return

    client = AirtableClient(cfg["airtable"])
    existing_urls = client.get_existing_urls()
    new_items = [i for i in scored if i["source_link"] not in existing_urls]
    log.info("New items (after dedup): %d", len(new_items))

    client.bulk_insert(new_items)
    log.info("=== Pipeline complete. Inserted %d records. ===", len(new_items))


def main():
    parser = argparse.ArgumentParser(description="EcomRocket Signal Pipeline")
    parser.add_argument("--schedule", action="store_true", help="Run on a recurring schedule")
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing to Airtable")
    parser.add_argument("--list-alerts", action="store_true", help="Print all Google Alert queries to create")
    args = parser.parse_args()

    cfg = load_config()

    if args.list_alerts:
        list_alerts(cfg)
        return

    if args.schedule:
        interval = (cfg.get("schedule") or {}).get("interval_minutes") or 60
        log.info("Scheduling pipeline every %d minutes.", interval)
        run_pipeline(cfg, dry_run=args.dry_run)
        schedule.every(interval).minutes.do(run_pipeline, cfg=cfg, dry_run=args.dry_run)
        while True:
            schedule.run_pending()
            time.sleep(30)
    else:
        run_pipeline(cfg, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
