import logging
import os
from pyairtable import Api

log = logging.getLogger(__name__)

# Airtable field name mapping (change these if you rename columns in Airtable)
FIELD_MAP = {
    "brand_name":         "Brand Name",
    "product_name":       "Product Name",
    "source_link":        "Source Link",
    "platform":           "Platform",
    "revenue_signal":     "Revenue Signal",
    "tiktok_search_link": "TikTok Search Link",
    "status":             "Status",
    "feed_label":         "Source Feed",
    "title":              "Raw Title",
    "published_at":       "Date Added",
}


class AirtableClient:
    def __init__(self, cfg: dict):
        api_key = os.environ.get("AIRTABLE_API_KEY")
        if not api_key:
            raise EnvironmentError("AIRTABLE_API_KEY is not set. Check your .env file.")
        self._api = Api(api_key)
        self._table = self._api.table(cfg["base_id"], cfg["table_name"])

    def get_existing_urls(self) -> set[str]:
        """Fetch all source links already in Airtable to prevent duplicates."""
        field = FIELD_MAP["source_link"]
        try:
            records = self._table.all(fields=[field])
            urls = {r["fields"].get(field, "") for r in records}
            urls.discard("")
            log.info("Loaded %d existing URLs from Airtable", len(urls))
            return urls
        except Exception as exc:
            log.error("Could not fetch existing URLs from Airtable: %s", exc)
            return set()

    def insert(self, item: dict) -> None:
        fields = {}
        for key, airtable_name in FIELD_MAP.items():
            value = item.get(key, "")
            if value:
                fields[airtable_name] = value

        try:
            self._table.create(fields)
            log.info("Inserted: %s  [%s]", item.get("brand_name"), item.get("source_link"))
        except Exception as exc:
            log.error("Failed to insert record for '%s': %s", item.get("brand_name"), exc)

    def bulk_insert(self, items: list[dict]) -> None:
        for item in items:
            self.insert(item)
