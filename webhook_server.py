"""
Webhook server that receives Airtable button triggers and generates teardown scripts.

Airtable setup:
  1. In "Content Inbox" table, add a Button field named "Generate Script"
  2. Set button action to: Open URL → https://your-ngrok-or-server/generate
     (or use Airtable Automations → Run script → webhook)
  3. In Airtable Automations, use "When button clicked" trigger → "Send a webhook" action
     POST body: { "record_id": "{{record_id}}", "base_id": "{{base_id}}" }

Run locally:
  python webhook_server.py            # listens on http://localhost:5000
  ngrok http 5000                     # expose to Airtable (free plan is fine)

Environment variables required (.env):
  AIRTABLE_API_KEY
  AIRTABLE_BASE_ID
  ANTHROPIC_API_KEY
"""

import logging
import os
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from pyairtable import Api
from dotenv import load_dotenv

from script_generator import generate_script

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ── Airtable field names (Content Inbox → Script Queue) ─────────────────────

# Fields read from Content Inbox
INBOX_FIELDS = {
    "brand_name":            "Brand Name",
    "product_name":          "Product Name",
    "source_link":           "Source Link",
    "platform":              "Platform",
    "revenue_signal":        "Revenue Signal",
    "play_count":            "Play Count",
    "raw_title":             "Raw Title",
    # Fields the user fills manually before pressing Generate Script:
    "video_hook":            "Video Hook",
    "video_format":          "Video Format",
    "comment_observations":  "Comment Observations",
    "cta_present":           "CTA Present",
    "bio_link_destination":  "Bio Link Destination",
    "price_point":           "Price Point",
    "product_category":      "Product Category",
}

# Fields written to Script Queue
QUEUE_FIELDS = {
    "Brand Name":      "brand_name",
    "Source Link":     "source_link",
    "Entry Point":     "entry_point",
    "Villain":         "villain",
    "Focal Lens":      "focal_lens",
    "Role":            "role",
    "Outcome":         "outcome",
    "Generated Script":"script_text",
    "Status":          "_status",
    "Date Generated":  "_date",
    "Inbox Record ID": "_inbox_id",
}

SCRIPT_QUEUE_TABLE = "Script Queue"


def _get_airtable_api() -> Api:
    api_key = os.environ.get("AIRTABLE_API_KEY")
    if not api_key:
        raise EnvironmentError("AIRTABLE_API_KEY not set")
    return Api(api_key)


def _fetch_inbox_record(base_id: str, record_id: str) -> dict:
    api = _get_airtable_api()
    table = api.table(base_id, "Content Inbox")
    record = table.get(record_id)
    fields = record.get("fields", {})

    # Map Airtable field names → internal Python keys
    data = {"_record_id": record_id}
    for python_key, airtable_name in INBOX_FIELDS.items():
        data[python_key] = fields.get(airtable_name, "")

    # play_count stored as number in Airtable
    if isinstance(data.get("play_count"), str):
        try:
            data["play_count"] = int(data["play_count"].replace(",", ""))
        except ValueError:
            data["play_count"] = 0

    return data


def _write_to_script_queue(base_id: str, brand_data: dict, result: dict) -> str:
    api = _get_airtable_api()
    table = api.table(base_id, SCRIPT_QUEUE_TABLE)

    fields = {
        "Brand Name":       brand_data.get("brand_name", ""),
        "Source Link":      brand_data.get("source_link", ""),
        "Entry Point":      result["entry_point"],
        "Villain":          result["villain"],
        "Focal Lens":       result["focal_lens"],
        "Role":             result["role"],
        "Outcome":          result["outcome"],
        "Generated Script": result["script_text"],
        "Status":           "Draft",
        "Date Generated":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "Inbox Record ID":  brand_data.get("_record_id", ""),
    }

    # Drop empty values so Airtable doesn't reject them
    fields = {k: v for k, v in fields.items() if v}

    record = table.create(fields)
    record_id = record.get("id", "")
    log.info("Written to Script Queue: %s", record_id)
    return record_id


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/generate", methods=["POST"])
def generate():
    payload = request.get_json(silent=True) or {}
    record_id = payload.get("record_id") or payload.get("recordId")
    base_id   = payload.get("base_id")   or payload.get("baseId") \
                or os.environ.get("AIRTABLE_BASE_ID")

    if not record_id:
        return jsonify({"error": "record_id is required"}), 400
    if not base_id:
        return jsonify({"error": "base_id is required (or set AIRTABLE_BASE_ID in .env)"}), 400

    log.info("Received generate request: record_id=%s", record_id)

    try:
        brand_data = _fetch_inbox_record(base_id, record_id)
    except Exception as exc:
        log.error("Failed to fetch Content Inbox record: %s", exc)
        return jsonify({"error": f"Could not fetch record: {exc}"}), 500

    try:
        result = generate_script(brand_data)
    except Exception as exc:
        log.error("Script generation failed: %s", exc)
        return jsonify({"error": f"Script generation failed: {exc}"}), 500

    try:
        queue_id = _write_to_script_queue(base_id, brand_data, result)
    except Exception as exc:
        log.error("Failed to write to Script Queue: %s", exc)
        return jsonify({"error": f"Could not write to Script Queue: {exc}"}), 500

    return jsonify({
        "ok": True,
        "script_queue_record_id": queue_id,
        "brand_name": brand_data.get("brand_name"),
        "entry_point": result["entry_point"],
        "villain": result["villain"],
    }), 200


if __name__ == "__main__":
    port = int(os.environ.get("WEBHOOK_PORT", 5000))
    log.info("Starting webhook server on port %d", port)
    log.info("POST http://localhost:%d/generate  {record_id, base_id}", port)
    app.run(host="0.0.0.0", port=port, debug=False)
