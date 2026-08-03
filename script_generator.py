"""
Viral teardown script generator using ChatGPT (gpt-4o).

Randomly selects one option from each rotation layer (Entry Point, Villain,
Focal Lens, Role, Outcome), then builds a master prompt and streams a complete
YouTube teardown script via the OpenAI SDK.

Assessment data schema (all keys optional — omit any you don't have):
  assessment: {
    # Organic Revenue Summary
    "organic_revenue_score": int,          # e.g. 36  (out of 100)
    "score_label": str,                    # e.g. "Critical"
    "revenue_opportunity_lost": float,     # e.g. 30724.0
    "recovery_potential": float,           # e.g. 23043.0
    "buying_intent_comments": int,         # e.g. 1617
    "confidence_level": int,               # e.g. 65  (percent)

    # Social Performance
    "total_views": int,
    "shares": int,
    "saves": int,
    "total_comments": int,
    "profile_visits": int,

    # Buyer Intent Comment breakdown (counts per category)
    "intent_how_to_buy": int,              # Weight 12
    "intent_price": int,                   # Weight 10
    "intent_available": int,               # Weight 8
    "intent_coupon": int,                  # Weight 7
    "intent_shipping": int,                # Weight 6
    "intent_restock": int,                 # Weight 5
    "intent_ordered": int,                 # Weight 10
    "intent_need_want": int,               # Weight 5

    # Top Revenue Leaks (list of dicts with "issue" and "lift")
    "revenue_leaks": [
      {"issue": "No website or product page", "lift": 14149},
      ...
    ],
  }
"""

import base64
import os
import random
import logging
import requests
from openai import OpenAI

log = logging.getLogger(__name__)

# ── Rotation layers (from ConversionPrompt framework) ───────────────────────

ENTRY_POINTS = [
    "Observation Hook — open with a specific detail noticed in the video that most viewers missed",
    "Question Hook — open with a provocative question that frames the entire teardown",
    "Myth-Bust Hook — open by stating a belief 90% of the audience holds, then immediately challenging it",
    "Data Hook — open with a specific number, stat, or result from the brand/video",
    "Story Hook — open with a 2-sentence scene that drops the viewer into the moment",
]

VILLAINS = [
    "The Platform Algorithm — TikTok/Instagram rewards performance signals the brand accidentally triggered",
    "The Confused Founder — owner doesn't understand WHY their content worked",
    "The Copycat Trap — everyone is copying the surface-level execution, missing the real mechanic",
    "The Optimization Myth — brand is optimizing for the wrong metric (views, not conversions)",
    "The Trust Gap — the content works but the funnel after the click destroys the conversion",
    "The Invisible Friction — the buying process has a hidden step that kills 40–60% of intent",
    "The Wrong Audience Magnet — viral content attracts browsers, not buyers",
    "The Niche Blindspot — brand is leaving a specific buyer segment completely unaddressed",
]

FOCAL_LENSES = [
    "Traffic Reality — what the signal data reveals about who is actually watching vs. who the brand thinks is watching",
    "Intent Signals — which comments, shares, and save behaviors reveal purchase-ready buyers",
    "Belief Alignment — which core belief about themselves does this product let viewers confirm",
    "Friction & Anxiety Points — what is the #1 thing stopping a ready buyer from clicking 'add to cart'",
    "Micro-Decision Diagnosis — the 3–5 micro-decisions a viewer makes between 'watch' and 'buy'",
    "Likely Outcome Mapping — what transformation does the buyer actually want, vs. what the brand shows",
    "System-Level Fix — if you could change ONE thing in their funnel, what would 10x conversions",
    "Hook Anatomy — break down exactly why the first 3 seconds worked on a neurological level",
]

ROLES = [
    "Conversion Strategist — speaking to a brand owner who wants to turn viral into revenue",
    "Media Buyer — speaking to someone who will scale this content with paid spend",
    "Content Director — speaking to a creator or agency deciding how to replicate the format",
    "Founder Advisor — speaking directly to the founder of the brand in the video",
    "DTC Analyst — speaking to an investor or operator analyzing the brand's growth signal",
]

OUTCOMES = [
    "The viewer walks away with ONE specific change they can make to their own brand this week",
    "The viewer books a call / reaches out to hire help executing the insight",
    "The viewer shares the video because it explains something they've seen but couldn't articulate",
    "The viewer sees the brand in the video as a case study for their own situation",
    "The viewer forms a belief that the host understands conversion better than anyone else they follow",
]


# ── Prompt builder ───────────────────────────────────────────────────────────

def _build_system_prompt(has_assessment: bool = False) -> str:
    assessment_rules = ""
    if has_assessment:
        assessment_rules = """
ASSESSMENT OPENING (mandatory — follow exactly):
The assessment data comes from either the structured block labeled ORGANIC REVENUE ASSESSMENT below, or from the screenshot images attached to this message, or both. Read all sources and use the exact numbers you find — do not round or estimate.
A. The very first sentence must be a hook built from the assessment numbers — revenue lost and recovery potential. Model: "This store just missed $X in revenue from a single post. And they could claw back $Y with a few changes that take less than a day to make." Use the exact dollar figures from the assessment. Make it land like a gut punch. No warm-up, no preamble.
B. Immediately after the hook, walk through the Organic Revenue Score as if you're reading a report card out loud. State the score, state what it means ("that's Critical territory"), then state the confidence level. Keep it to 2–3 sentences.
C. Next, walk through the Top Revenue Leaks in descending order by dollar lift. Read each one like items on an autopsy report — specific, matter-of-fact, slightly damning. Each leak gets one tight sentence: what the problem is and what it costs them. Do NOT editorialize yet.
D. After the leaks, pivot to the buying-intent comment data. Frame it as proof: "Meanwhile, X people left comments showing clear purchase intent. Y people asked where to buy. Z wanted to know the price." Let the numbers indict the funnel silently.
E. Transition naturally into the rest of the teardown — something like "So let's actually break down why this is happening and what they'd need to change." This should feel like flipping from the scorecard to the film room.
F. The assessment section should run 200–300 words. Punchy, not padded.
"""

    return f"""You write YouTube teardown scripts for a DTC conversion analyst channel.

RULES — follow every one, no exceptions:
1. Output ONLY the spoken script. No preamble ("Certainly!", "Here's a script..."), no section labels, no timestamps, no meta-commentary ("here's where we...", "now let's look at..."). First word out is the first spoken word of the video.
2. Every specific fact, number, observation, and detail in the script must come directly from the analyst notes and assessment data. Do not invent, substitute, or generalize. If the notes say there is no bio link — discuss the absence of a bio link. If the notes give an exact number — use that exact number. If the notes mention AI-generated content — that goes in the script.
3. This is a breakdown of the SPECIFIC video described in the notes, not a generic teardown template. The viewer should be able to follow along watching that exact video.
4. Tone: casual and direct. Like a sharp friend who knows more about conversion than anyone in the room. Not formal, not corporate. Short punchy sentences. Expand when an idea needs space. Confident but never stiff.
5. Structure (follow naturally, do not label or announce):{assessment_rules}
   - {"After the assessment section: open" if has_assessment else "Open"} with a pattern interrupt. Walk through the video and its exact numbers. Build the gap between reach and revenue.
   - Immediately after the intro, drop in a story, anecdote, or piece of evidence that grounds the analysis. This can be: a personal story or moment the host experienced, a quick anecdote about a brand that got this wrong or right, a relevant study or data point (psychology, consumer behavior, conversion research), or a concrete real-world parallel that DTC store owners will instantly recognize. It should feel like a natural pivot — "and here's why that matters…" or "I've seen this exact thing before…" — not a labeled section. Keep it tight: 3–5 sentences. It must reinforce the core insight of the teardown, not just be interesting for its own sake.
   - Dig into 3–4 non-obvious insights pulled from the notes. Each gets fully expanded — state it, then spend 45–60 seconds on the buyer's internal experience, where momentum broke, what cold traffic actually does (it doesn't investigate, doesn't open tabs, follows momentum — the moment it breaks, they're scrolling again).
   - Zoom out to the larger pattern. Name the villain naturally in conversation. One concrete system fix.
   - Close with the single transferable principle. End on the insight, not an ask.
6. Length: 1,200–1,600 words."""


def _format_assessment_block(a: dict) -> str:
    """Render the assessment dict into a structured text block for the prompt."""
    lines = ["ORGANIC REVENUE ASSESSMENT (use every number that appears here):"]

    score = a.get("organic_revenue_score")
    label = a.get("score_label", "")
    lost  = a.get("revenue_opportunity_lost")
    recov = a.get("recovery_potential")
    conf  = a.get("confidence_level")
    bic   = a.get("buying_intent_comments")

    if score is not None:
        lines.append(f"  Organic Revenue Score: {score}/100 — {label}")
    if lost is not None:
        lines.append(f"  Revenue Opportunity Lost: ${lost:,.0f}")
    if recov is not None:
        lines.append(f"  Recovery Potential: +${recov:,.0f}")
    if bic is not None:
        lines.append(f"  Buying Intent Comments: {bic:,}")
    if conf is not None:
        lines.append(f"  Confidence Level: {conf}%")

    social_keys = [
        ("total_views", "Total Views"),
        ("shares", "Shares"),
        ("saves", "Saves"),
        ("total_comments", "Total Comments"),
        ("profile_visits", "Profile Visits"),
    ]
    social_vals = [(label, a[k]) for k, label in social_keys if k in a]
    if social_vals:
        lines.append("  Social Performance: " + " | ".join(f"{l}: {v:,}" for l, v in social_vals))

    intent_map = [
        ("intent_how_to_buy",  '"How do I buy?" / "Where can I get it?"',  12),
        ("intent_price",       '"Price?" / "How much?"',                   10),
        ("intent_available",   '"Available?" / "In stock?"',                8),
        ("intent_coupon",      '"Coupon?" / "Discount?"',                   7),
        ("intent_shipping",    '"Shipping?" / "Does it ship to..."',         6),
        ("intent_restock",     '"Restock?" / "When is it back?"',            5),
        ("intent_ordered",     '"Ordered!" / "Just bought!"',               10),
        ("intent_need_want",   '"Need this" / "Want" / "Link?"',             5),
    ]
    intent_lines = [
        f"    {label} (weight {w}): {a[k]:,}"
        for k, label, w in intent_map if k in a
    ]
    if intent_lines:
        lines.append("  Buyer Intent Comments:")
        lines.extend(intent_lines)

    leaks = a.get("revenue_leaks", [])
    if leaks:
        lines.append("  Top Revenue Leaks (ranked by estimated lift):")
        for leak in leaks:
            issue = leak.get("issue", "")
            lift  = leak.get("lift", 0)
            lines.append(f"    +${lift:,} — {issue}")

    return "\n".join(lines)


def _build_user_prompt(brand_data: dict, rotation: dict) -> str:
    brand      = brand_data.get("brand_name", "Unknown Brand")
    src_link   = brand_data.get("source_link", "")
    notes      = brand_data.get("notes", "").strip()
    assessment = brand_data.get("assessment")

    if not notes:
        notes = "(No analyst notes provided.)"

    assessment_block = ""
    if assessment:
        assessment_block = f"""
{_format_assessment_block(assessment)}

"""

    return f"""Write a YouTube teardown script for this video using ONLY the details in the notes and assessment data below.

Video: {src_link}
Brand: {brand}
{assessment_block}
ANALYST NOTES (your only source material — every point in these notes must appear in the script):
{notes}

Rotation variables (shape the angle and voice — do not state these out loud):
Entry Point: {rotation['entry_point']}
Villain: {rotation['villain']}
Focal Lens: {rotation['focal_lens']}
Role: {rotation['role']}
Outcome: {rotation['outcome']}

Write the script now. Start with the first spoken word."""


# ── Image fetcher ────────────────────────────────────────────────────────────

def _fetch_images_as_base64(urls: list[str]) -> list[str]:
    """Download Airtable attachment URLs and return base64-encoded strings."""
    api_key = os.environ.get("AIRTABLE_API_KEY", "")
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    results = []
    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            results.append(base64.b64encode(resp.content).decode("utf-8"))
            log.info("Fetched screenshot from %s (%d bytes)", url[:60], len(resp.content))
        except Exception as exc:
            log.warning("Could not fetch screenshot %s: %s", url[:60], exc)
    return results


def _build_vision_user_message(text: str, image_b64_list: list[str]) -> list[dict]:
    """Build a GPT-4o vision content array: text first, then each image."""
    content: list[dict] = [{"type": "text", "text": text}]
    for b64 in image_b64_list:
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64}", "detail": "high"},
        })
    return content


# ── Main generator ────────────────────────────────────────────────────────────

def generate_script(brand_data: dict) -> dict:
    """
    Generate a teardown script for the given brand_data dict.
    Returns a dict with the script text and the rotation variables used.
    """
    rotation = {
        "entry_point": random.choice(ENTRY_POINTS),
        "villain":     random.choice(VILLAINS),
        "focal_lens":  random.choice(FOCAL_LENSES),
        "role":        random.choice(ROLES),
        "outcome":     random.choice(OUTCOMES),
    }

    screenshot_urls = brand_data.get("screenshot_urls") or []
    image_b64_list  = _fetch_images_as_base64(screenshot_urls) if screenshot_urls else []

    has_assessment = bool(brand_data.get("assessment") or image_b64_list)
    system_prompt  = _build_system_prompt(has_assessment)
    user_prompt    = _build_user_prompt(brand_data, rotation)
    client = OpenAI()

    notes_preview = (brand_data.get("notes") or "")[:120] or "EMPTY"
    log.info(
        "Generating script for '%s' | Entry: %s... | Villain: %s... | Screenshots: %d | Notes: %s",
        brand_data.get("brand_name", "?"),
        rotation["entry_point"][:40],
        rotation["villain"][:40],
        len(image_b64_list),
        notes_preview,
    )

    # Build user message — plain text or vision content depending on screenshots
    user_content = (
        _build_vision_user_message(user_prompt, image_b64_list)
        if image_b64_list
        else user_prompt
    )

    full_text = ""
    with client.chat.completions.create(
        model="gpt-4o",
        max_tokens=4000,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_content},
        ],
        stream=True,
    ) as stream:
        for chunk in stream:
            delta = chunk.choices[0].delta.content
            if delta:
                full_text += delta

    log.info("Script generated (%d chars)", len(full_text))

    return {
        "script_text": full_text,
        "entry_point": rotation["entry_point"],
        "villain":     rotation["villain"],
        "focal_lens":  rotation["focal_lens"],
        "role":        rotation["role"],
        "outcome":     rotation["outcome"],
    }
