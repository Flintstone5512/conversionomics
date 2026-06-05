"""
Viral teardown script generator using ChatGPT (gpt-4o).

Randomly selects one option from each rotation layer (Entry Point, Villain,
Focal Lens, Role, Outcome), then builds a master prompt and streams a complete
YouTube teardown script via the OpenAI SDK.
"""

import random
import logging
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

def _build_system_prompt() -> str:
    return """You write YouTube teardown scripts for a DTC conversion analyst channel.

RULES — follow every one, no exceptions:
1. Output ONLY the spoken script. No preamble ("Certainly!", "Here's a script..."), no section labels, no timestamps, no meta-commentary ("here's where we...", "now let's look at..."). First word out is the first spoken word of the video.
2. Every specific fact, number, observation, and detail in the script must come directly from the analyst notes. Do not invent, substitute, or generalize. If the notes say there is no bio link — discuss the absence of a bio link. If the notes give an exact number — use that exact number. If the notes mention AI-generated content — that goes in the script.
3. This is a breakdown of the SPECIFIC video described in the notes, not a generic teardown template. The viewer should be able to follow along watching that exact video.
4. Tone: casual and direct. Like a sharp friend who knows more about conversion than anyone in the room. Not formal, not corporate. Short punchy sentences. Expand when an idea needs space. Confident but never stiff.
5. Structure (follow naturally, do not label or announce):
   - Open with a pattern interrupt. Walk through the video and its exact numbers. Build the gap between reach and revenue.
   - Dig into 3–4 non-obvious insights pulled from the notes. Each gets fully expanded — state it, then spend 45–60 seconds on the buyer's internal experience, where momentum broke, what cold traffic actually does (it doesn't investigate, doesn't open tabs, follows momentum — the moment it breaks, they're scrolling again).
   - Zoom out to the larger pattern. Name the villain naturally in conversation. One concrete system fix.
   - Close with the single transferable principle. End on the insight, not an ask.
6. Length: 1,200–1,600 words."""


def _build_user_prompt(brand_data: dict, rotation: dict) -> str:
    brand    = brand_data.get("brand_name", "Unknown Brand")
    src_link = brand_data.get("source_link", "")
    notes    = brand_data.get("notes", "").strip()

    if not notes:
        notes = "(No analyst notes provided.)"

    return f"""Write a YouTube teardown script for this video using ONLY the details in the notes below.

Video: {src_link}
Brand: {brand}

ANALYST NOTES (your only source material — every point in these notes must appear in the script):
{notes}

Rotation variables (shape the angle and voice — do not state these out loud):
Entry Point: {rotation['entry_point']}
Villain: {rotation['villain']}
Focal Lens: {rotation['focal_lens']}
Role: {rotation['role']}
Outcome: {rotation['outcome']}

Write the script now. Start with the first spoken word."""


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

    system_prompt = _build_system_prompt()
    user_prompt   = _build_user_prompt(brand_data, rotation)
    client = OpenAI()

    notes_preview = (brand_data.get("notes") or "")[:120] or "EMPTY"
    log.info(
        "Generating script for '%s' | Entry: %s... | Villain: %s... | Notes: %s",
        brand_data.get("brand_name", "?"),
        rotation["entry_point"][:40],
        rotation["villain"][:40],
        notes_preview,
    )

    full_text = ""
    with client.chat.completions.create(
        model="gpt-4o",
        max_tokens=4000,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
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
