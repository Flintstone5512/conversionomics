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

def _build_prompt(brand_data: dict, rotation: dict) -> str:
    brand    = brand_data.get("brand_name", "Unknown Brand")
    src_link = brand_data.get("source_link", "")
    notes    = brand_data.get("notes", "").strip()

    if not notes:
        notes = "(No analyst notes provided — infer from source link and brand name only.)"

    return f"""You are writing a YouTube script for a DTC conversion analyst channel. Output ONLY the spoken script — no preamble, no sign-off, no section labels, no meta-commentary. Start with the first spoken word and end with the last spoken word.

TONE: Casual and direct, like a smart friend who happens to know more about conversion than anyone in the room. Not formal. Not corporate. Confident but conversational — short punchy sentences mixed with longer explanations when an idea needs space.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VIDEO REFERENCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Brand: {brand}
Source Video: {src_link}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ANALYST NOTES — YOUR ONLY SOURCE MATERIAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Everything in the script must come directly from these notes.
Do not invent details. Do not substitute generic observations.
If a note says there is no bio link, the script must address the absence of a bio link — not invent one.
Cite the specific numbers from the notes. Use the analyst's exact observations as the raw material.

{notes}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PRE-WRITE (internal only — do not output this)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Before writing, silently work through the 5 layers using ONLY what's in the notes above:
Layer 1 — Observation: What exactly happened per the notes?
Layer 2 — Diagnosis: Why did it happen?
Layer 3 — Psychology: What was the buyer thinking at each step?
Layer 4 — System: Why does this pattern repeat across brands?
Layer 5 — Principle: The one transferable lesson.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCRIPT ROTATION (shapes angle and voice — do not state these out loud)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Entry Point:  {rotation['entry_point']}
Villain:      {rotation['villain']}
Focal Lens:   {rotation['focal_lens']}
Role:         {rotation['role']}
Outcome:      {rotation['outcome']}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCRIPT STRUCTURE (internal guide — do not label or announce sections)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Follow this arc naturally without labeling it:

Act 1 (0:00–2:00) — Hook + Evidence
Open with the Entry Point approach. Pattern interrupt in the first sentence.
Walk through what happened — the content, the numbers from the notes, what engagement looked like.
Build the gap between reach and revenue without naming it yet.
End with a question that pulls into Act 2.

Act 2 (2:00–5:00) — The Hidden Mechanism
Surface 3–4 non-obvious insights directly from the notes.
Each insight: one clear sentence to state it, then 45–60 seconds to expand it — show the buyer's internal experience, the moment momentum broke. Cold traffic doesn't investigate. It doesn't open tabs. It follows momentum. The moment it breaks, curiosity turns back into scrolling.
Build toward the villain without naming it.

Act 3 (5:00–7:30) — The Larger Pattern
Zoom out. Name the villain naturally in conversation — not as an announcement, just as the word that finally fits what you've been describing. Show the structural blind spot. System fix: one specific concrete change, what it does to the funnel, and why you know it works.

Close (7:30–8:00)
The single transferable principle. One sentence that reframes how the viewer sees their own content going forward. No generic CTAs. End on the insight.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTPUT RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Output ONLY the spoken script. Nothing else.
- No section headers, no labels, no timestamps
- No "here's where we..." or "now let's look at..." meta-commentary
- No invented details — only what's in the analyst notes
- 1,200–1,600 words. Casual but sharp. Every sentence earns its place.
"""


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

    prompt = _build_prompt(brand_data, rotation)
    client = OpenAI()

    log.info(
        "Generating script for '%s' | Entry: %s... | Villain: %s...",
        brand_data.get("brand_name", "?"),
        rotation["entry_point"][:40],
        rotation["villain"][:40],
    )

    full_text = ""
    with client.chat.completions.create(
        model="gpt-4o",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
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
