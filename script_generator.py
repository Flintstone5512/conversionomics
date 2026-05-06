"""
Viral teardown script generator using Claude claude-opus-4-7.

Randomly selects one option from each rotation layer (Entry Point, Villain,
Focal Lens, Role, Outcome), then builds a master prompt and streams a complete
YouTube teardown script via the Anthropic SDK.
"""

import random
import logging
from anthropic import Anthropic

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
    brand     = brand_data.get("brand_name", "Unknown Brand")
    product   = brand_data.get("product_name", "")
    src_link  = brand_data.get("source_link", "")
    raw_title = brand_data.get("raw_title", "")
    revenue   = brand_data.get("revenue_signal", "")
    views     = brand_data.get("play_count", 0)
    hook      = brand_data.get("video_hook", "")
    fmt       = brand_data.get("video_format", "")
    comments  = brand_data.get("comment_observations", "")
    cta       = brand_data.get("cta_present", "")
    bio_link  = brand_data.get("bio_link_destination", "")
    price     = brand_data.get("price_point", "")
    category  = brand_data.get("product_category", "")

    product_block = f"\nProduct: {product}" if product else ""
    views_fmt     = f"{views:,}" if isinstance(views, int) else str(views)

    return f"""You are a viral DTC conversion analyst who creates YouTube teardown scripts.

Your audience is ecommerce brand owners, media buyers, and DTC founders who want to understand WHY certain content converts — not just why it goes viral. You apply direct-response principles to organic content. You never sell; you shift beliefs and surface non-obvious insights.

Your tone: sharp, confident, specific. No filler. No generic advice. Every sentence earns its place.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BRAND / VIDEO CONTEXT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Brand: {brand}{product_block}
Category: {category or "Unknown"}
Price Point: {price or "Unknown"}
Revenue Signal: {revenue or "Unknown"}
View Count: {views_fmt}
Source Video: {src_link}
Raw Title / Caption: {raw_title}

Video Hook (first 3 seconds): {hook or "Not provided"}
Video Format: {fmt or "Not provided"}
Comment Observations: {comments or "Not provided"}
CTA Present: {cta or "Not provided"}
Bio Link Destination: {bio_link or "Not provided"}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HIDDEN CONSTANT ANALYSIS
(Do this internally before writing — do not show this section in output)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Before writing a single word of the script, silently work through all 7 points:
1. Traffic Reality — who is actually watching this content vs. who the brand thinks is watching
2. Intent Signals — which engagement behaviors (saves, comments, shares) reveal purchase-ready viewers
3. Belief Alignment — what core belief about themselves does this product let the buyer confirm
4. Friction & Anxiety Points — what is the #1 hidden barrier stopping a ready buyer from converting
5. Micro-Decision Diagnosis — the 3–5 micro-decisions made between "watch" and "buy"
6. Likely Outcome — the real transformation the buyer wants vs. what the brand communicates
7. System-Level Fix — the single highest-leverage change in funnel or content that would 10x results

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCRIPT ROTATION VARIABLES (selected for this episode)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Entry Point:  {rotation['entry_point']}
Villain:      {rotation['villain']}
Focal Lens:   {rotation['focal_lens']}
Role:         {rotation['role']}
Outcome:      {rotation['outcome']}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCRIPT STRUCTURE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Write a complete YouTube script with the following sections. Label each section clearly.

[HOOK — 0:00–0:20]
Use the selected Entry Point approach. Must create a pattern interrupt in the first sentence.
Target: viewer says "wait, I didn't think about it that way."

[SETUP — 0:20–1:00]
Introduce the brand and video being analyzed. Establish WHY this specific video is worth 8 minutes
of anyone's time. Reference the Villain without naming it yet.

[THE BREAKDOWN — 1:00–5:30]
This is the core of the episode. Apply the Focal Lens rigorously. Surface 3–5 non-obvious insights.
Each insight must be specific (cite actual elements: exact timestamp, specific comment phrasing,
bio link type, hook structure). No generic statements like "they used social proof well."

[THE VILLAIN REVEAL — 5:30–6:30]
Name the Villain explicitly. Show how it explains something the viewer has been confused about
in their own business. Make it feel like a diagnosis, not a lecture.

[THE SYSTEM FIX — 6:30–7:30]
Present one specific, actionable change the brand (or the viewer's own brand) could make.
Be concrete: what exactly would change, what would that do to the funnel, and how do you know.

[CLOSE — 7:30–8:00]
Deliver the selected Outcome. End with a single sentence that makes the viewer want to come back
for the next teardown. No generic CTAs. No "smash that subscribe button."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTPUT FORMAT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Write in spoken English — natural, punchy, like a confident analyst on camera
- Total length: 900–1,200 words (tight enough to hold attention, deep enough to deliver value)
- Do not include stage directions, camera cues, or B-roll notes
- Do not include the Hidden Constant section in the output
- Label each section with the bracket headers above
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
    client = Anthropic()

    log.info(
        "Generating script for '%s' | Entry: %s... | Villain: %s...",
        brand_data.get("brand_name", "?"),
        rotation["entry_point"][:40],
        rotation["villain"][:40],
    )

    full_text = ""
    with client.messages.stream(
        model="claude-opus-4-7",
        max_tokens=2000,
        thinking={"type": "adaptive"},
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        for text in stream.text_stream:
            full_text += text

    log.info("Script generated (%d chars)", len(full_text))

    return {
        "script_text": full_text,
        "entry_point": rotation["entry_point"],
        "villain":     rotation["villain"],
        "focal_lens":  rotation["focal_lens"],
        "role":        rotation["role"],
        "outcome":     rotation["outcome"],
    }
