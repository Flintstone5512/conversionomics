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

Your tone: sharp, confident, specific. No filler. No generic advice. Every sentence earns its place. When you land an insight, spend 45–60 seconds on it — expand it, make it visceral, show the buyer's internal experience. Do not move on after one sentence.

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
Before writing a single word of the script, work through all 5 diagnostic layers for this brand:

Layer 1 — Observation: What exactly happened? What did the data show?
Layer 2 — Diagnosis: Why did it happen? What mechanic drove or killed the result?
Layer 3 — Psychology: What was the buyer thinking at each step? What did they feel, fear, or assume?
Layer 4 — System: Why does this pattern repeat across brands? What structural force causes it?
Layer 5 — Principle: What is the one transferable lesson a founder can carry into their own brand?

Also work through:
- Traffic Reality — who is actually watching vs. who the brand thinks is watching
- Friction & Anxiety Points — the #1 hidden barrier stopping a ready buyer from converting
- Micro-Decision Diagnosis — the 3–5 decisions made between "watch" and "buy"
- System-Level Fix — the single highest-leverage change that would 10x results

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
Write a complete YouTube script using the structure below. Label each section. Target 6–8 minutes of spoken content.

[ACT 1 — HOOK + EVIDENCE  |  0:00–2:00]
Open with the selected Entry Point. First sentence must be a pattern interrupt — something the viewer
did not expect. Then show the video: what brand, what content, what the numbers were.
Then show the evidence — what happened after the video? Comments, saves, views, bio-link behavior.
Make the viewer feel the gap: huge reach, unclear conversion. End Act 1 with a question that pulls
them into Act 2. ("So why didn't it convert? That's what we're here for.")

[ACT 2 — THE HIDDEN MECHANISM  |  2:00–5:00]
This is the analytical core. Apply the Focal Lens and the 5 diagnostic layers rigorously.
Surface 3–4 non-obvious insights. For each insight:
  • State it clearly in one sentence.
  • Then expand it for 45–60 seconds: show the buyer's internal experience, the specific moment
    momentum broke, the micro-decision that killed intent. Use the cold traffic principle where
    relevant: cold traffic doesn't investigate. It doesn't open tabs. It follows momentum.
    The moment momentum breaks, curiosity turns back into scrolling.
  • Ground every insight in a specific detail from the video (hook phrasing, comment text,
    CTA placement, bio link type). No generic observations.
Weave in the Villain without naming it yet — let it build.

[ACT 3 — THE LARGER PATTERN  |  5:00–7:30]
Zoom out. Name the Villain explicitly. Show why this same misdiagnosis happens repeatedly across
DTC brands — not because founders are careless, but because the system creates a blind spot.
Explain the structural force at work. Why does interest not equal intent? Why does reach not
equal revenue? Make it feel like a diagnosis the viewer recognizes from their own business.
Then present the System Fix: one specific, concrete change — what changes, what it does to the
funnel, and how you know. Be precise, not vague.

[CLOSE — BIG INSIGHT  |  7:30–8:00]
Land the Principle — the single transferable lesson from this teardown. Deliver the selected
Outcome. One sentence that reframes how the viewer will look at their own content from now on.
No generic CTAs. No "smash that subscribe button." End on the insight, not the ask.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTPUT FORMAT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Write in spoken English — natural, punchy, like a confident analyst on camera
- Total length: 1,200–1,600 words (6–8 minutes at a deliberate on-camera pace)
- Every key insight gets fully expanded — do not move on after one sentence
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
