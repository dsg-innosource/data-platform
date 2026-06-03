"""Anthropic narrative refinement, with deterministic fallback.

`refine(callouts, data)` calls the Anthropic API to refine the wording of the
deterministic callouts. On any failure (timeout, error, schema validation), the
original `fallback_text` remains and the report goes out anyway.

Contract — see routines/_framework.md § 5:
  Input:  list of structured Callouts (with rule_id, tone, fallback_text, data)
  Output: same callouts with refined_text populated where successful

Env:
  ANTHROPIC_API_KEY    — required when narrative is enabled
  NARRATIVE_MODEL      — default "claude-sonnet-4-6"
  NARRATIVE_DISABLE    — set to "1" to skip refinement entirely
"""

from __future__ import annotations

from typing import Any

# import anthropic  # — uncomment when implementing

from .base import Callout


BRAND_VOICE = """\
You are writing one-paragraph insights for an InnoSource operational report. Style:
plain, direct business language. Lead with the insight. No filler phrases ("It's
worth noting that…", "Of particular interest…"). No metaphors. No exclamation
points. Numbers belong in the sentence, not parenthetically. 1–3 sentences per
callout, never more. The reader is an experienced operations leader who has 30
seconds — assume competence.

You are refining wording only. The rule that triggered each callout already decided
what to say; your job is how to say it. Do not invent new callouts. Do not remove
callouts. Do not change the tone classification.
"""


def refine(
    callouts: list[Callout],
    data: dict[str, Any],
    *,
    report_name: str,
    timeout_seconds: float = 30.0,
) -> tuple[list[Callout], bool]:
    """Refine the wording of `callouts` via Anthropic API. Mutates callouts in place.

    Returns (callouts, narrative_used) where narrative_used is True iff the API
    call succeeded and at least one callout received a refined_text value.

    Never raises. Falls back silently to fallback_text on any error.
    """
    raise NotImplementedError
