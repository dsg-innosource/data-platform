"""Deterministic callout rules for the recruiter-activity report.

The rule table in routines/recruiter-activity.md § 7 is the authoritative spec.
This module is the executable form. Each rule is a small function that returns
a Callout (or None) given the report's structured data.
"""

from __future__ import annotations

from ...framework.base import Callout


# ── Section 02 / Insights ────────────────────────────────────────────────────

def _oar_callout(extended: int, accepted: int, oar: float | None) -> Callout | None:
    """OAR-related callout. At most one fires per run."""
    raise NotImplementedError


def _s2i_callout(phone_screens: int, inno_interviews: int, s2i: float | None) -> Callout | None:
    """Screen-to-inno-related callout. At most one fires per run."""
    raise NotImplementedError


# ── Section 03 / Recruiter table ─────────────────────────────────────────────

def _recruiter_callouts(recruiter_rows: list[dict]) -> list[Callout]:
    """Up to two callouts: zero-screens warning and closer recognition."""
    raise NotImplementedError


def derive_all(kpis: dict, recruiter_rows: list[dict]) -> list[Callout]:
    """Top-level entry — produces all callouts for the report."""
    raise NotImplementedError
