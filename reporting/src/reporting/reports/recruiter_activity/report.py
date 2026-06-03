"""Daily Recruiter Activity Report.

Spec: reporting/routines/recruiter-activity.md
Source view: silver.v_applicant_activity_events
"""

from __future__ import annotations

from pathlib import Path

from ...framework.base import Callout, Report, ReportData
from ...framework.window import Window


class RecruiterActivityReport(Report):
    routine_name = "recruiter-activity"
    recipients_section_name = "recruiter-activity-report"
    output_basename = "activity"

    # File-prefix override matches the existing convention
    # (activity-2026-04-23.html, not recruiter-activity-2026-04-23.html).

    def fetch(self, conn, window: Window) -> ReportData:
        """Run Section 02 totals query, Section 03 per-recruiter query, and the
        Monday-only weekend probe. Compute derived OAR and screen-to-inno rates.

        See queries.py for the SQL.
        """
        raise NotImplementedError

    def derive_callouts(self, data: ReportData) -> list[Callout]:
        """Apply the deterministic rule table from spec § 7. See callouts.py."""
        raise NotImplementedError

    def render_pdf_html(self, data: ReportData, callouts: list[Callout]) -> str:
        """Render templates/pdf.html.j2 with the standard context."""
        raise NotImplementedError

    def render_email_html(self, data: ReportData, callouts: list[Callout]) -> str:
        """Render templates/email.html.j2 with the standard context."""
        raise NotImplementedError
