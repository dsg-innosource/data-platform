"""Dagster asset for the daily recruiter-activity report.

Spec: data-platform/reporting/routines/recruiter-activity.md § 1, § 14
"""

from __future__ import annotations

from dagster import AssetExecutionContext, MetadataValue, RetryPolicy, asset

from reporting.reports.recruiter_activity import RecruiterActivityReport


@asset(
    group_name="daily_reports",
    description=(
        "Daily Recruiter Activity Report — emails morning summary to recruiting leads. "
        "Source: silver.v_applicant_activity_events. See reporting/routines/recruiter-activity.md."
    ),
    retry_policy=RetryPolicy(max_retries=2, delay=60),
    # deps=["v_applicant_activity_events"],   # uncomment once that asset exists
)
def recruiter_activity_report(context: AssetExecutionContext):
    """Materialize the daily recruiter-activity report (PDF + email)."""
    report = RecruiterActivityReport()
    result = report.run()

    context.add_output_metadata({
        "report_date":      MetadataValue.text(str(result.window.report_date)),
        "recipient_count":  MetadataValue.int(result.recipient_count),
        "narrative_used":   MetadataValue.bool(result.narrative_used),
        "duration_seconds": MetadataValue.float(result.duration_seconds),
        "pdf_size_bytes":   MetadataValue.int(result.pdf_path.stat().st_size),
        "pdf_path":         MetadataValue.path(result.pdf_path),
        "email_html_path":  MetadataValue.path(result.email_html_path),
        "spec":             MetadataValue.url(
            "https://github.com/dsg-innosource/data-platform/blob/main/"
            "reporting/routines/recruiter-activity.md"
        ),
    })
    return result
