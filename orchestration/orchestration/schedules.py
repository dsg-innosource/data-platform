"""Dagster schedules for production reports.

Cron-style schedules that trigger asset materialization. All times are in
America/New_York; Dagster handles DST.
"""

from __future__ import annotations

from dagster import ScheduleDefinition, define_asset_job

from .assets.reports.recruiter_activity import recruiter_activity_report


# ── Daily recruiter activity — Mon–Fri 7:00 AM ET ────────────────────────────
recruiter_activity_schedule = ScheduleDefinition(
    job=define_asset_job(
        "recruiter_activity_job",
        selection=[recruiter_activity_report],
    ),
    cron_schedule="0 7 * * 1-5",
    execution_timezone="America/New_York",
    description="Daily recruiter activity report — runs Mon–Fri at 7am ET.",
)
