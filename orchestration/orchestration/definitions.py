"""Top-level Dagster Definitions.

This is the entry point Dagster Cloud loads via the `tool.dagster.module_name`
field in pyproject.toml. It collects every asset, schedule, sensor, and resource
the project exposes and hands them to Dagster.

For now: just the reports group. Other groups (transforms, ELT sensors, etc.)
will be added as the data-platform repo migrates more workloads to Dagster.
"""

from __future__ import annotations

from dagster import Definitions

from .assets.reports import recruiter_activity
from .schedules import recruiter_activity_schedule
from .resources import resources_for_env


defs = Definitions(
    assets=[
        recruiter_activity.recruiter_activity_report,
        # position_status.position_status_report,    # planned
        # terminations.terminations_report,          # planned
    ],
    schedules=[
        recruiter_activity_schedule,
        # position_status_schedule,
        # terminations_schedule,
    ],
    resources=resources_for_env(),
)
