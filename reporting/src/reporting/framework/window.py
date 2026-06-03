"""Report date-window resolution.

Implements the Monday rule: on Monday the report covers Friday with a query window
extending through Sunday; on Tue–Fri it covers the prior calendar day.

Weekly reports (e.g. terminations) override `report_window()` on their Report
subclass to return ISO-week-aligned windows instead.

See routines/_framework.md § 4.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class Window:
    today: date
    report_date: date
    window_start: date
    window_end: date
    label_long: str
    label_short: str
    is_monday_run: bool


def report_window(today: date) -> Window:
    """Resolve the daily report window for `today`. Applies the Monday rule.

    >>> report_window(date(2026, 4, 27)).is_monday_run  # Monday
    True
    >>> report_window(date(2026, 4, 28)).is_monday_run  # Tuesday
    False
    """
    raise NotImplementedError("Implement per routines/_framework.md § 4")
