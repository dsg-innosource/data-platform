"""Tests for framework.window — the Monday rule and edge cases.

Reference: routines/_framework.md § 4.
"""

from datetime import date

import pytest

from reporting.framework.window import report_window


@pytest.mark.skip("framework.window.report_window not yet implemented")
def test_monday_rule_picks_friday():
    w = report_window(date(2026, 4, 27))   # Monday
    assert w.is_monday_run is True
    assert w.report_date == date(2026, 4, 24)        # Friday
    assert w.window_start == date(2026, 4, 24)
    assert w.window_end == date(2026, 4, 26)         # Sunday


@pytest.mark.skip("framework.window.report_window not yet implemented")
def test_tuesday_picks_yesterday():
    w = report_window(date(2026, 4, 28))   # Tuesday
    assert w.is_monday_run is False
    assert w.report_date == date(2026, 4, 27)
    assert w.window_start == w.window_end == date(2026, 4, 27)


# TODO: edge cases — DST transitions, year boundary, leap day
