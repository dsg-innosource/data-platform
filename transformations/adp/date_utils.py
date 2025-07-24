"""
Date utilities for ADP pipeline.
"""
from datetime import datetime, timedelta
from typing import Tuple


def get_monday_dates(target_date: datetime = None) -> Tuple[str, str]:
    """
    Calculate snapshot_date (current Monday) and report_date (previous Monday).
    
    This implements the business logic where:
    - The pipeline runs on Monday
    - snapshot_date = the Monday when the pipeline runs  
    - report_date = the previous Monday (7 days before snapshot_date)
    
    Args:
        target_date: Date to calculate from. If None, uses today.
        
    Returns:
        Tuple of (snapshot_date, report_date) as YYYY-MM-DD strings
        
    Examples:
        >>> get_monday_dates(datetime(2024, 1, 17))  # Wednesday
        ('2024-01-15', '2024-01-08')  # Current Monday, Previous Monday
        
        >>> get_monday_dates(datetime(2024, 1, 15))  # Monday  
        ('2024-01-15', '2024-01-08')  # Same Monday, Previous Monday
    """
    if target_date is None:
        target_date = datetime.now()
    
    # Find the Monday of the current week
    # Monday is 0 in weekday()
    days_since_monday = target_date.weekday()
    current_monday = target_date - timedelta(days=days_since_monday)
    
    # Previous Monday is 7 days before current Monday
    previous_monday = current_monday - timedelta(days=7)
    
    snapshot_date = current_monday.strftime('%Y-%m-%d')
    report_date = previous_monday.strftime('%Y-%m-%d')
    
    return snapshot_date, report_date


def is_monday(date: datetime = None) -> bool:
    """
    Check if the given date (or today) is a Monday.
    
    Args:
        date: Date to check. If None, uses today.
        
    Returns:
        True if the date is a Monday
    """
    if date is None:
        date = datetime.now()
    
    return date.weekday() == 0


def get_next_monday(date: datetime = None) -> str:
    """
    Get the next Monday from the given date.
    
    Args:
        date: Starting date. If None, uses today.
        
    Returns:
        Next Monday as YYYY-MM-DD string
    """
    if date is None:
        date = datetime.now()
    
    days_until_monday = (7 - date.weekday()) % 7
    if days_until_monday == 0 and date.weekday() == 0:
        # If today is Monday, return today
        next_monday = date
    else:
        # Otherwise, find next Monday
        if days_until_monday == 0:
            days_until_monday = 7
        next_monday = date + timedelta(days=days_until_monday)
    
    return next_monday.strftime('%Y-%m-%d')


def get_week_range(monday_date: str) -> Tuple[str, str]:
    """
    Get the start and end dates for a week given a Monday.
    
    Args:
        monday_date: Monday date as YYYY-MM-DD string
        
    Returns:
        Tuple of (start_date, end_date) as YYYY-MM-DD strings
        start_date is the Monday, end_date is the Sunday
    """
    monday = datetime.strptime(monday_date, '%Y-%m-%d')
    
    if monday.weekday() != 0:
        raise ValueError(f"Date {monday_date} is not a Monday")
    
    sunday = monday + timedelta(days=6)
    
    return monday_date, sunday.strftime('%Y-%m-%d')


def format_business_period(snapshot_date: str, report_date: str) -> str:
    """
    Format the business period for reporting.
    
    Args:
        snapshot_date: Snapshot Monday date
        report_date: Report Monday date
        
    Returns:
        Formatted business period string
        
    Example:
        >>> format_business_period('2024-01-15', '2024-01-08')
        'Week of 2024-01-08 through 2024-01-14 (reported 2024-01-15)'
    """
    report_monday = datetime.strptime(report_date, '%Y-%m-%d')
    report_sunday = report_monday + timedelta(days=6)
    
    return (
        f"Week of {report_date} through {report_sunday.strftime('%Y-%m-%d')} "
        f"(reported {snapshot_date})"
    )


def validate_monday_dates(snapshot_date: str, report_date: str) -> bool:
    """
    Validate that snapshot and report dates follow business rules.
    
    Args:
        snapshot_date: Snapshot date as YYYY-MM-DD string
        report_date: Report date as YYYY-MM-DD string
        
    Returns:
        True if dates are valid
        
    Raises:
        ValueError: If dates don't follow business rules
    """
    snapshot = datetime.strptime(snapshot_date, '%Y-%m-%d')
    report = datetime.strptime(report_date, '%Y-%m-%d')
    
    # Both should be Mondays
    if snapshot.weekday() != 0:
        raise ValueError(f"Snapshot date {snapshot_date} is not a Monday")
    
    if report.weekday() != 0:
        raise ValueError(f"Report date {report_date} is not a Monday")
    
    # Report date should be before snapshot date
    if report >= snapshot:
        raise ValueError(f"Report date {report_date} should be before snapshot date {snapshot_date}")
    
    # Should be exactly 7 days apart (1 week)
    days_diff = (snapshot - report).days
    if days_diff != 7:
        raise ValueError(
            f"Report and snapshot dates should be exactly 7 days apart, "
            f"but got {days_diff} days between {report_date} and {snapshot_date}"
        )
    
    return True