"""Dagster assets for production reports.

Each module in this package wraps a single report from `reporting/reports/<name>/`
in a Dagster asset and emits asset-materialization metadata that shows up in the
Dagster Cloud lineage UI.
"""
