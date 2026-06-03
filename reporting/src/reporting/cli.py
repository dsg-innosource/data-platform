"""CLI entry point: `python -m reporting.cli <routine-name> [flags]`.

See routines/_framework.md § 9 for the full surface.
"""

from __future__ import annotations

from datetime import date

import click


REGISTRY = {
    # Lazy import — populated by the report packages on first lookup
    "recruiter-activity": "reporting.reports.recruiter_activity:RecruiterActivityReport",
    # "position-status":  "reporting.reports.position_status:PositionStatusReport",
    # "terminations":     "reporting.reports.terminations:TerminationsReport",
}


@click.command()
@click.argument("routine_name", type=click.Choice(list(REGISTRY)))
@click.option("--date", "today", type=click.DateTime(formats=["%Y-%m-%d"]),
              default=None, help="Override today's date (YYYY-MM-DD)")
@click.option("--dry-run", is_flag=True, help="Render files but do not send email")
@click.option("--send-to", default=None, help="Comma-separated override recipients")
@click.option("--no-narrative", is_flag=True, help="Skip Anthropic narrative refinement")
@click.option("--window-only", is_flag=True, help="Print computed window and exit")
def main(routine_name, today, dry_run, send_to, no_narrative, window_only):
    """Run a production reporting routine."""
    raise NotImplementedError("Implement per routines/_framework.md § 9")


if __name__ == "__main__":
    main()
