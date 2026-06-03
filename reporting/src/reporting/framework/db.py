"""Postgres connection helper.

Reads connection settings from the existing `shared/database.yaml` (which uses
${DB_HOST}, ${DB_PORT}, etc. env-var substitution) so reporting inherits the same
connection pattern as the existing data-platform processes.

In production (Dagster Cloud) the DB env vars come from Dagster Cloud secrets.
Locally they come from `.env`.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

# import psycopg2  # — uncomment when implementing


@contextmanager
def connect() -> Iterator["psycopg2.extensions.connection"]:  # noqa: F821
    """Open a connection using shared/database.yaml + env vars.

    Usage:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(SQL, params)
                ...
    """
    raise NotImplementedError("Implement using shared/database.yaml env-var pattern")


def execute(conn, sql: str, params: dict | None = None) -> list[dict]:
    """Run a SELECT and return rows as a list of dicts (column-name keyed)."""
    raise NotImplementedError
