"""Dagster resources — shared connections and clients.

`resources_for_env()` returns the resource dict appropriate for the current env
(local dev vs Dagster Cloud production). Currently a passthrough placeholder;
add Postgres, Anthropic, and Power Automate clients here as we wire them up.
"""

from __future__ import annotations


def resources_for_env() -> dict:
    """Return the resource dict for Definitions(...).

    Initially empty — the reporting framework reads its own env vars directly.
    As we move shared connections into Dagster resources (recommended for
    connection pooling and easier mocking), populate this dict.
    """
    return {}
