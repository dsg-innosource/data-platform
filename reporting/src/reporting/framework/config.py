"""Recipients and runtime config loading.

Recipients live only in `config/recipients.md` — never inline in code. This module
parses the markdown table for a named section and returns the email list.

Other env-loading helpers (DATABASE_URL, POWER_AUTOMATE_URL, ANTHROPIC_API_KEY)
live here too so a Report subclass never reads os.environ directly.
"""

from __future__ import annotations

import os
from pathlib import Path

from .base import RecipientCountError


def recipients_for(section_name: str, config_path: Path | None = None) -> list[str]:
    """Parse `config/recipients.md`, return the email list for the named section.

    Verifies that the count of addresses in the "To field (semicolon-joined):" line
    matches the count of rows in the markdown table for that section. Raises
    RecipientCountError on mismatch.
    """
    raise NotImplementedError


def env(name: str, *, required: bool = True, default: str | None = None) -> str | None:
    """Read an env var with explicit required/default behavior."""
    val = os.environ.get(name, default)
    if required and val is None:
        raise RuntimeError(f"Required env var {name!r} is not set")
    return val
