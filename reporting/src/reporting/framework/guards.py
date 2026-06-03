"""Pre-send email body guards.

Five rules, all raise EmailGuardError on violation. See routines/_framework.md § 6.
"""

from __future__ import annotations

import re

from .base import EmailGuardError


# Allowlist for legitimate `[bracketed]` patterns that look like placeholders but
# are intentional content (e.g. "[N of N offers accepted]").
_PLACEHOLDER_ALLOWLIST = {
    "[N of N",   # offer counts in fallback strings
    "[DEV]",     # dev override marker in subject (but subject isn't passed here)
}


def check_email_body(body: str) -> None:
    """Validate the rendered email body. Raise EmailGuardError on any violation."""
    raise NotImplementedError(
        "Implement five rules per routines/_framework.md § 6: "
        "HTML comments, bash heredoc artifacts, empty body, unrendered Jinja, "
        "unsubstituted placeholders."
    )
