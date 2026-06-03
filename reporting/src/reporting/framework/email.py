"""Power Automate webhook email sender.

Single function: post a JSON payload (to, subject, body, attachments) to the
Power Automate webhook. The webhook accepts:

    {
        "to": "addr1@example.com;addr2@example.com",   # semicolon-joined
        "subject": "...",
        "body": "<html>...",
        "attachments": [{"name": "file.pdf", "contentBytes": "<base64>"}]
    }

Env: POWER_AUTOMATE_URL
"""

from __future__ import annotations

import base64
from pathlib import Path

# import requests  # — uncomment when implementing


def send(
    *,
    to: list[str],
    subject: str,
    body_html: str,
    attachments: list[Path] | None = None,
) -> None:
    """Send an HTML email via the Power Automate webhook.

    Recipients are joined with `;` (Power Automate Office 365 connector convention).
    Attachments are base64-encoded into `contentBytes`.

    Raises framework.base.WebhookError on non-200 response.
    """
    raise NotImplementedError


def _b64_attachment(path: Path) -> dict:
    """Convert a file to a Power Automate attachment dict."""
    return {"name": path.name, "contentBytes": base64.b64encode(path.read_bytes()).decode("utf-8")}
