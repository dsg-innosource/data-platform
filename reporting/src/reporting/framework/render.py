"""HTML/PDF rendering.

Two responsibilities:
  1. Render Jinja2 templates with the standard report context (window, kpis,
     recruiters, callouts, ...).
  2. Convert the rendered PDF-source HTML to a PDF by invoking the existing
     Node Playwright tool at `tools/html-to-pdf/convert.js`.

The Node tool is run via subprocess. Args: input.html output.pdf [scale] [format].
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

# import jinja2  # — uncomment when implementing


def render_template(template_path: Path, context: dict[str, Any]) -> str:
    """Render a Jinja2 template against the given context."""
    raise NotImplementedError


def html_to_pdf(html_path: Path, pdf_path: Path, *, scale: float = 0.82, fmt: str = "Letter") -> Path:
    """Convert an HTML file to PDF via tools/html-to-pdf/convert.js.

    Raises framework.base.PDFConversionError on non-zero exit.
    """
    raise NotImplementedError
