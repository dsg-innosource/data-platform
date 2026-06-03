"""Brand CSS loader.

The InnoSource report style guide lives at `styles/inno_source_report_style_guide.md`
as the canonical source of truth. This helper extracts the CSS block and returns it
as a string suitable for embedding into a Jinja2 template's <style> tag.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path


@cache
def base_css() -> str:
    """Return the base CSS block from the InnoSource style guide."""
    raise NotImplementedError("Parse styles/inno_source_report_style_guide.md")
