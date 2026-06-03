"""Report abstract base class and the standard run() lifecycle.

Every production report subclasses `Report` and implements four methods:
  - fetch(conn, window)         — run queries, return ReportData
  - derive_callouts(data)       — apply deterministic rule table
  - render_pdf_html(data, callouts)
  - render_email_html(data, callouts)

The framework owns everything else (window resolution, narrative refinement,
guards, PDF conversion, email send, run logging). See routines/_framework.md § 2.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Literal

from .window import Window, report_window


# ── Exceptions ───────────────────────────────────────────────────────────────
class ReportRunError(Exception):
    """Base for all framework-raised report errors."""


class DBConnectionError(ReportRunError): ...
class EmailGuardError(ReportRunError): ...
class RecipientCountError(ReportRunError): ...
class WebhookError(ReportRunError): ...
class PDFConversionError(ReportRunError): ...


# ── Data structures ──────────────────────────────────────────────────────────
@dataclass
class Callout:
    section: str
    tone: Literal["positive", "watch", "context", "alert"]
    rule_id: str
    fallback_text: str
    refined_text: str | None = None
    data: dict[str, Any] = field(default_factory=dict)

    @property
    def text(self) -> str:
        """The text to render — refined if available, fallback otherwise."""
        return self.refined_text or self.fallback_text


@dataclass
class ReportData:
    window: Window
    sections: dict[str, Any] = field(default_factory=dict)


@dataclass
class RunResult:
    routine_name: str
    window: Window
    pdf_path: Path
    email_html_path: Path
    sent: bool
    recipient_count: int
    narrative_used: bool
    duration_seconds: float


# ── Report ABC ───────────────────────────────────────────────────────────────
class Report(ABC):
    """Abstract base for all production reports.

    Subclasses MUST set:
      - routine_name (str): kebab-case identifier matching the spec filename
      - recipients_section_name (str): section header in config/recipients.md
      - output_basename (str): file prefix for HTML/PDF outputs

    Subclasses MAY override:
      - report_window(today) — for non-daily windows (e.g. weekly)
    """

    routine_name: str
    recipients_section_name: str
    output_basename: str

    # ── Abstract methods (implement these per report) ────────────────────────
    @abstractmethod
    def fetch(self, conn, window: Window) -> ReportData: ...

    @abstractmethod
    def derive_callouts(self, data: ReportData) -> list[Callout]: ...

    @abstractmethod
    def render_pdf_html(self, data: ReportData, callouts: list[Callout]) -> str: ...

    @abstractmethod
    def render_email_html(self, data: ReportData, callouts: list[Callout]) -> str: ...

    # ── Optional override ────────────────────────────────────────────────────
    def report_window(self, today: date) -> Window:
        """Default: daily window with the Monday rule. Override for weekly reports."""
        return report_window(today)

    # ── The standard lifecycle ───────────────────────────────────────────────
    def run(
        self,
        *,
        today: date | None = None,
        dry_run: bool = False,
        recipients_override: list[str] | None = None,
        narrative_disable: bool = False,
    ) -> RunResult:
        """Execute the standard 11-step lifecycle. See routines/_framework.md § 2."""
        raise NotImplementedError(
            "Implement per routines/_framework.md § 2 — orchestrate fetch → "
            "derive_callouts → narrative.refine → render → guards → PDF → email → log"
        )
