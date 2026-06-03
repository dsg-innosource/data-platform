"""Shared reporting lifecycle implementation.

This package provides the abstract `Report` class and supporting helpers that every
production report inherits from. See `reporting/routines/_framework.md` for the
canonical specification.

Modules:
  base       — Report ABC and run() lifecycle
  window     — report_window(today) — Monday rule, week alignment
  db         — psycopg2 connection helper (uses shared/database.yaml)
  render     — Jinja2 → HTML → PDF (wraps tools/html-to-pdf/convert.js)
  email      — Power Automate webhook client
  narrative  — Anthropic API narrative refinement, with fallback
  guards     — pre-send body checks
  config     — recipients, secrets, env loading
  style      — shared CSS loader
"""
