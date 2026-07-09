# Shared Directory

This directory contains common macros, utilities, and shared code used across the data platform.

## Purpose

- Reusable SQL macros
- Common utility functions
- Shared configuration files
- Data validation helpers
- Constants and enums

## Organization

- `macros/` - SQL macros for reuse in models
- `utils/` - Python utility functions
- `config/` - Shared configuration files

## Utilities

- `database.yaml` - Shared database connection configuration (reads `${DB_*}` from `.env`).
- `query.py` - Ad-hoc **read-only** query helper for the warehouse. Runs a SQL
  statement against `innodw_prod` using the `.env` credentials (the `dwreader`
  read-only role) and prints an aligned table. Use it for quick investigation so
  each session doesn't rewrite a connection script:

  ```bash
  .venv/bin/python shared/query.py "SELECT count(*) FROM silver.v_hires_unified"
  # or pipe multi-line SQL via stdin
  echo "SELECT client_name, count(*) FROM silver.v_terminations_unified GROUP BY 1" \
    | .venv/bin/python shared/query.py
  ```

  Read-only by role — writes/DDL fail at the database. Deploy view DDL via the
  files in `database/views/` (see `database/views/silver/silver_schema.md`), not this.