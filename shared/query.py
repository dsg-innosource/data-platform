#!/usr/bin/env python
"""Ad-hoc read-only query helper for the data warehouse (innodw_prod).

Runs a SQL statement against the PostgreSQL warehouse using the DB_* credentials
in the repo-root .env, and prints the result as an aligned table. Intended for
quick, interactive investigation (inspecting silver views, spot-checking data)
so each session doesn't have to re-write a connection script.

The credentials in .env are the read-only `dwreader` role, so this is safe for
exploration — writes/DDL will fail at the database, not here.

USAGE
    # pass SQL as an argument
    .venv/bin/python shared/query.py "SELECT count(*) FROM silver.v_hires_unified"

    # or pipe it in (handy for multi-line SQL / heredocs)
    .venv/bin/python shared/query.py <<'SQL'
    SELECT client_name, count(*)
    FROM silver.v_terminations_unified
    WHERE termination_date >= '2026-01-01'
    GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
    SQL

REQUIREMENTS
    Run with a Python that has psycopg2 installed (it's in requirements.txt as
    psycopg2-binary). The project virtualenv at .venv has it:
        .venv/bin/python shared/query.py "..."

NOTES
    - Reads DB_HOST / DB_PORT / DB_NAME / DB_USERNAME / DB_PASSWORD from the
      repo-root .env (gitignored). sslmode=require.
    - Location-independent: resolves the repo root from this file's path, so it
      works regardless of the current working directory.
    - Read-only by role. Do not use for migrations/deploys — deploy view DDL via
      the files in database/views/ (see database/views/silver/silver_schema.md).
"""
import os
import sys

# Repo root is the parent of the directory holding this script (shared/).
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(REPO_ROOT, ".env")


def load_db_env():
    """Parse DB_* keys out of the repo-root .env (simple KEY=VALUE parser)."""
    if not os.path.exists(ENV_PATH):
        sys.exit(f"error: {ENV_PATH} not found (needs DB_HOST/PORT/NAME/USERNAME/PASSWORD)")
    env = {}
    with open(ENV_PATH) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            env[key.strip()] = val.strip().strip('"').strip("'")
    required = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USERNAME", "DB_PASSWORD"]
    missing = [k for k in required if not env.get(k)]
    if missing:
        sys.exit(f"error: .env missing {', '.join(missing)}")
    return env


def print_table(cols, rows):
    """Print rows as a simple aligned pipe-separated table."""
    widths = [len(c) for c in cols]
    srows = []
    for row in rows:
        srow = ["" if v is None else str(v) for v in row]
        srows.append(srow)
        for i, val in enumerate(srow):
            widths[i] = max(widths[i], len(val))
    print(" | ".join(c.ljust(widths[i]) for i, c in enumerate(cols)))
    print("-+-".join("-" * widths[i] for i in range(len(cols))))
    for srow in srows:
        print(" | ".join(val.ljust(widths[i]) for i, val in enumerate(srow)))
    print(f"\n({len(rows)} rows)")


def main():
    sql = sys.argv[1] if len(sys.argv) > 1 else sys.stdin.read()
    if not sql.strip():
        sys.exit("error: no SQL provided (pass as an argument or via stdin)")

    try:
        import psycopg2
    except ImportError:
        sys.exit("error: psycopg2 not installed — run with the project venv: "
                 ".venv/bin/python shared/query.py \"...\"")

    env = load_db_env()
    conn = psycopg2.connect(
        host=env["DB_HOST"], port=env["DB_PORT"], dbname=env["DB_NAME"],
        user=env["DB_USERNAME"], password=env["DB_PASSWORD"],
        sslmode="require", connect_timeout=30,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        if cur.description:
            print_table([d[0] for d in cur.description], cur.fetchall())
        else:
            print(f"OK ({cur.rowcount} rows affected)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
