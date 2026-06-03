# Monthly Billing — Migration Spec

A spec/prompt for migrating the monthly billing process out of this repo (a Python script + manual CSV export workflow) into a feature inside a separate web app the data team is building. Hand this whole document to the agent working in that repo.

## Context

Today, monthly billing runs like this:

1. Sean exports a ClickUp time-tracking report to CSV and drops it into `monthly-billing/input/`.
2. He runs `monthly-billing/process.py`. The script:
   - Reads the most recent CSV in `input/`
   - Parses durations (`"1 h 30 m"`, `"1:30:00"`) into decimal hours
   - Maps a ClickUp `CATEGORY` tag (e.g., `BILLABLE - JN`) to a client name
   - Multiplies hours × per-client rate to get an amount
   - Writes a clean CSV (for accounting) and a Markdown summary (internal)
3. Sean reviews and sends the CSV to accounting, then archives the input + outputs into `archive/YYYY-MM/`.

The script is fine, but it has weak spots: ClickUp export schemas drift (we just lost the `Custom Task ID` column), nothing is persisted between runs (no history, no analytics), and configuration changes (new client, rate change) require editing YAML.

The new home is a web app the team is already building. We want to move this functionality there as a first-class feature, take advantage of the app's SQLite database for persistence, and ingest from the ClickUp API directly when possible (with CSV upload as a fallback for whenever the API can't give us what we need).

The current Python script is the source of truth for transformation behavior. The agent building the new feature should read `monthly-billing/process.py`, `config.yaml`, `README.md`, and the example inputs/outputs in `monthly-billing/archive/` for any detail this spec doesn't cover.

## Goals

- Replace the manual CSV-export-and-script workflow with a web UI
- Persist time entries at the row level so we can do trend analysis later (hours per client over time, per-team-member load, etc.)
- Move client list, category mappings, and billing rates into the UI (no more YAML edits)
- Ingest from the ClickUp API as the primary source; CSV upload remains supported as a fallback
- Preserve exact output parity (cleaned CSV format, summary structure) so accounting sees no change

## Non-goals (for the first cut)

- PDF generation
- Emailing reports / Power Automate handoff
- Sending invoices to accounting systems (QuickBooks etc.)
- Multi-tenancy or external client portals
- User auth / role-based access — out of scope for this feature; rely on the app's existing auth
- Per-entry editing or exclusion in the UI (review-only for now)

## Data ingestion

### Primary: ClickUp API

The first build step is **research/spike**: confirm ClickUp's API can return what we need. Specifically, the agent should validate that ClickUp's time-tracking endpoints can:

- Filter time entries by date range
- Return per-entry details (user, start/stop, duration, task, task name, custom-field values, tags/category)
- **Return the friendly task ID (e.g., `DSG-4100`) — this is what accounting sees in our outputs.** This bit us in the most recent CSV export, where the column disappeared. If the API can't supply it, we have to fall back to CSV.

Assuming the spike confirms feasibility, the UI flow:

- User picks a billing period (year-month) and clicks "Fetch from ClickUp"
- App calls the ClickUp API, pulls all time entries with a billable category in that period
- Each entry is upserted into the `time_entries` table linked to the billing period

### Fallback: CSV upload

- Drag-and-drop a ClickUp CSV export onto the billing-period detail page
- Same downstream processing as API ingestion (parse, map, persist)
- Required columns (current ClickUp export shape): `Username`, `Start Text`, `Time Tracked Text`, `Task Name`, `Custom Task ID`, `CATEGORY`. If `Custom Task ID` is missing the upload should fail loudly with a clear message — don't silently fall back to the internal `Task ID`.
- Re-uploading for a period that already has data: replace, don't append (warn the user first)

## Data model (SQLite)

Roughly these tables — names/shapes can flex to match the app's conventions:

- **`clients`** — `id`, `name` (e.g., `Job News`), `billing_rate` (decimal, dollars/hour), `active` (bool). One flat rate per client; no historical/effective-dated rates.
- **`category_mappings`** — `id`, `clickup_category` (e.g., `BILLABLE - JN`), `client_id`. Many-to-one; a category maps to exactly one client.
- **`billing_periods`** — `id`, `year_month` (e.g., `2026-05`), `status` (`draft` | `finalized`), `created_at`, `finalized_at`. The `year_month` represents the **billing month** (when the report is generated), not the work month — so April work belongs to the `2026-05` period.
- **`time_entries`** — one row per ClickUp time entry. Fields: `id`, `billing_period_id`, `client_id` (resolved via category mapping at ingest), `clickup_time_entry_id` (for idempotency), `username`, `work_date`, `billable_hours` (decimal), `task_name`, `task_id` (the friendly `DSG-####`), `clickup_category` (raw, for audit), `source` (`api` | `csv`), `ingested_at`.

Storing every entry at the row level is what makes future analytics possible.

## Transformations (preserve from current script)

These rules come from `process.py` — keep them identical so outputs match.

- **Duration parsing**: handle `"1 h 30 m"`, `"1 h"`, `"45 m"`, `"1:15:00"`, `"1:15"`, and bare decimals. Round to 2 decimal places. Empty → 0.0.
- **Date parsing**: input format `"MM/DD/YYYY, HH:MM:SS AM/PM TZ"` (e.g., `"04/28/2026, 7:28:00 PM EDT"`) → use the date portion only, drop time and timezone.
- **Category → client**: lookup against `category_mappings`. **Unrecognized categories must not silently pass through** — surface them in the UI for the user to map (or skip) before the period can be finalized.
- **Amount**: `billable_hours × client.billing_rate`, rounded to 2 decimals.
- **Sort order** (for output): by date, then client, then name.

## UI

### Settings: Clients

- Table view: name, rate, active toggle
- Add / edit / deactivate
- Deactivating a client doesn't break historical periods

### Settings: Category mappings

- Table view: ClickUp category string → client
- Add / edit / delete
- "Unmapped categories" surface here when ingestion finds something new

### Billing periods (list)

- One row per period: year-month, status, total hours, total amount, last updated
- Click through to the detail page

### Billing period detail

The main working surface. Has these sections on one page:

- **Header**: year-month, status (draft/finalized), buttons to fetch from API / upload CSV / finalize
- **Ingestion panel**: shows source (api/csv), ingested-at timestamp, count of entries; warning if any unmapped categories
- **Summary panels** (these mirror the current Markdown summary):
  - By client: client, hours, rate, amount
  - Grand total row
  - By team member: name, hours, amount
  - By client × month: client, month, hours, amount (will collapse to one row per client when entries don't cross months, but keep the structure for periods that do)
- **Detailed log table**: date, client, name, hours, rate, amount, task, task ID — sortable
- **Download buttons**:
  - "Download cleaned CSV" — exact format below
  - (no PDF, no email — deferred)

### Finalize

- Sets `status = finalized`, locks the period from further ingestion or edits
- Reversible by an admin action (just flip the status); not a destructive step

## Outputs

### Cleaned CSV (download)

Exact format as today, for accounting compatibility. Filename: `billing_report_YYYY-MM-DD_to_YYYY-MM-DD.csv` where the dates are the actual min/max work dates in the period (not the calendar month).

Columns, in order:

```
Date,Month-Year,Client,Name,Billable Hours,Task,Task ID
```

- `Date`: ISO `YYYY-MM-DD`
- `Month-Year`: `YYYY-MM`
- `Billable Hours`: decimal, no thousands separator
- `Task ID`: the friendly ClickUp ID (`DSG-####`)
- Sort: by Date, then Client, then Name

Reference example: `monthly-billing/archive/2026-04/billing_report_2026-03-05_to_2026-03-30.csv`.

### In-browser summary

Renders the same content as today's `billing_summary_YYYY-MM.md` (see `monthly-billing/archive/2026-04/billing_summary_2026-04.md`), but as native UI sections rather than a Markdown file. The "billing month" in the title is the `billing_periods.year_month` value, not the work month.

## Edge cases worth handling

- **Empty period**: if ingestion returns zero billable entries, show a "no data yet" state — don't crash, don't generate empty outputs.
- **Re-ingestion of the same period**: replace existing entries, warn the user, log the prior count.
- **Unmapped category**: block finalization until every entry has a `client_id` or is explicitly excluded.
- **Rate change after ingestion**: when a client's rate changes, recompute amounts for all *draft* periods automatically; do not change finalized periods.
- **Client renamed**: time entries keep their `client_id` link, so historical reports update to the new name (this is fine — there's only one canonical name).

## Open questions for the implementer

1. Confirm via ClickUp API spike whether the friendly task ID (`DSG-####`) is reachable. If not, document the workaround (CSV-only) before building API ingestion.
2. Confirm SQLite is the right home given the rest of the app — if the app already has a different store, use that.
3. Settle on the period lifecycle: is `finalized` enough, or do we want a separate `sent_to_accounting` flag?

## Reference

Source-of-truth code and examples in this repo:

- Script: `monthly-billing/process.py`
- Config: `monthly-billing/config.yaml`
- README: `monthly-billing/README.md`
- Archived runs (sample inputs + outputs): `monthly-billing/archive/`

Read `process.py` for any transformation detail this spec leaves ambiguous — match its behavior exactly unless this spec deliberately overrides it.
