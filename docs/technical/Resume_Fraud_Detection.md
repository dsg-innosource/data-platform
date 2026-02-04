# Resume Fraud Detection - Technical Documentation

## Overview

The resume fraud detection process identifies potentially fraudulent resumes in the applicant database using three detection methods. It queries the PostgreSQL bronze layer, classifies results by severity, and outputs CSV files and reports.

### Detection Methods

1. **Employment History Matching** (PRIMARY) - Identifies groups of 3+ applicants with identical employer/date fingerprints
2. **Multi-Resume Divergence** (PRIMARY) - Flags same-email applicants who submit significantly different resumes
3. **Professional Summary Matching** (SECONDARY) - Corroborating signal only; people commonly copy intro templates

### Architecture

```
process.py
  ├── load_config()              # Config + shared DB credentials
  ├── build_employment_history_query()   # Method 1 SQL
  ├── build_multi_resume_query()         # Method 2 SQL
  ├── build_summary_match_query()        # Method 3 SQL
  ├── run_detection_method()     # Execute + save CSV
  ├── classify_signals()         # Combine into severity levels
  ├── generate_report()          # Markdown report
  └── generate_pdf()             # pandoc + weasyprint
```

## Database Tables

All tables are in the `bronze` schema. These are the same portal tables used by swim-campaigns.

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `bronze.portal_applicants` | Applicant master records | `id`, `first_name`, `last_name`, `email`, `deleted_at` |
| `bronze.portal_applicant_applications` | Structured application data | `applicant_id`, `previous_employer_1_name`, `previous_employer_1_start_date`, `previous_employer_1_end_date` (1-3) |
| `bronze.portal_applicant_documents` | Resume documents | `applicant_id`, `resume_text`, `is_resume`, `created_at` |
| `bronze.portal_resume_scores` | AI-extracted resume data | `applicant_id`, `resume_only_experience`, `created_at` |
| `bronze.portal_applicant_statuses` | Status lookup | `id`, `name` |

## Detection Method Details

### Method 1: Employment History Matching

Runs two sub-approaches combined via UNION:

**Option A - Structured Fields:**
- Concatenates `previous_employer_1_name` through `previous_employer_3_name` with their start/end dates
- Normalizes with `LOWER(TRIM(...))` and `COALESCE` for nulls
- MD5 hashes the concatenated string
- Groups by hash, flags groups with 3+ distinct applicant IDs

**Option B - Experience Text:**
- Uses `resume_only_experience` from `portal_resume_scores`
- Falls back to regex-extracted experience from `portal_applicant_documents.resume_text`
- MD5 hashes the normalized text
- Same grouping/threshold logic as Option A

**Why both?** Option A catches fraud when applicants fill out structured fields identically. Option B catches it when they submit identical resume documents but may not fill out structured fields.

**Threshold:** 3+ unique applicants (configurable in `config.yaml`)

**Lookback:** 30 days (configurable)

### Method 2: Multi-Resume Divergence

- Groups all resumes by applicant email address
- Hashes the first 1000 characters of each resume
- Counts distinct hashes per email
- Flags when all submitted resumes are different (HIGH) or most are different (MEDIUM)
- Also detects identity issues: same email mapped to multiple `applicant_id` values

**Lookback:** 90 days (wider window to catch patterns over time)

**Minimum distinct resumes:** 2 (configurable)

### Method 3: Professional Summary Matching

- Extracts professional summary section via PostgreSQL regex: `(?i)(?:professional summary|summary of qualifications|summary|profile)[\s\n]+(.{100,200})`
- MD5 hashes the extracted text
- Groups by hash, flags groups with 5+ distinct applicants

**Why higher threshold (5)?** Professional summaries have a high false positive rate. People commonly copy intro templates from Google without fraudulent intent. This method is only useful as corroborating evidence alongside Method 1 or 2.

**Lookback:** 30 days (configurable)

### Signal Classification

The `classify_signals()` function combines results from all three methods:

| Condition | Severity | Signal Type |
|-----------|----------|-------------|
| Employment match + Summary match | HIGH | `employment_and_summary_match` |
| Employment match only | HIGH | `employment_history_match` |
| Multi-resume divergence | MEDIUM | `multi_resume_divergence` |
| Summary match only | LOW | `summary_match_only` |

## Configuration

### config.yaml

```yaml
tables:
  applicants: "bronze.portal_applicants"
  applications: "bronze.portal_applicant_applications"
  # ... etc

detection:
  employment_history:
    threshold: 3        # Min applicants to flag a group
    lookback_days: 30   # How far back to scan

  multi_resume:
    lookback_days: 90
    min_distinct_resumes: 2

  professional_summary:
    threshold: 5        # Higher threshold (secondary signal)
    lookback_days: 30
```

### Tuning Thresholds

- **Lowering employment threshold (e.g., 2)**: More sensitive but more false positives. Two people can legitimately work at the same places.
- **Raising employment threshold (e.g., 5)**: Fewer false positives but may miss smaller fraud rings.
- **Adjusting lookback**: Wider windows catch more patterns but increase processing time and may surface old data.
- **`--lookback` CLI flag**: Overrides ALL methods uniformly. Note this changes the multi-resume window from its default 90 days.

## Output Structure

Each run creates a date-stamped folder:

```
resume-fraud-detection/output/2026-02-04/
├── employment_history_query.sql        # Transparency: exact SQL executed
├── employment_history_results.csv      # Raw Method 1 results
├── multi_resume_divergence_query.sql
├── multi_resume_divergence_results.csv
├── professional_summary_query.sql
├── professional_summary_results.csv
├── combined_signals.csv                # Unified classification
├── fraud_detection_report.md           # Full Markdown report
└── fraud_detection_report.pdf          # Optional (--pdf flag)
```

### CSV Schemas

**combined_signals.csv** (primary output):
```
applicant_id, first_name, last_name, email, severity, signal_type, details
```

**employment_history_results.csv:**
```
applicant_id, first_name, last_name, email, employment_hash, hash_source, ring_size, employment_summary, created_at
```

**multi_resume_divergence_results.csv:**
```
email, resume_count, unique_content_count, applicant_id_count, names_used, applicant_ids, first_upload, last_upload, divergence_level, identity_flag
```

**professional_summary_results.csv:**
```
applicant_id, first_name, last_name, email, summary_hash, ring_size, summary_preview, created_at
```

## PDF Generation

Uses pandoc with weasyprint engine and a custom CSS stylesheet:

```bash
pandoc report.md -o report.pdf --pdf-engine=weasyprint --css=assets/report-style.css
```

**Dependencies:**
- `pandoc` - Install with `brew install pandoc`
- `weasyprint` - Install with `pip install weasyprint`

PDF generation is optional (`--pdf` flag) and gracefully skips if dependencies are missing.

## Known Patterns Reference

Instead of silver layer registry tables (planned for future), confirmed fraud patterns are documented in `resume-fraud-detection/reference/known-patterns.md`. Update this file when fraud rings are confirmed through investigation.

Future migration path: create `silver.fraud_employment_patterns` and `silver.fraud_divergent_emails` tables and load confirmed patterns there for automated exclusion/escalation.

## Troubleshooting

### No results returned
- Check lookback window covers expected period
- Verify tables have recent data: `SELECT MAX(created_at) FROM bronze.portal_applicant_applications;`
- Run with `--dry-run` and manually execute queries to debug

### PostgreSQL regex errors in Method 3
- The summary extraction regex may not match all resume formats
- If regex fails, the summary will be NULL and that applicant is excluded from Method 3
- This is acceptable since Method 3 is a secondary signal

### Performance issues
- Lookback windows limit dataset size. If queries are slow, reduce with `--lookback`
- Method 2 hashes first 1000 chars of resume text (not full text) for performance
- Consider adding indexes on `created_at` columns if not already present

### Duplicate output folders
- Running multiple times on the same day writes to the same `output/YYYY-MM-DD/` folder
- Previous results are overwritten
- If you need to preserve a specific run, copy the folder before re-running

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-01-07 | Initial workflow - professional summary MD5 matching only |
| 2.0 | 2026-02-04 | Shift to employment history matching; add multi-resume divergence; demote professional summary to secondary signal |
