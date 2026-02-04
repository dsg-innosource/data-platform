# Resume Fraud Detection Weekly Process

Quick guide for running the weekly resume fraud detection scan.

## Prerequisites

- Python 3.8+ installed
- Database credentials configured (via `.env` file or environment variables)
- Virtual environment activated (`source venv/bin/activate`)
- For PDF output: pandoc and weasyprint installed (`brew install pandoc`)

## Weekly Workflow

### Step 1: Dry Run (Recommended First)

Always preview the generated SQL before running against production:

```bash
python resume-fraud-detection/process.py --dry-run
```

**What this does:**
- Generates all three detection SQL queries
- Saves them to `resume-fraud-detection/output/YYYY-MM-DD/`
- Does NOT connect to or query the database

**Review the queries:**
- `employment_history_query.sql` - Employment fingerprint matching
- `multi_resume_divergence_query.sql` - Same-email resume comparison
- `professional_summary_query.sql` - Summary text matching

### Step 2: Run Full Detection

```bash
python resume-fraud-detection/process.py
```

**What this does:**
- Connects to the production PostgreSQL database
- Runs all three detection methods
- Classifies results by severity (HIGH / MEDIUM / LOW)
- Saves CSV files and a Markdown report

### Step 3: Review Results

Check the output folder for this run:

```
resume-fraud-detection/output/YYYY-MM-DD/
├── employment_history_results.csv
├── multi_resume_divergence_results.csv
├── professional_summary_results.csv
├── combined_signals.csv              # Start here
└── fraud_detection_report.md         # Full report
```

**Start with `combined_signals.csv`** - this has all flagged applicants sorted by severity.

### Step 4: Generate PDF (Optional)

```bash
python resume-fraud-detection/process.py --pdf
```

This generates `fraud_detection_report.pdf` alongside the Markdown report.

## Understanding Signal Severity

| Severity | What It Means | Action |
|----------|---------------|--------|
| **HIGH** | Employment history match (3+ applicants with identical employer/date fingerprints) | Immediate investigation |
| **MEDIUM** | Same email submitted significantly different resumes | Manual review |
| **LOW** | Professional summary match only (likely false positive from template copying) | Monitor only |

## Common Options

**Override lookback window:**
```bash
# Use 60 days instead of defaults (30 for employment/summary, 90 for divergence)
python resume-fraud-detection/process.py --lookback 60
```

**Run with PDF output:**
```bash
python resume-fraud-detection/process.py --pdf
```

## Quick Troubleshooting

**Database connection error**: Check that your `.env` file exists at the project root with `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD`

**"pandoc not found"**: Install with `brew install pandoc` (only needed for `--pdf` flag)

**No results returned**: Check that the lookback window covers the period you expect. Default is 30 days for employment/summary, 90 days for multi-resume divergence.

For detailed troubleshooting, see [Technical Documentation](../technical/Resume_Fraud_Detection.md).

## File Locations

| Item | Location |
|------|----------|
| Output files | `resume-fraud-detection/output/YYYY-MM-DD/` |
| Configuration | `resume-fraud-detection/config.yaml` |
| Known patterns | `resume-fraud-detection/reference/known-patterns.md` |
| Process script | `resume-fraud-detection/process.py` |
| Report styling | `resume-fraud-detection/assets/report-style.css` |

---

**For technical details, detection algorithms, and threshold tuning, see [Resume Fraud Detection Technical Documentation](../technical/Resume_Fraud_Detection.md)**
