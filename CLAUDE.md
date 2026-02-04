# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project Overview

This is a data platform repository containing automated data processes for business workflows. The repository is actively used for:

- **ADP Headcount**: Weekly employee headcount data loading and warehouse management
- **Monthly Billing**: ClickUp time tracking transformation to client billing reports
- **Swim Campaigns**: Targeted candidate searches for recruiter outreach
- **Resume Fraud Detection**: Automated detection of fraudulent resumes via employment history matching, multi-resume divergence, and summary fingerprinting

Each process is self-contained with its own configuration, input/output handling, and documentation.

## Current Architecture

```
data-platform/
├── adp-headcount/         # ADP employee headcount pipeline
│   ├── input/            # Place .xls files here
│   ├── archive/          # Processed files with timestamps
│   ├── modules/          # extract.py, transform.py, load.py, etc.
│   ├── process.py        # Main script (standardized name)
│   └── config.yaml       # Database and pipeline config
│
├── monthly-billing/       # ClickUp billing process
│   ├── input/            # Place ClickUp CSV exports here
│   ├── output/           # Generated reports and CSVs
│   ├── archive/          # Historical reports
│   ├── process.py        # Main script (standardized name)
│   └── config.yaml       # Rates, budgets, category mappings
│
├── swim-campaigns/        # Targeted candidate search campaigns
│   ├── campaigns/        # One folder per campaign (audit trail)
│   ├── docs/             # Swim campaign guide
│   ├── process.py        # Main script
│   └── config.yaml       # Database connection
│
├── resume-fraud-detection/ # Automated resume fraud detection
│   ├── assets/           # Report styling (CSS)
│   ├── output/           # Date-stamped detection results
│   ├── reference/        # Known fraud patterns documentation
│   ├── process.py        # Main script
│   └── config.yaml       # Thresholds, lookback windows, table refs
│
├── docs/
│   ├── guides/           # User guides (how to run processes)
│   ├── technical/        # Technical documentation (architecture, config)
│   └── database/         # Database schema documentation
│
├── venv/                 # Python virtual environment
└── README.md             # Project overview and quick start
```

## Key Technologies

- **Python 3.8+**: Primary language for ETL processes
- **Pandas**: Data transformation and manipulation
- **PostgreSQL**: Production data warehouse
- **DuckDB**: Local testing database (ADP process)
- **SQLAlchemy**: Database connection management

## Running Processes

### ADP Headcount (Weekly)

```bash
# Test mode (recommended first)
python adp-headcount/process.py --test-mode

# Production
python adp-headcount/process.py

# Custom dates
python adp-headcount/process.py --snapshot-date "2025-11-04" --report-date "2025-10-28"
```

**Process flow**: Extract → Transform → Load (Bronze) → Calculate (Silver) → Archive

### Monthly Billing

```bash
cd monthly-billing
python process.py
```

**Process flow**: Extract → Transform → Calculate → Generate Reports

### Swim Campaigns

```bash
# List available campaigns
python swim-campaigns/process.py --list

# Run a specific campaign
python swim-campaigns/process.py --campaign 2026-02-02-ads-order-processor

# Dry run (generate SQL queries without executing)
python swim-campaigns/process.py --campaign 2026-02-02-ads-order-processor --dry-run
```

**Process flow**: Load campaign config → Build SQL → Query database → Generate CSVs

### Resume Fraud Detection (Weekly)

```bash
# Run all detection methods
python resume-fraud-detection/process.py

# Dry run (generate SQL queries without executing)
python resume-fraud-detection/process.py --dry-run

# Override lookback window (days) for all methods
python resume-fraud-detection/process.py --lookback 60

# Also generate PDF report
python resume-fraud-detection/process.py --pdf
```

**Process flow**: Build queries → Run 3 detection methods → Classify signals → Generate report

**Detection methods:**
1. Employment history fingerprinting (structured fields + resume text)
2. Multi-resume divergence (same email, different resumes)
3. Professional summary matching (secondary/corroborating signal only)

## Documentation

All documentation follows a split structure:

- **User Guides** (`docs/guides/`): Simple step-by-step instructions for running processes
- **Technical Docs** (`docs/technical/`): Comprehensive architecture, configuration, troubleshooting
- **Database Docs** (`docs/database/`): Schema definitions, relationships, data dictionary

**Always check both the user guide AND technical documentation** when working with a process.

## Development Guidelines

### Virtual Environment

Always activate the virtual environment:

```bash
source venv/bin/activate
```

### Adding a New Process

Follow the established pattern:

1. Create process folder: `new-process/`
2. Include subfolders: `input/`, `archive/`, `output/` (if needed), `modules/` (if complex)
3. Main script: `process.py` (standardized name)
4. Configuration: `config.yaml`
5. Documentation:
   - User guide: `docs/guides/New_Process.md`
   - Technical docs: `docs/technical/New_Process.md`

### Code Patterns

- **Standardized naming**: All main entry points are named `process.py`
- **Self-contained**: Each process folder contains everything needed
- **Input/Archive pattern**: Files in `input/` are processed and moved to `archive/`
- **Configuration-driven**: All settings in `config.yaml`, not hardcoded

### Data Security

**PII and sensitive data** is excluded from git via `.gitignore`:

- `adp-headcount/input/` - Employee data
- `adp-headcount/archive/` - Processed employee files
- `monthly-billing/input/` - Time tracking data
- `monthly-billing/output/` - Billing reports with rates
- `monthly-billing/archive/` - Historical billing data
- `swim-campaigns/campaigns/*/*.csv` - Candidate contact lists
- `swim-campaigns/campaigns/*/*.sql` - Generated queries
- `resume-fraud-detection/output/` - Fraud detection results with applicant PII

**Never commit**:
- Excel files from ADP
- CSV files from ClickUp
- Billing output files
- Real database credentials

## Database Schema

### Bronze Layer
- `bronze.adp_tenure_history` - Raw ADP employee data snapshots

### Silver Layer
- `silver.fact_active_headcount` - Aggregated headcount metrics by department

See `docs/database/schemas.md` for complete schema documentation.

## Testing

### ADP Pipeline

```bash
# Test mode with DuckDB (safe, doesn't affect production)
python adp-headcount/process.py --test-mode

# Run test suite
python adp-headcount/testing/run_tests.py
```

### Monthly Billing

Currently tested manually by reviewing output files before sending to accounting.

## Common Tasks

### Updating Billing Rates

Edit `monthly-billing/config.yaml`:

```yaml
billing:
  rates:
    client_name: 175.00  # Update rate here
```

### Adding a New Billing Client

1. Add rate in `monthly-billing/config.yaml`
2. Add budget tracking (optional)
3. Add category mapping
4. Create matching categories in ClickUp

See `docs/technical/Billing_Process.md` for detailed instructions.

### Modifying Database Schema

1. Document change in `docs/database/schemas.md`
2. Write migration SQL
3. Test on dev/staging database
4. Update pipeline code
5. Execute migration on production

## Troubleshooting

### Module Not Found Errors

```bash
source venv/bin/activate
pip install -r requirements.txt
```

### Database Connection Errors

- Check credentials in `config.yaml`
- Verify network access
- For ADP: Use `--test-mode` to test with DuckDB

### File Not Found Errors

- Verify files are in correct `input/` directory
- Check file naming (ADP: .xls/.xlsx, Billing: .csv)

See technical documentation for detailed troubleshooting.

## Future Plans

- **Additional Processes**: More automated workflows following same self-contained pattern
- **Shared Utilities**: Common code extracted to `shared/` folder
- **Enhanced Testing**: Automated test suites for all processes

## Contributing

When making changes:

1. Follow established patterns (self-contained processes, `process.py` naming)
2. Update both user guide and technical documentation
3. Test thoroughly (use test modes where available)
4. Ensure sensitive data is excluded from git
5. Update this file if architecture changes significantly

## Important Files

- `README.md` - Project overview and quick start
- `docs/README.md` - Documentation organization guide
- `docs/guides/` - User-facing process guides
- `docs/technical/` - Technical implementation details
- `docs/database/schemas.md` - Complete database schema documentation

---

**Repository Status**: Active (production use)
**Last Updated**: 2026-02-04
**Version**: 2.0.0
