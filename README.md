# Data Platform

A centralized data warehouse and analytics repository for processing business data workflows.

## Overview

This repository contains automated data processes for:
- **ADP Headcount**: Weekly employee headcount data loading and analysis
- **Monthly Billing**: ClickUp time tracking to client billing reports
- **Swim Campaigns**: Targeted candidate searches for recruiter outreach

Each process is self-contained with its own input/output folders, configuration, and documentation.

## Project Structure

```
data-platform/
├── adp-headcount/         # ADP employee headcount pipeline
│   ├── input/            # Place .xls files here
│   ├── archive/          # Processed files with timestamps
│   ├── modules/          # Pipeline modules (extract, transform, load)
│   ├── process.py        # Main script
│   └── config.yaml       # Database and pipeline config
│
├── monthly-billing/       # ClickUp billing process
│   ├── input/            # Place ClickUp CSV exports here
│   ├── output/           # Generated reports and CSVs
│   ├── archive/          # Historical reports
│   ├── process.py        # Main script
│   └── config.yaml       # Rates, budgets, category mappings
│
├── swim-campaigns/        # Targeted candidate search campaigns
│   ├── campaigns/        # Individual campaign folders (one per request)
│   ├── docs/             # Swim campaign guide and reference
│   ├── process.py        # Main script
│   └── config.yaml       # Database connection settings
│
├── docs/
│   ├── guides/           # User guides (how to run processes)
│   ├── technical/        # Technical documentation (architecture, config)
│   └── database/         # Database schema documentation
│
└── venv/                 # Python virtual environment
```

## Quick Start

### Prerequisites

- Python 3.8+
- Database access (PostgreSQL for production, DuckDB for testing)
- Virtual environment activated: `source venv/bin/activate`

### Running Processes

**ADP Headcount (Weekly)**
```bash
# 1. Place ADP .xls file in adp-headcount/input/
# 2. Test first (recommended)
python adp-headcount/process.py --test-mode

# 3. Run production
python adp-headcount/process.py
```

**Monthly Billing**
```bash
# 1. Export time tracking from ClickUp to monthly-billing/input/
# 2. Process billing
cd monthly-billing
python process.py
```

**Swim Campaigns**
```bash
# List available campaigns
python swim-campaigns/process.py --list

# Run a campaign
python swim-campaigns/process.py --campaign 2026-02-02-ads-order-processor

# Dry run (generate queries without executing)
python swim-campaigns/process.py --campaign 2026-02-02-ads-order-processor --dry-run
```

## Documentation

### User Guides (How to Run)
- **[ADP Headcount](docs/guides/ADP_Headcount.md)** - Step-by-step monthly workflow
- **[Monthly Billing](docs/guides/Monthly_Billing.md)** - Step-by-step billing process
- **[Swim Campaigns](swim-campaigns/docs/swim-campaign-guide.md)** - Targeted candidate searches

### Technical Documentation (How it Works)
- **[ADP Pipeline](docs/technical/ADP_Pipeline.md)** - Architecture, configuration, troubleshooting
- **[Billing Process](docs/technical/Billing_Process.md)** - Technical details, budget tracking, configuration
- **[Database Schemas](docs/database/schemas.md)** - All table definitions and relationships

### Other Guides
- **[PDF Export Guide](docs/PDF_EXPORT_GUIDE.md)** - Converting Markdown to PDFs

## Process Overview

### ADP Headcount Pipeline

**Purpose**: Load employee headcount data from ADP into data warehouse

**Frequency**: Weekly (every Monday)

**Flow**:
1. Extract: Read ADP Excel file
2. Transform: Clean and standardize data
3. Load: Insert to `bronze.adp_tenure_history`
4. Calculate: Generate `silver.fact_active_headcount` metrics
5. Archive: Move processed file with timestamp

**Key Features**:
- Test mode using DuckDB (safe testing without affecting production)
- Automatic Monday date calculation
- PII protection (files excluded from git)
- Automatic archiving with timestamps

### Monthly Billing Process

**Purpose**: Transform ClickUp time tracking into billing reports

**Frequency**: Monthly (first week of each month)

**Flow**:
1. Extract: Read ClickUp CSV export
2. Transform: Map categories to clients, convert durations
3. Calculate: Apply billing rates, track budgets
4. Generate: Create accounting CSV and summary report

**Key Features**:
- Budget tracking with burn rate analysis
- Multiple client support with different rates
- Markdown summary reports with insights
- CSV output ready for accounting software import

## Development Setup

### Initial Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd data-platform
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure databases**
   - ADP: Edit `adp-headcount/config.yaml`
   - Billing: Edit `monthly-billing/config.yaml`

### Adding a New Process

Follow the established pattern:

```
new-process/
├── input/              # Raw data files
├── archive/           # Processed files
├── output/            # Generated outputs (if applicable)
├── modules/           # Supporting code (if complex)
├── testing/           # Tests
├── process.py         # Main entry point (standardized name)
└── config.yaml        # Configuration
```

Then add documentation:
- User guide: `docs/guides/New_Process.md`
- Technical docs: `docs/technical/New_Process.md`

## Configuration

### ADP Headcount

**Production**: `adp-headcount/config.yaml`
- PostgreSQL connection details
- Table names (bronze/silver)
- Logging configuration

**Test Mode**: `adp-headcount/config.test.yaml`
- DuckDB (local file database)
- Same schema as production

### Monthly Billing

**Config**: `monthly-billing/config.yaml`
- Billing rates per client
- Budget amounts and tracking
- ClickUp category to client mappings

## Data Security

### Protected Data (Not in Git)

The following contain PII or sensitive information and are excluded via `.gitignore`:

- `adp-headcount/input/*.xls` - Employee data
- `adp-headcount/archive/*.xls` - Processed employee data
- `monthly-billing/input/*.csv` - Time tracking with names
- `monthly-billing/output/` - Billing reports with rates
- `monthly-billing/archive/` - Historical billing data

### Configuration Security

- Use environment variables for database passwords
- Don't commit real credentials to git
- Restrict file permissions on config files:
  ```bash
  chmod 600 adp-headcount/config.yaml
  chmod 600 monthly-billing/config.yaml
  ```

## Testing

### ADP Pipeline Tests

```bash
# Run test suite (uses DuckDB)
python adp-headcount/testing/run_tests.py

# Or use test mode with real data
python adp-headcount/process.py --test-mode
```

### Monthly Billing

Test mode coming soon. Currently, verify by reviewing output files before sending to accounting.

## Troubleshooting

### Common Issues

**"No module named 'pandas'"**
```bash
source venv/bin/activate
pip install -r requirements.txt
```

**"Database connection failed"**
- Verify credentials in config.yaml
- Check network access to database
- For ADP test mode: Use `--test-mode` flag

**"No files found in input/"**
- Place source files in the appropriate input directory
- For ADP: `adp-headcount/input/`
- For billing: `monthly-billing/input/`

See technical documentation for detailed troubleshooting.

## Future Plans

- **Additional Processes**: More automated data workflows following same pattern
- **Shared Utilities**: Common code for all processes in `shared/`
- **Enhanced Testing**: Automated test suites for all processes

## Contributing

1. Create a feature branch
2. Make changes following established patterns
3. Test thoroughly (use test modes where available)
4. Update documentation (both user guide and technical)
5. Submit a pull request

## Support

For questions or issues:
- Check the user guides in `docs/guides/`
- Review technical documentation in `docs/technical/`
- Examine troubleshooting sections in technical docs

---

**Last Updated**: 2025-11-03
**Version**: 2.0.0
