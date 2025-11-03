# Database Schema Documentation

This document describes all database tables used across data processes.

## Schema Organization

The database follows a medallion architecture:

- **Bronze Layer**: Raw data with minimal transformation (historical archive)
- **Silver Layer**: Cleaned, aggregated, business-ready data (analytics)
- **Gold Layer**: (Future) Highly refined, domain-specific datasets

## ADP Headcount Tables

### bronze.adp_tenure_history

Historical archive of ADP tenure snapshots.

**Purpose**: Store point-in-time snapshots of employee tenure data from ADP

**Update Frequency**: Weekly (every Monday)

**Schema**:

```sql
CREATE TABLE bronze.adp_tenure_history (
    employee_id VARCHAR(50) NOT NULL,
    full_name VARCHAR(255),
    department_number VARCHAR(6),
    department_name VARCHAR(255),
    position_title VARCHAR(255),
    hire_date DATE,
    tenure_years DECIMAL(10, 2),
    snapshot_date DATE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (employee_id, snapshot_date)
);
```

**Column Descriptions**:

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| employee_id | VARCHAR(50) | Unique employee identifier from ADP | ADP: Employee ID |
| full_name | VARCHAR(255) | Employee's full name | ADP: Full Name |
| department_number | VARCHAR(6) | Department identifier (e.g., "000001") | ADP: Department # |
| department_name | VARCHAR(255) | Department name | ADP: Department Name |
| position_title | VARCHAR(255) | Job title/position | ADP: Position/Title |
| hire_date | DATE | Date employee was hired | ADP: Hire Date |
| tenure_years | DECIMAL(10,2) | Years of service as of snapshot date | ADP: Tenure (Years) |
| snapshot_date | DATE | Monday date when this snapshot was taken | Calculated (Monday logic) |
| created_at | TIMESTAMP | When record was inserted into database | System timestamp |

**Primary Key**: `(employee_id, snapshot_date)`
- Allows tracking same employee across multiple snapshots
- Prevents duplicate entries for same employee on same date

**Indexes**:

```sql
CREATE INDEX idx_adp_tenure_snapshot_date
    ON bronze.adp_tenure_history (snapshot_date);

CREATE INDEX idx_adp_tenure_department
    ON bronze.adp_tenure_history (department_number, snapshot_date);
```

**Sample Data**:

```sql
SELECT * FROM bronze.adp_tenure_history
WHERE snapshot_date = '2025-11-04'
LIMIT 3;
```

| employee_id | full_name | dept# | dept_name | position | hire_date | tenure | snapshot_date | created_at |
|-------------|-----------|-------|-----------|----------|-----------|--------|---------------|------------|
| EMP001 | John Doe | 000001 | Engineering | Senior Dev | 2020-03-15 | 5.65 | 2025-11-04 | 2025-11-04 10:30:00 |
| EMP002 | Jane Smith | 000002 | Marketing | Manager | 2019-06-01 | 6.42 | 2025-11-04 | 2025-11-04 10:30:00 |
| EMP003 | Bob Johnson | 000001 | Engineering | Junior Dev | 2023-01-10 | 2.82 | 2025-11-04 | 2025-11-04 10:30:00 |

**Data Retention**: Indefinite (historical archive)

**Usage**:
- Historical headcount analysis
- Tenure tracking over time
- Employee lifecycle reporting
- Audit trail for headcount changes

---

### silver.fact_active_headcount

Aggregated headcount metrics by department.

**Purpose**: Provide point-in-time active employee counts by department for analytics

**Update Frequency**: Weekly (every Monday, after bronze load)

**Schema**:

```sql
CREATE TABLE silver.fact_active_headcount (
    department_number VARCHAR(6),
    snapshot_date DATE NOT NULL,
    report_date DATE,
    active_count INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (department_number, snapshot_date)
);
```

**Column Descriptions**:

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| department_number | VARCHAR(6) | Department identifier | From bronze.adp_tenure_history |
| snapshot_date | DATE | Monday date of this snapshot | From bronze.adp_tenure_history |
| report_date | DATE | Previous Monday (7 days before snapshot) | Calculated |
| active_count | INTEGER | Count of active employees in department | Calculated (COUNT) |
| created_at | TIMESTAMP | When record was inserted | System timestamp |

**Primary Key**: `(department_number, snapshot_date)`
- One record per department per snapshot
- `department_number` can be NULL (for organization-wide totals)

**Calculation Logic**:

```sql
INSERT INTO silver.fact_active_headcount
    (department_number, snapshot_date, report_date, active_count, created_at)
SELECT
    department_number,
    snapshot_date,
    ? AS report_date,  -- Previous Monday
    COUNT(*) AS active_count,
    CURRENT_TIMESTAMP AS created_at
FROM bronze.adp_tenure_history
WHERE snapshot_date = ?  -- Current Monday
GROUP BY department_number, snapshot_date;
```

**Indexes**:

```sql
CREATE INDEX idx_active_headcount_snapshot
    ON silver.fact_active_headcount (snapshot_date);

CREATE INDEX idx_active_headcount_dept_snapshot
    ON silver.fact_active_headcount (department_number, snapshot_date);
```

**Sample Data**:

```sql
SELECT * FROM silver.fact_active_headcount
WHERE snapshot_date = '2025-11-04'
ORDER BY department_number;
```

| dept# | snapshot_date | report_date | active_count | created_at |
|-------|---------------|-------------|--------------|------------|
| 000001 | 2025-11-04 | 2025-10-28 | 15 | 2025-11-04 10:30:15 |
| 000002 | 2025-11-04 | 2025-10-28 | 8 | 2025-11-04 10:30:15 |
| 000003 | 2025-11-04 | 2025-10-28 | 12 | 2025-11-04 10:30:15 |
| NULL | 2025-11-04 | 2025-10-28 | 35 | 2025-11-04 10:30:15 |

**Data Retention**: Indefinite (time series for trending)

**Usage**:
- Headcount dashboards
- Department growth analysis
- Weekly/monthly headcount reports
- Budget and capacity planning

**Relationship to Bronze**:

```sql
-- Verify silver matches bronze
SELECT
    b.snapshot_date,
    b.department_number,
    COUNT(*) as bronze_count,
    s.active_count as silver_count
FROM bronze.adp_tenure_history b
LEFT JOIN silver.fact_active_headcount s
    ON b.department_number = s.department_number
    AND b.snapshot_date = s.snapshot_date
WHERE b.snapshot_date = '2025-11-04'
GROUP BY b.snapshot_date, b.department_number, s.active_count
HAVING COUNT(*) != s.active_count;

-- Should return no rows if silver calculation is correct
```

---

## Future Schemas

### Planned Tables

**Bronze Layer:**
- `bronze.clickup_time_tracking` - Raw time tracking data (if we move billing to database)
- `bronze.financial_transactions` - Raw financial data import

**Silver Layer:**
- `silver.dim_employees` - Employee dimension table
- `silver.dim_departments` - Department dimension table
- `silver.fact_monthly_billing` - Aggregated billing by client/month

**Gold Layer:**
- `gold.executive_dashboard_metrics` - Pre-aggregated executive KPIs
- `gold.budget_vs_actual` - Budget tracking and variance analysis

---

## Database Maintenance

### Monitoring Table Growth

```sql
-- Check row counts and sizes
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
    n_live_tup AS row_count
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Typical Growth Rates

**bronze.adp_tenure_history**:
- ~150-200 rows per week (depends on employee count)
- ~7,800-10,400 rows per year
- Estimated size: 1-2 MB per year

**silver.fact_active_headcount**:
- ~10-15 rows per week (depends on department count)
- ~520-780 rows per year
- Estimated size: < 1 MB per year

### Archival Strategy

Current retention: **Indefinite** (keep all historical data)

Future considerations:
- Archive data older than 7 years to cold storage
- Implement table partitioning by year for performance

---

## Schema Change Process

When updating schemas:

1. **Document the change** in this file
2. **Write migration SQL** for production
3. **Test migration** on dev/staging database
4. **Update pipeline code** to use new schema
5. **Execute migration** on production (during maintenance window)
6. **Verify data** post-migration

Example migration:

```sql
-- Add new column to bronze table
ALTER TABLE bronze.adp_tenure_history
    ADD COLUMN employment_status VARCHAR(20);

-- Backfill existing records (if needed)
UPDATE bronze.adp_tenure_history
    SET employment_status = 'ACTIVE'
    WHERE employment_status IS NULL;

-- Update pipeline code to populate new column
```

---

## Access Control

### Recommended Permissions

**Read-only user** (for BI tools, analysts):
```sql
GRANT USAGE ON SCHEMA bronze, silver, gold TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze, silver, gold TO readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze, silver, gold
    GRANT SELECT ON TABLES TO readonly_user;
```

**ETL user** (for data pipelines):
```sql
GRANT USAGE ON SCHEMA bronze, silver TO etl_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bronze, silver TO etl_user;
GRANT CREATE ON SCHEMA bronze, silver TO etl_user;
```

**Admin user** (for schema changes):
```sql
GRANT ALL PRIVILEGES ON SCHEMA bronze, silver, gold TO admin_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze, silver, gold TO admin_user;
```

---

**Last Updated**: 2025-11-03
**Version**: 1.0.0
