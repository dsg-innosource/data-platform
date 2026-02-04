#!/usr/bin/env python3
"""
Resume Fraud Detection

Detects potentially fraudulent resumes using employment history fingerprinting,
multi-resume divergence analysis, and professional summary matching.

Usage:
    python resume-fraud-detection/process.py
    python resume-fraud-detection/process.py --dry-run
    python resume-fraud-detection/process.py --lookback 60
    python resume-fraud-detection/process.py --pdf
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def load_config():
    """Load main configuration and shared database config."""
    # Load environment variables from .env file
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    # Load process-specific config
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Load shared database config
    shared_db_path = Path(__file__).parent.parent / "shared" / "database.yaml"
    with open(shared_db_path) as f:
        shared_config = yaml.safe_load(f)

    # Merge database config
    config["database"] = shared_config["database"]

    # Expand environment variables in database config
    db = config["database"]
    for key in db:
        if isinstance(db[key], str) and db[key].startswith("${"):
            env_var = db[key][2:-1]
            db[key] = os.environ.get(env_var, "")

    return config


def get_db_connection(config):
    """Create database connection."""
    db = config["database"]
    connection_string = (
        f"postgresql://{db['username']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['database']}"
        f"?sslmode={db['sslmode']}"
    )
    return create_engine(connection_string)


def setup_output_dir():
    """Create date-stamped output directory for this run."""
    today = datetime.now().strftime("%Y-%m-%d")
    output_dir = Path(__file__).parent / "output" / today
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


# --------------------------------------
# Date Filter Helper
# --------------------------------------

def build_date_filter(config, column, lookback_key):
    """Build a SQL date filter clause for the given column.

    If --start-date/--end-date were provided, uses a BETWEEN clause.
    Otherwise uses CURRENT_DATE - INTERVAL based on the config lookback.
    """
    date_range = config.get("_date_range")
    if date_range:
        return f"{column} BETWEEN '{date_range['start']}'::date AND '{date_range['end']}'::date + INTERVAL '1 day'"

    lookback = config["detection"][lookback_key]["lookback_days"]
    return f"{column} >= CURRENT_DATE - INTERVAL '{lookback} days'"


def describe_date_filter(config, lookback_key):
    """Return a human-readable description of the date filter for SQL comments."""
    date_range = config.get("_date_range")
    if date_range:
        return f"Period: {date_range['start']} to {date_range['end']}"

    lookback = config["detection"][lookback_key]["lookback_days"]
    return f"Lookback: {lookback} days"


# --------------------------------------
# Detection Method 1: Employment History Matching
# --------------------------------------

def build_employment_history_query(config):
    """Build SQL for employment history fingerprint matching.

    Runs two sub-approaches:
    - Option A: Hash structured employer fields from applications table
    - Option B: Hash experience text from documents/resume scores
    Groups by hash and flags when 3+ unique applicants share the same fingerprint.
    """
    tables = config["tables"]
    detection = config["detection"]["employment_history"]
    threshold = detection["threshold"]
    date_desc = describe_date_filter(config, "employment_history")
    app_date_filter = build_date_filter(config, "app.created_at", "employment_history")
    doc_date_filter = build_date_filter(config, "d.created_at", "employment_history")

    query = f"""
-- Resume Fraud Detection: Employment History Matching
-- Generated: {datetime.now().isoformat()}
-- Threshold: {threshold}+ unique applicants with identical employment fingerprint
-- {date_desc}

WITH
-- Option A: Structured fields from applications table
employment_fingerprints AS (
    SELECT
        a.id AS applicant_id,
        a.first_name,
        a.last_name,
        a.email,
        MD5(LOWER(CONCAT_WS('|',
            COALESCE(NULLIF(TRIM(app.previous_employer_1_name), ''), 'none'),
            COALESCE(app.previous_employer_1_start_date::text, ''),
            COALESCE(app.previous_employer_1_end_date::text, ''),
            COALESCE(NULLIF(TRIM(app.previous_employer_2_name), ''), 'none'),
            COALESCE(app.previous_employer_2_start_date::text, ''),
            COALESCE(app.previous_employer_2_end_date::text, ''),
            COALESCE(NULLIF(TRIM(app.previous_employer_3_name), ''), 'none'),
            COALESCE(app.previous_employer_3_start_date::text, ''),
            COALESCE(app.previous_employer_3_end_date::text, '')
        ))) AS employment_hash,
        CONCAT_WS(' | ',
            CASE WHEN app.previous_employer_1_name IS NOT NULL
                      AND TRIM(app.previous_employer_1_name) != ''
                 THEN app.previous_employer_1_name || ' (' ||
                      COALESCE(app.previous_employer_1_start_date::text, '?') || ' to ' ||
                      COALESCE(app.previous_employer_1_end_date::text, 'present') || ')'
            END,
            CASE WHEN app.previous_employer_2_name IS NOT NULL
                      AND TRIM(app.previous_employer_2_name) != ''
                 THEN app.previous_employer_2_name || ' (' ||
                      COALESCE(app.previous_employer_2_start_date::text, '?') || ' to ' ||
                      COALESCE(app.previous_employer_2_end_date::text, 'present') || ')'
            END,
            CASE WHEN app.previous_employer_3_name IS NOT NULL
                      AND TRIM(app.previous_employer_3_name) != ''
                 THEN app.previous_employer_3_name || ' (' ||
                      COALESCE(app.previous_employer_3_start_date::text, '?') || ' to ' ||
                      COALESCE(app.previous_employer_3_end_date::text, 'present') || ')'
            END
        ) AS employment_summary,
        'structured_fields' AS hash_source,
        app.created_at
    FROM {tables['applicants']} a
    JOIN {tables['applications']} app ON a.id = app.applicant_id
    WHERE {app_date_filter}
      AND a.deleted_at IS NULL
      AND app.previous_employer_1_name IS NOT NULL
      AND TRIM(app.previous_employer_1_name) != ''
),

-- Option B: Experience text from resume scores
experience_fingerprints AS (
    SELECT
        a.id AS applicant_id,
        a.first_name,
        a.last_name,
        a.email,
        MD5(COALESCE(
            LOWER(TRIM(rs.resume_only_experience)),
            'no_experience_found'
        )) AS employment_hash,
        COALESCE(
            LEFT(rs.resume_only_experience, 300),
            'No experience extracted'
        ) AS employment_summary,
        'experience_text' AS hash_source,
        d.created_at
    FROM {tables['applicants']} a
    JOIN {tables['documents']} d ON a.id = d.applicant_id
    LEFT JOIN {tables['resume_scores']} rs ON a.id = rs.applicant_id
    WHERE {doc_date_filter}
      AND a.deleted_at IS NULL
      AND d.is_resume = TRUE
      AND d.resume_text IS NOT NULL
      AND LENGTH(d.resume_text) > 500
      AND rs.resume_only_experience IS NOT NULL
      AND LENGTH(TRIM(rs.resume_only_experience)) > 100
),

-- Combine both approaches
all_fingerprints AS (
    SELECT * FROM employment_fingerprints
    UNION ALL
    SELECT * FROM experience_fingerprints
),

-- Find suspicious groups
suspicious_groups AS (
    SELECT
        employment_hash,
        hash_source,
        COUNT(DISTINCT applicant_id) AS ring_size,
        MIN(employment_summary) AS employment_summary
    FROM all_fingerprints
    GROUP BY employment_hash, hash_source
    HAVING COUNT(DISTINCT applicant_id) >= {threshold}
)

SELECT
    af.applicant_id,
    af.first_name,
    af.last_name,
    af.email,
    af.employment_hash,
    af.hash_source,
    sg.ring_size,
    sg.employment_summary,
    af.created_at
FROM suspicious_groups sg
JOIN all_fingerprints af
    ON sg.employment_hash = af.employment_hash
    AND sg.hash_source = af.hash_source
ORDER BY sg.ring_size DESC, sg.employment_hash, af.last_name;
"""
    return query


# --------------------------------------
# Detection Method 2: Multi-Resume Divergence
# --------------------------------------

def build_multi_resume_query(config):
    """Build SQL for multi-resume divergence detection.

    Finds applicants who submit significantly different resumes across
    multiple applications. Flags identity issues when same email maps
    to multiple applicant IDs.
    """
    tables = config["tables"]
    detection = config["detection"]["multi_resume"]
    min_distinct = detection["min_distinct_resumes"]
    date_desc = describe_date_filter(config, "multi_resume")
    date_filter = build_date_filter(config, "d.created_at", "multi_resume")

    query = f"""
-- Resume Fraud Detection: Multi-Resume Divergence
-- Generated: {datetime.now().isoformat()}
-- {date_desc}
-- Minimum distinct resumes: {min_distinct}

WITH applicant_resumes AS (
    SELECT
        a.email,
        a.id AS applicant_id,
        a.first_name || ' ' || a.last_name AS applicant_name,
        d.id AS document_id,
        d.created_at,
        MD5(LOWER(SUBSTRING(d.resume_text FROM 1 FOR 1000))) AS content_hash,
        LEFT(d.resume_text, 500) AS resume_preview
    FROM {tables['applicants']} a
    JOIN {tables['documents']} d ON a.id = d.applicant_id
    WHERE {date_filter}
      AND d.is_resume = TRUE
      AND d.resume_text IS NOT NULL
      AND LENGTH(d.resume_text) > 500
      AND a.email IS NOT NULL
      AND a.deleted_at IS NULL
),

email_resume_counts AS (
    SELECT
        email,
        COUNT(DISTINCT document_id) AS resume_count,
        COUNT(DISTINCT content_hash) AS unique_content_count,
        COUNT(DISTINCT applicant_id) AS applicant_id_count,
        ARRAY_AGG(DISTINCT applicant_name ORDER BY applicant_name) AS names_used,
        ARRAY_AGG(DISTINCT applicant_id ORDER BY applicant_id) AS applicant_ids,
        MIN(created_at) AS first_upload,
        MAX(created_at) AS last_upload
    FROM applicant_resumes
    GROUP BY email
    HAVING COUNT(DISTINCT document_id) >= {min_distinct}
)

SELECT
    erc.email,
    erc.resume_count,
    erc.unique_content_count,
    erc.applicant_id_count,
    erc.names_used,
    erc.applicant_ids,
    erc.first_upload::date AS first_upload,
    erc.last_upload::date AS last_upload,
    CASE
        WHEN erc.unique_content_count = erc.resume_count
            THEN 'HIGH - All resumes different'
        WHEN erc.unique_content_count > erc.resume_count * 0.5
            THEN 'MEDIUM - Most resumes different'
        ELSE 'LOW - Mostly consistent'
    END AS divergence_level,
    CASE
        WHEN erc.applicant_id_count > 1
            THEN 'ALERT: Multiple applicant IDs for same email'
        ELSE 'Single applicant ID'
    END AS identity_flag
FROM email_resume_counts erc
WHERE erc.unique_content_count > 1
  AND (
    erc.unique_content_count = erc.resume_count
    OR erc.applicant_id_count > 1
  )
ORDER BY
    CASE WHEN erc.applicant_id_count > 1 THEN 0 ELSE 1 END,
    erc.unique_content_count DESC,
    erc.resume_count DESC;
"""
    return query


# --------------------------------------
# Detection Method 3: Professional Summary Matching
# --------------------------------------

def build_summary_match_query(config):
    """Build SQL for professional summary matching.

    Secondary signal only - uses MD5 hash of extracted professional summary
    section. Higher threshold (5+) since people commonly copy intro templates.
    """
    tables = config["tables"]
    detection = config["detection"]["professional_summary"]
    threshold = detection["threshold"]
    date_desc = describe_date_filter(config, "professional_summary")
    date_filter = build_date_filter(config, "d.created_at", "professional_summary")

    query = f"""
-- Resume Fraud Detection: Professional Summary Matching (SECONDARY SIGNAL)
-- Generated: {datetime.now().isoformat()}
-- Threshold: {threshold}+ unique applicants sharing identical summary
-- {date_desc}
-- NOTE: This is a corroborating signal only, not a standalone fraud indicator

WITH summary_extracts AS (
    SELECT
        a.id AS applicant_id,
        a.first_name,
        a.last_name,
        a.email,
        SUBSTRING(d.resume_text
            FROM '(?i)(?:professional summary|summary of qualifications|summary|profile)[\\s\\n]+(.{{100,200}})') AS professional_summary,
        MD5(LOWER(TRIM(SUBSTRING(d.resume_text
            FROM '(?i)(?:professional summary|summary of qualifications|summary|profile)[\\s\\n]+(.{{100,200}})')))) AS summary_hash,
        d.created_at
    FROM {tables['applicants']} a
    JOIN {tables['documents']} d ON a.id = d.applicant_id
    WHERE {date_filter}
      AND a.deleted_at IS NULL
      AND d.is_resume = TRUE
      AND d.resume_text IS NOT NULL
),

suspicious_summaries AS (
    SELECT
        summary_hash,
        COUNT(DISTINCT applicant_id) AS ring_size
    FROM summary_extracts
    WHERE professional_summary IS NOT NULL
      AND LENGTH(TRIM(professional_summary)) > 20
    GROUP BY summary_hash
    HAVING COUNT(DISTINCT applicant_id) >= {threshold}
)

SELECT
    se.applicant_id,
    se.first_name,
    se.last_name,
    se.email,
    se.summary_hash,
    ss.ring_size,
    LEFT(se.professional_summary, 200) AS summary_preview,
    se.created_at
FROM suspicious_summaries ss
JOIN summary_extracts se ON ss.summary_hash = se.summary_hash
ORDER BY ss.ring_size DESC, ss.summary_hash, se.last_name;
"""
    return query


# --------------------------------------
# Query Execution
# --------------------------------------

def run_detection_method(engine, query, method_name, output_dir):
    """Run a detection method query and save results."""
    print(f"\nRunning detection: {method_name}")

    # Save query for transparency
    query_file = output_dir / f"{method_name}_query.sql"
    with open(query_file, "w") as f:
        f.write(query)
    print(f"  Query saved: {query_file.name}")

    # Execute query
    # Use exec_driver_sql to avoid SQLAlchemy interpreting regex patterns as bind params
    try:
        with engine.connect() as conn:
            result = conn.exec_driver_sql(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(f"  Results: {len(df)} rows flagged")

        # Save CSV
        csv_file = output_dir / f"{method_name}_results.csv"
        df.to_csv(csv_file, index=False)
        print(f"  CSV saved: {csv_file.name}")

        return df

    except Exception as e:
        print(f"  ERROR: {e}")
        return None


# --------------------------------------
# Signal Classification
# --------------------------------------

def get_applicant_info(applicant_id, *dataframes):
    """Extract applicant name/email from whichever DataFrame has the info."""
    for df in dataframes:
        if df is None:
            continue
        row = df[df["applicant_id"] == applicant_id]
        if not row.empty:
            first_row = row.iloc[0]
            return {
                "first_name": first_row.get("first_name", ""),
                "last_name": first_row.get("last_name", ""),
                "email": first_row.get("email", ""),
            }
    return {"first_name": "", "last_name": "", "email": ""}


def build_detail_string(applicant_id, emp_df, multi_df, summary_df):
    """Build a human-readable detail string for the combined report."""
    details = []

    if emp_df is not None:
        rows = emp_df[emp_df["applicant_id"] == applicant_id]
        if not rows.empty:
            row = rows.iloc[0]
            source = row.get("hash_source", "unknown")
            ring = row.get("ring_size", "?")
            details.append(f"Employment match ({source}, ring={ring})")

    if multi_df is not None and "applicant_ids" in multi_df.columns:
        # Multi-resume uses array columns, check if applicant_id appears
        for _, row in multi_df.iterrows():
            ids = row.get("applicant_ids", [])
            if ids is not None and applicant_id in ids:
                level = row.get("divergence_level", "?")
                flag = row.get("identity_flag", "")
                details.append(f"Resume divergence ({level}; {flag})")
                break

    if summary_df is not None:
        rows = summary_df[summary_df["applicant_id"] == applicant_id]
        if not rows.empty:
            row = rows.iloc[0]
            ring = row.get("ring_size", "?")
            details.append(f"Summary match (ring={ring})")

    return "; ".join(details) if details else "No details"


def classify_signals(emp_df, multi_df, summary_df):
    """Combine detection results into unified signal classification."""
    results = []

    # Build lookup sets
    emp_ids = set(emp_df["applicant_id"].unique()) if emp_df is not None and len(emp_df) > 0 else set()
    summary_ids = set(summary_df["applicant_id"].unique()) if summary_df is not None and len(summary_df) > 0 else set()

    # Multi-resume results use applicant_ids array column
    multi_applicant_ids = set()
    multi_email_lookup = {}
    if multi_df is not None and len(multi_df) > 0 and "applicant_ids" in multi_df.columns:
        for _, row in multi_df.iterrows():
            ids = row.get("applicant_ids", [])
            if ids is not None:
                for aid in ids:
                    multi_applicant_ids.add(aid)
                    multi_email_lookup[aid] = row.get("email", "")

    all_ids = emp_ids | summary_ids | multi_applicant_ids

    if not all_ids:
        print("  No signals detected across any method")
        return pd.DataFrame(columns=[
            "applicant_id", "first_name", "last_name", "email",
            "severity", "signal_type", "details"
        ])

    for applicant_id in all_ids:
        in_emp = applicant_id in emp_ids
        in_summary = applicant_id in summary_ids
        in_multi = applicant_id in multi_applicant_ids

        # Classification logic
        if in_emp and in_summary:
            severity = "HIGH"
            signal = "employment_and_summary_match"
        elif in_emp:
            severity = "HIGH"
            signal = "employment_history_match"
        elif in_multi:
            severity = "MEDIUM"
            signal = "multi_resume_divergence"
        elif in_summary:
            severity = "LOW"
            signal = "summary_match_only"
        else:
            continue

        # Gather applicant info
        info = get_applicant_info(applicant_id, emp_df, summary_df)
        # If not found in emp/summary, check multi-resume email
        if not info["email"] and applicant_id in multi_email_lookup:
            info["email"] = multi_email_lookup[applicant_id]

        details = build_detail_string(applicant_id, emp_df, multi_df, summary_df)

        results.append({
            "applicant_id": applicant_id,
            "first_name": info.get("first_name", ""),
            "last_name": info.get("last_name", ""),
            "email": info.get("email", ""),
            "severity": severity,
            "signal_type": signal,
            "details": details,
        })

    df = pd.DataFrame(results)
    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    df["_sort"] = df["severity"].map(severity_order)
    df = df.sort_values(["_sort", "signal_type", "last_name"]).drop(columns=["_sort"])
    df = df.reset_index(drop=True)
    return df


# --------------------------------------
# Report Generation
# --------------------------------------

def generate_report(config, results, method_results, output_dir):
    """Generate markdown fraud detection report."""
    report_lines = []
    detection = config["detection"]

    # Header
    report_lines.append("# Resume Fraud Detection Report")
    report_lines.append("")
    report_lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}  ")
    date_range = config.get("_date_range")
    if date_range:
        report_lines.append(f"**Analysis Period:** {date_range['start']} to {date_range['end']}  ")
    else:
        report_lines.append(f"**Employment History Lookback:** {detection['employment_history']['lookback_days']} days  ")
        report_lines.append(f"**Multi-Resume Lookback:** {detection['multi_resume']['lookback_days']} days  ")
        report_lines.append(f"**Summary Lookback:** {detection['professional_summary']['lookback_days']} days  ")
    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")

    # Methodology
    report_lines.append("## Methodology")
    report_lines.append("")
    report_lines.append("This report uses three detection methods to identify potentially fraudulent resumes:")
    report_lines.append("")
    report_lines.append("### Method 1: Employment History Matching (PRIMARY)")
    report_lines.append("")
    report_lines.append("Identifies groups of applicants with identical employment fingerprints. Two approaches are combined:")
    report_lines.append("")
    report_lines.append("- **Structured Fields**: Hashes the employer names and dates from application form fields (`previous_employer_1_name`, start/end dates, etc.)")
    report_lines.append("- **Resume Text**: Hashes the AI-extracted work experience section from parsed resumes")
    report_lines.append("")
    report_lines.append(f"**Threshold**: {detection['employment_history']['threshold']}+ unique applicants sharing identical fingerprint = flagged")
    report_lines.append("")
    report_lines.append("**Why this works**: Legitimate applicants rarely have identical employment histories. When 3+ people claim the exact same jobs at the same companies with the same dates, it strongly suggests coordinated fraud or resume mills.")
    report_lines.append("")
    report_lines.append("### Method 2: Multi-Resume Divergence (PRIMARY)")
    report_lines.append("")
    report_lines.append("Identifies applicants who submit significantly different resumes across multiple applications.")
    report_lines.append("")
    report_lines.append("- Hashes the first 1000 characters of each resume document")
    report_lines.append("- Groups by email address")
    report_lines.append("- Flags when all submitted resumes are completely different")
    report_lines.append("- Also detects identity issues (same email linked to multiple applicant IDs)")
    report_lines.append("")
    report_lines.append(f"**Threshold**: {detection['multi_resume']['min_distinct_resumes']}+ distinct resume versions from same email")
    report_lines.append("")
    report_lines.append("**Why this works**: While tailoring resumes is normal, having completely different resumes (different employers, different skills) suggests resume mills cycling through templates or credential fraud.")
    report_lines.append("")
    report_lines.append("### Method 3: Professional Summary Matching (SECONDARY)")
    report_lines.append("")
    report_lines.append("Identifies groups of applicants with identical professional summary/objective sections.")
    report_lines.append("")
    report_lines.append("- Extracts the \"Professional Summary\" or \"Profile\" section via regex")
    report_lines.append("- Hashes the extracted text")
    report_lines.append("- Groups by hash")
    report_lines.append("")
    report_lines.append(f"**Threshold**: {detection['professional_summary']['threshold']}+ unique applicants (higher threshold)")
    report_lines.append("")
    report_lines.append("**Why this is SECONDARY**: Professional summaries have high false positive rates. People commonly copy intro templates from Google without fraudulent intent. This signal is only meaningful when combined with Method 1 or 2.")
    report_lines.append("")
    report_lines.append("### Signal Classification")
    report_lines.append("")
    report_lines.append("| Severity | Condition | Recommended Action |")
    report_lines.append("|----------|-----------|-------------------|")
    report_lines.append("| HIGH | Employment history match (with or without summary match) | Immediate investigation |")
    report_lines.append("| MEDIUM | Multi-resume divergence | Manual review |")
    report_lines.append("| LOW | Professional summary match only | Monitor only (likely false positive) |")
    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")

    # Executive Summary
    report_lines.append("## Executive Summary")
    report_lines.append("")

    if len(results) == 0:
        report_lines.append("No fraud signals detected in this analysis period.")
        report_lines.append("")
    else:
        high_count = len(results[results["severity"] == "HIGH"])
        med_count = len(results[results["severity"] == "MEDIUM"])
        low_count = len(results[results["severity"] == "LOW"])
        total = len(results)

        report_lines.append(f"**Total flagged applicants:** {total}")
        report_lines.append("")
        report_lines.append("| Severity | Count | Action |")
        report_lines.append("|----------|-------|--------|")
        report_lines.append(f"| HIGH | {high_count} | Immediate investigation |")
        report_lines.append(f"| MEDIUM | {med_count} | Manual review |")
        report_lines.append(f"| LOW | {low_count} | Monitor only |")
        report_lines.append("")

    # Method 1: Employment History
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## Method 1: Employment History Matching")
    report_lines.append("")

    emp_df = method_results.get("employment_history")
    if emp_df is not None and len(emp_df) > 0:
        # Count unique rings
        unique_rings = emp_df.groupby(["employment_hash", "hash_source"]).agg(
            ring_size=("applicant_id", "nunique")
        ).reset_index()
        report_lines.append(f"**Suspicious groups found:** {len(unique_rings)}")
        report_lines.append(f"**Largest ring size:** {unique_rings['ring_size'].max()}")
        report_lines.append(f"**Total applicants flagged:** {emp_df['applicant_id'].nunique()}")
        report_lines.append("")

        # Top rings table
        report_lines.append("### Flagged Applicants")
        report_lines.append("")
        report_lines.append("| Name | Email | Hash Source | Ring Size | Employment Summary |")
        report_lines.append("|------|-------|-------------|-----------|-------------------|")

        for _, row in emp_df.head(50).iterrows():
            name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
            email = str(row.get("email", ""))[:30]
            source = row.get("hash_source", "")
            ring = row.get("ring_size", "")
            summary = str(row.get("employment_summary", ""))[:60]
            report_lines.append(f"| {name} | {email} | {source} | {ring} | {summary} |")

        report_lines.append("")
    else:
        report_lines.append("No employment history matches detected.")
        report_lines.append("")

    # Method 2: Multi-Resume Divergence
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## Method 2: Multi-Resume Divergence")
    report_lines.append("")

    multi_df = method_results.get("multi_resume_divergence")
    if multi_df is not None and len(multi_df) > 0:
        report_lines.append(f"**Emails flagged:** {multi_df['email'].nunique()}")
        report_lines.append("")

        # Identity alerts first
        identity_alerts = multi_df[multi_df["identity_flag"].str.contains("ALERT", na=False)]
        if len(identity_alerts) > 0:
            report_lines.append("### Identity Alerts (Multiple Applicant IDs)")
            report_lines.append("")
            report_lines.append("| Email | Resume Count | Unique Versions | Applicant IDs | Divergence |")
            report_lines.append("|-------|-------------|-----------------|---------------|------------|")
            for _, row in identity_alerts.iterrows():
                report_lines.append(
                    f"| {row.get('email', '')} | {row.get('resume_count', '')} | "
                    f"{row.get('unique_content_count', '')} | {row.get('applicant_id_count', '')} | "
                    f"{row.get('divergence_level', '')} |"
                )
            report_lines.append("")

        # All divergence flags
        report_lines.append("### All Divergence Flags")
        report_lines.append("")
        report_lines.append("| Email | Resumes | Unique Versions | Divergence Level | Identity |")
        report_lines.append("|-------|---------|-----------------|------------------|----------|")
        for _, row in multi_df.head(50).iterrows():
            report_lines.append(
                f"| {row.get('email', '')} | {row.get('resume_count', '')} | "
                f"{row.get('unique_content_count', '')} | {row.get('divergence_level', '')} | "
                f"{row.get('identity_flag', '')} |"
            )
        report_lines.append("")
    else:
        report_lines.append("No multi-resume divergence detected.")
        report_lines.append("")

    # Method 3: Professional Summary
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## Method 3: Professional Summary Matching")
    report_lines.append("")
    report_lines.append("*Secondary signal only - corroborating evidence, not standalone fraud indicator.*")
    report_lines.append("")

    summary_df = method_results.get("professional_summary")
    if summary_df is not None and len(summary_df) > 0:
        unique_rings = summary_df.groupby("summary_hash").agg(
            ring_size=("applicant_id", "nunique")
        ).reset_index()
        report_lines.append(f"**Summary groups found:** {len(unique_rings)}")
        report_lines.append(f"**Largest ring size:** {unique_rings['ring_size'].max()}")
        report_lines.append(f"**Total applicants flagged:** {summary_df['applicant_id'].nunique()}")
        report_lines.append("")

        report_lines.append("| Name | Email | Ring Size | Summary Preview |")
        report_lines.append("|------|-------|-----------|-----------------|")
        for _, row in summary_df.head(30).iterrows():
            name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
            email = str(row.get("email", ""))[:30]
            ring = row.get("ring_size", "")
            preview = str(row.get("summary_preview", ""))[:80]
            report_lines.append(f"| {name} | {email} | {ring} | {preview} |")
        report_lines.append("")
    else:
        report_lines.append("No professional summary matches detected.")
        report_lines.append("")

    # Combined Classification
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## Combined Signal Classification")
    report_lines.append("")

    if len(results) > 0:
        report_lines.append("| Severity | Applicant ID | Name | Email | Signal Type | Details |")
        report_lines.append("|----------|-------------|------|-------|-------------|---------|")
        for _, row in results.iterrows():
            name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
            report_lines.append(
                f"| {row['severity']} | {row['applicant_id']} | {name} | "
                f"{row.get('email', '')} | {row['signal_type']} | {row.get('details', '')} |"
            )
        report_lines.append("")
    else:
        report_lines.append("No signals to classify.")
        report_lines.append("")

    # Deliverables
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## Deliverables")
    report_lines.append("")
    report_lines.append("**CSV Files:**")
    report_lines.append("- `employment_history_results.csv`")
    report_lines.append("- `multi_resume_divergence_results.csv`")
    report_lines.append("- `professional_summary_results.csv`")
    report_lines.append("- `combined_signals.csv`")
    report_lines.append("")
    report_lines.append("**SQL Queries:**")
    report_lines.append("- `employment_history_query.sql`")
    report_lines.append("- `multi_resume_divergence_query.sql`")
    report_lines.append("- `professional_summary_query.sql`")
    report_lines.append("")

    # Write report
    report_content = "\n".join(report_lines)
    report_file = output_dir / "fraud_detection_report.md"
    with open(report_file, "w") as f:
        f.write(report_content)

    print(f"\nReport saved: {report_file.name}")
    return report_file


# --------------------------------------
# PDF Generation
# --------------------------------------

def generate_pdf(report_file, output_dir):
    """Generate PDF from markdown report using pandoc + weasyprint."""
    css_path = Path(__file__).parent / "assets" / "report-style.css"
    pdf_file = output_dir / "fraud_detection_report.pdf"

    cmd = [
        "pandoc",
        str(report_file),
        "-o", str(pdf_file),
        "--pdf-engine=weasyprint",
        f"--css={css_path}",
        "-f", "markdown",
        "--metadata", "title=Resume Fraud Detection Report",
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"PDF generated: {pdf_file.name}")
        return pdf_file
    except FileNotFoundError:
        print("WARNING: pandoc not found. Skipping PDF generation.")
        print("  Install with: brew install pandoc")
        return None
    except subprocess.CalledProcessError as e:
        print(f"WARNING: PDF generation failed: {e.stderr}")
        return None


# --------------------------------------
# Main
# --------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Detect potentially fraudulent resumes"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Generate SQL queries but don't execute",
    )
    parser.add_argument(
        "--lookback", type=int,
        help="Override default lookback days for all methods",
    )
    parser.add_argument(
        "--start-date",
        help="Start of analysis period (YYYY-MM-DD). Use with --end-date.",
    )
    parser.add_argument(
        "--end-date",
        help="End of analysis period (YYYY-MM-DD). Use with --start-date.",
    )
    parser.add_argument(
        "--pdf", action="store_true",
        help="Generate PDF report (requires pandoc + weasyprint)",
    )

    args = parser.parse_args()

    # Validate date range args
    if args.start_date and not args.end_date:
        parser.error("--start-date requires --end-date")
    if args.end_date and not args.start_date:
        parser.error("--end-date requires --start-date")
    if args.start_date and args.lookback:
        parser.error("Cannot use --lookback with --start-date/--end-date")

    # Load configuration
    print("Loading configuration...")
    config = load_config()

    # Apply date range or lookback override
    if args.start_date:
        config["_date_range"] = {
            "start": args.start_date,
            "end": args.end_date,
        }
        print(f"  Analysis period: {args.start_date} to {args.end_date}")
    elif args.lookback:
        config["detection"]["employment_history"]["lookback_days"] = args.lookback
        config["detection"]["multi_resume"]["lookback_days"] = args.lookback
        config["detection"]["professional_summary"]["lookback_days"] = args.lookback
        print(f"  Lookback override: {args.lookback} days for all methods")

    # Set up output directory
    output_dir = setup_output_dir()
    print(f"Output directory: {output_dir}")

    # Build queries
    print("\nBuilding detection queries...")
    emp_query = build_employment_history_query(config)
    multi_query = build_multi_resume_query(config)
    summary_query = build_summary_match_query(config)

    if args.dry_run:
        print("\n[DRY RUN - Queries generated but not executed]")
        for name, query in [
            ("employment_history", emp_query),
            ("multi_resume_divergence", multi_query),
            ("professional_summary", summary_query),
        ]:
            query_file = output_dir / f"{name}_query.sql"
            with open(query_file, "w") as f:
                f.write(query)
            print(f"  Saved: {query_file}")
        return

    # Connect to database
    print("\nConnecting to database...")
    engine = get_db_connection(config)

    # Run all detection methods
    emp_df = run_detection_method(
        engine, emp_query, "employment_history", output_dir
    )
    multi_df = run_detection_method(
        engine, multi_query, "multi_resume_divergence", output_dir
    )
    summary_df = run_detection_method(
        engine, summary_query, "professional_summary", output_dir
    )

    # Classify combined signals
    print("\nClassifying signals...")
    results = classify_signals(emp_df, multi_df, summary_df)

    # Save combined results
    combined_file = output_dir / "combined_signals.csv"
    results.to_csv(combined_file, index=False)
    print(f"Combined signals saved: {combined_file.name}")

    # Generate report
    method_results = {
        "employment_history": emp_df,
        "multi_resume_divergence": multi_df,
        "professional_summary": summary_df,
    }
    report_file = generate_report(config, results, method_results, output_dir)

    # Generate PDF if requested
    if args.pdf:
        generate_pdf(report_file, output_dir)

    # Summary
    print("\n" + "=" * 50)
    print("Fraud detection complete!")
    if len(results) > 0:
        high_count = len(results[results["severity"] == "HIGH"])
        med_count = len(results[results["severity"] == "MEDIUM"])
        low_count = len(results[results["severity"] == "LOW"])
        print(f"  HIGH: {high_count} | MEDIUM: {med_count} | LOW: {low_count}")
    else:
        print("  No fraud signals detected")
    print(f"  Output: {output_dir}")


if __name__ == "__main__":
    main()
