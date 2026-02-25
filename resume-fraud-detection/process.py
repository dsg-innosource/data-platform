#!/usr/bin/env python3
"""
Resume Fraud Detection

Detects potentially fraudulent resumes using employment history fingerprinting,
shared text block detection, multi-resume divergence analysis, and professional
summary matching.

Usage:
    python resume-fraud-detection/process.py
    python resume-fraud-detection/process.py --dry-run
    python resume-fraud-detection/process.py --lookback 60
    python resume-fraud-detection/process.py --pdf
    python resume-fraud-detection/process.py --seed "user@example.com" --start-date 2026-01-01 --end-date 2026-02-20
    python resume-fraud-detection/process.py --seed "user@example.com" --start-date 2026-01-01 --end-date 2026-02-20 --pdf
    python resume-fraud-detection/process.py --req 9175 --lookback 30 --pdf
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
# Requisition Filter Helper
# --------------------------------------

def build_req_filter(config, applicant_alias="a"):
    """Build SQL filter to constrain results to applicants on a specific requisition.

    Returns an empty string when no --req flag was provided.
    Uses portal_requisition_statistics to link applicants to requisitions.
    """
    req_key = config.get("_req_filter")
    if not req_key:
        return ""
    # Match by numeric requisition ID or by requisition_key substring
    # Use %% to escape percent signs since exec_driver_sql uses % as param markers
    return f"""
      AND {applicant_alias}.id IN (
          SELECT rs_req.applicant_id
          FROM bronze.portal_requisition_statistics rs_req
          JOIN bronze.portal_requisitions r_req ON rs_req.requisition_id = r_req.id
          WHERE r_req.id = {req_key}
             OR r_req.requisition_key LIKE '%%{req_key}%%'
      )"""


def describe_req_filter(config):
    """Return a human-readable description of the requisition filter for SQL comments."""
    req_key = config.get("_req_filter")
    if req_key:
        return f"Requisition filter: #{req_key}"
    return ""


# --------------------------------------
# Seed Applicant Lookup
# --------------------------------------

def lookup_seed_applicant(engine, config):
    """Look up a seed applicant by email and return their fingerprints.

    Returns a dict with applicant info, employment hash, and summary hash.
    Exits with error if email not found.
    """
    tables = config["tables"]
    seed_email = config["_seed_email"]

    query = f"""
    SELECT
        a.id AS applicant_id,
        a.first_name,
        a.last_name,
        a.email,
        MD5(COALESCE(
            LOWER(TRIM(rs.resume_only_experience)),
            'no_experience_found'
        )) AS employment_hash,
        MD5(LOWER(TRIM(SUBSTRING(d.resume_text
            FROM '(?i)(?:professional summary|summary of qualifications|summary|profile)[\\s\\n]+(.{{100,200}})')))) AS summary_hash
    FROM {tables['applicants']} a
    LEFT JOIN {tables['documents']} d
        ON a.id = d.applicant_id
        AND d.is_resume = TRUE
        AND d.resume_text IS NOT NULL
    LEFT JOIN {tables['resume_scores']} rs
        ON a.id = rs.applicant_id
    WHERE LOWER(a.email) = LOWER('{seed_email}')
      AND a.deleted_at IS NULL
    ORDER BY d.created_at DESC NULLS LAST
    LIMIT 1;
    """

    with engine.connect() as conn:
        result = conn.exec_driver_sql(query)
        columns = list(result.keys())
        rows = result.fetchall()

    if not rows:
        print(f"\nERROR: No applicant found with email '{seed_email}'")
        sys.exit(1)

    row = rows[0]
    seed_info = dict(zip(columns, row))

    print(f"\nSeed applicant found:")
    print(f"  Name: {seed_info['first_name']} {seed_info['last_name']}")
    print(f"  Email: {seed_info['email']}")
    print(f"  Applicant ID: {seed_info['applicant_id']}")
    print(f"  Employment hash: {seed_info['employment_hash']}")
    print(f"  Summary hash: {seed_info['summary_hash'] or 'None (no summary extracted)'}")

    # Warn about NULL hashes from LEFT JOINs
    if seed_info['employment_hash'] is None:
        print("  WARNING: No resume data found for this applicant")
    if seed_info['summary_hash'] is None:
        print("  WARNING: No professional summary extracted for this applicant")

    return seed_info


# --------------------------------------
# Detection Method 1: Employment History Matching
# --------------------------------------

def build_employment_history_query(config):
    """Build SQL for employment history fingerprint matching.

    Hashes the AI-extracted work experience section from parsed resumes.
    Groups by hash and flags when 3+ unique applicants share the same fingerprint.
    """
    tables = config["tables"]
    detection = config["detection"]["employment_history"]
    threshold = detection["threshold"]
    date_desc = describe_date_filter(config, "employment_history")
    doc_date_filter = build_date_filter(config, "d.created_at", "employment_history")
    req_filter = build_req_filter(config, "a")
    req_desc = describe_req_filter(config)

    query = f"""
-- Resume Fraud Detection: Employment History Matching
-- Generated: {datetime.now().isoformat()}
-- Threshold: {threshold}+ unique applicants with identical employment fingerprint
-- {date_desc}
-- {req_desc}
-- Method: MD5 hash of AI-extracted work experience from resume_scores table

WITH
-- Experience text fingerprints from resume scores
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
        d.created_at
    FROM {tables['applicants']} a
    JOIN {tables['documents']} d ON a.id = d.applicant_id
    JOIN {tables['resume_scores']} rs ON a.id = rs.applicant_id
    WHERE {doc_date_filter}
      AND a.deleted_at IS NULL
      AND d.is_resume = TRUE
      AND d.resume_text IS NOT NULL
      AND LENGTH(d.resume_text) > 500
      AND rs.resume_only_experience IS NOT NULL
      AND LENGTH(TRIM(rs.resume_only_experience)) > 100{req_filter}
),

-- Find suspicious groups (3+ applicants with identical experience fingerprint)
suspicious_groups AS (
    SELECT
        employment_hash,
        COUNT(DISTINCT applicant_id) AS ring_size,
        MIN(employment_summary) AS employment_summary
    FROM experience_fingerprints
    GROUP BY employment_hash
    HAVING COUNT(DISTINCT applicant_id) >= {threshold}
)

SELECT
    ef.applicant_id,
    ef.first_name,
    ef.last_name,
    ef.email,
    ef.employment_hash,
    sg.ring_size,
    sg.employment_summary,
    ef.created_at
FROM suspicious_groups sg
JOIN experience_fingerprints ef ON sg.employment_hash = ef.employment_hash
ORDER BY sg.ring_size DESC, sg.employment_hash, ef.last_name;
"""
    return query


def build_seeded_employment_query(config, seed_info):
    """Build employment history query filtered to the seed applicant's hash.

    Uses threshold of 2 (seed + 1 other) since finding even one match is significant.
    Returns an empty-result query if seed has no employment hash.
    """
    if seed_info.get('employment_hash') is None:
        return "-- Seed applicant has no employment data; skipping employment matching\nSELECT NULL AS applicant_id WHERE FALSE;"

    tables = config["tables"]
    seed_hash = seed_info['employment_hash']
    threshold = 2
    date_desc = describe_date_filter(config, "employment_history")
    doc_date_filter = build_date_filter(config, "d.created_at", "employment_history")
    req_filter = build_req_filter(config, "a")
    req_desc = describe_req_filter(config)

    query = f"""
-- Resume Fraud Detection: Seeded Employment History Matching
-- Generated: {datetime.now().isoformat()}
-- Seed: {seed_info['email']} (applicant_id={seed_info['applicant_id']})
-- Seed employment hash: {seed_hash}
-- Threshold: {threshold}+ unique applicants (reduced for seed mode)
-- {date_desc}
-- {req_desc}

WITH
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
        d.created_at
    FROM {tables['applicants']} a
    JOIN {tables['documents']} d ON a.id = d.applicant_id
    JOIN {tables['resume_scores']} rs ON a.id = rs.applicant_id
    WHERE {doc_date_filter}
      AND a.deleted_at IS NULL
      AND d.is_resume = TRUE
      AND d.resume_text IS NOT NULL
      AND LENGTH(d.resume_text) > 500
      AND rs.resume_only_experience IS NOT NULL
      AND LENGTH(TRIM(rs.resume_only_experience)) > 100{req_filter}
),

-- Filter to seed's employment hash only
suspicious_groups AS (
    SELECT
        employment_hash,
        COUNT(DISTINCT applicant_id) AS ring_size,
        MIN(employment_summary) AS employment_summary
    FROM experience_fingerprints
    WHERE employment_hash = '{seed_hash}'
    GROUP BY employment_hash
    HAVING COUNT(DISTINCT applicant_id) >= {threshold}
)

SELECT
    ef.applicant_id,
    ef.first_name,
    ef.last_name,
    ef.email,
    ef.employment_hash,
    sg.ring_size,
    sg.employment_summary,
    ef.created_at
FROM suspicious_groups sg
JOIN experience_fingerprints ef ON sg.employment_hash = ef.employment_hash
ORDER BY sg.ring_size DESC, sg.employment_hash, ef.last_name;
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
    req_filter = build_req_filter(config, "a")
    req_desc = describe_req_filter(config)

    query = f"""
-- Resume Fraud Detection: Multi-Resume Divergence
-- Generated: {datetime.now().isoformat()}
-- {date_desc}
-- {req_desc}
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
      AND a.deleted_at IS NULL{req_filter}
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


def build_seeded_multi_resume_query(config, seed_info):
    """Build multi-resume divergence query filtered to seed's email only.

    Checks if the seed applicant has submitted multiple divergent resumes.
    """
    tables = config["tables"]
    seed_email = seed_info['email']
    detection = config["detection"]["multi_resume"]
    min_distinct = detection["min_distinct_resumes"]
    date_desc = describe_date_filter(config, "multi_resume")
    date_filter = build_date_filter(config, "d.created_at", "multi_resume")
    req_filter = build_req_filter(config, "a")
    req_desc = describe_req_filter(config)

    query = f"""
-- Resume Fraud Detection: Seeded Multi-Resume Divergence
-- Generated: {datetime.now().isoformat()}
-- Seed: {seed_email}
-- {date_desc}
-- {req_desc}

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
      AND LOWER(a.email) = LOWER('{seed_email}'){req_filter}
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
    req_filter = build_req_filter(config, "a")
    req_desc = describe_req_filter(config)

    query = f"""
-- Resume Fraud Detection: Professional Summary Matching (SECONDARY SIGNAL)
-- Generated: {datetime.now().isoformat()}
-- Threshold: {threshold}+ unique applicants sharing identical summary
-- {date_desc}
-- {req_desc}
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
      AND d.resume_text IS NOT NULL{req_filter}
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


def build_seeded_summary_query(config, seed_info):
    """Build professional summary query filtered to the seed applicant's hash.

    Uses threshold of 2 (seed + 1 other) since finding even one match is significant.
    Returns an empty-result query if seed has no summary hash.
    """
    if seed_info.get('summary_hash') is None:
        return "-- Seed applicant has no summary data; skipping summary matching\nSELECT NULL AS applicant_id WHERE FALSE;"

    tables = config["tables"]
    seed_hash = seed_info['summary_hash']
    threshold = 2
    date_desc = describe_date_filter(config, "professional_summary")
    date_filter = build_date_filter(config, "d.created_at", "professional_summary")
    req_filter = build_req_filter(config, "a")
    req_desc = describe_req_filter(config)

    query = f"""
-- Resume Fraud Detection: Seeded Professional Summary Matching
-- Generated: {datetime.now().isoformat()}
-- Seed: {seed_info['email']} (applicant_id={seed_info['applicant_id']})
-- Seed summary hash: {seed_hash}
-- Threshold: {threshold}+ unique applicants (reduced for seed mode)
-- {date_desc}
-- {req_desc}
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
      AND d.resume_text IS NOT NULL{req_filter}
),

-- Filter to seed's summary hash only
suspicious_summaries AS (
    SELECT
        summary_hash,
        COUNT(DISTINCT applicant_id) AS ring_size
    FROM summary_extracts
    WHERE professional_summary IS NOT NULL
      AND LENGTH(TRIM(professional_summary)) > 20
      AND summary_hash = '{seed_hash}'
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
# Detection Method 4: Shared Text Block Detection
# --------------------------------------

def build_shared_text_blocks_query(config):
    """Build SQL for shared text block detection.

    Splits resume experience into paragraph-level chunks, hashes each chunk,
    and finds applicant pairs sharing multiple identical chunks. Catches
    partial overlap that Method 1's full-field hash misses.
    """
    tables = config["tables"]
    detection = config["detection"]["shared_text_blocks"]
    min_chunk_length = detection["min_chunk_length"]
    min_shared_chunks = detection["min_shared_chunks"]
    date_desc = describe_date_filter(config, "shared_text_blocks")
    doc_date_filter = build_date_filter(config, "d.created_at", "shared_text_blocks")
    req_filter = build_req_filter(config, "a")
    req_desc = describe_req_filter(config)

    query = f"""
-- Resume Fraud Detection: Shared Text Block Detection
-- Generated: {datetime.now().isoformat()}
-- Min chunk length: {min_chunk_length} chars
-- Min shared chunks: {min_shared_chunks}
-- {date_desc}
-- {req_desc}
-- Method: Split experience into paragraphs, MD5 each, find pairs sharing multiple chunks

WITH
-- Get applicants with experience text
experience_text AS (
    SELECT
        a.id AS applicant_id,
        a.first_name,
        a.last_name,
        a.email,
        rs.resume_only_experience,
        d.created_at
    FROM {tables['applicants']} a
    JOIN {tables['documents']} d ON a.id = d.applicant_id
    JOIN {tables['resume_scores']} rs ON a.id = rs.applicant_id
    WHERE {doc_date_filter}
      AND a.deleted_at IS NULL
      AND d.is_resume = TRUE
      AND d.resume_text IS NOT NULL
      AND LENGTH(d.resume_text) > 500
      AND rs.resume_only_experience IS NOT NULL
      AND LENGTH(TRIM(rs.resume_only_experience)) > {min_chunk_length}{req_filter}
),

-- Split experience into paragraph chunks and hash each
experience_chunks AS (
    SELECT
        et.applicant_id,
        et.first_name,
        et.last_name,
        et.email,
        et.created_at,
        TRIM(chunk) AS chunk_text,
        MD5(LOWER(TRIM(chunk))) AS chunk_hash
    FROM experience_text et,
         LATERAL unnest(regexp_split_to_array(et.resume_only_experience, E'\\n')) AS chunk
    WHERE LENGTH(TRIM(chunk)) >= {min_chunk_length}
),

-- Find applicant pairs sharing chunks
applicant_pairs AS (
    SELECT
        c1.applicant_id,
        c1.first_name,
        c1.last_name,
        c1.email,
        c2.applicant_id AS matched_applicant_id,
        c2.first_name AS matched_first_name,
        c2.last_name AS matched_last_name,
        c2.email AS matched_email,
        COUNT(DISTINCT c1.chunk_hash) AS shared_chunk_count,
        MIN(c1.chunk_text) AS sample_shared_text,
        c1.created_at
    FROM experience_chunks c1
    JOIN experience_chunks c2
        ON c1.chunk_hash = c2.chunk_hash
        AND c1.applicant_id < c2.applicant_id
    GROUP BY
        c1.applicant_id, c1.first_name, c1.last_name, c1.email,
        c2.applicant_id, c2.first_name, c2.last_name, c2.email,
        c1.created_at
    HAVING COUNT(DISTINCT c1.chunk_hash) >= {min_shared_chunks}
)

SELECT
    applicant_id,
    first_name,
    last_name,
    email,
    matched_applicant_id,
    matched_first_name,
    matched_last_name,
    matched_email,
    shared_chunk_count,
    LEFT(sample_shared_text, 200) AS sample_shared_text,
    created_at
FROM applicant_pairs
ORDER BY shared_chunk_count DESC, applicant_id;
"""
    return query


def build_seeded_shared_text_blocks_query(config, seed_info):
    """Build shared text block query anchored to the seed applicant.

    Splits the seed's experience into chunks and finds any other applicant
    sharing at least one identical chunk. Uses threshold of 1 since any
    shared chunk with a known fraud seed is significant.
    """
    tables = config["tables"]
    detection = config["detection"]["shared_text_blocks"]
    min_chunk_length = detection["min_chunk_length"]
    seed_id = seed_info['applicant_id']
    date_desc = describe_date_filter(config, "shared_text_blocks")
    doc_date_filter = build_date_filter(config, "d.created_at", "shared_text_blocks")
    req_filter = build_req_filter(config, "a")
    req_desc = describe_req_filter(config)

    query = f"""
-- Resume Fraud Detection: Seeded Shared Text Block Detection
-- Generated: {datetime.now().isoformat()}
-- Seed: {seed_info['email']} (applicant_id={seed_id})
-- Min chunk length: {min_chunk_length} chars
-- Threshold: 1+ shared chunks (reduced for seed mode)
-- {date_desc}
-- {req_desc}

WITH
-- Get seed applicant's experience chunks
seed_chunks AS (
    SELECT
        MD5(LOWER(TRIM(chunk))) AS chunk_hash,
        TRIM(chunk) AS chunk_text
    FROM {tables['resume_scores']} rs,
         LATERAL unnest(regexp_split_to_array(rs.resume_only_experience, E'\\n')) AS chunk
    WHERE rs.applicant_id = {seed_id}
      AND rs.resume_only_experience IS NOT NULL
      AND LENGTH(TRIM(chunk)) >= {min_chunk_length}
),

-- Get all other applicants' experience chunks in date range
other_chunks AS (
    SELECT
        a.id AS applicant_id,
        a.first_name,
        a.last_name,
        a.email,
        TRIM(chunk) AS chunk_text,
        MD5(LOWER(TRIM(chunk))) AS chunk_hash,
        d.created_at
    FROM {tables['applicants']} a
    JOIN {tables['documents']} d ON a.id = d.applicant_id
    JOIN {tables['resume_scores']} rs ON a.id = rs.applicant_id,
         LATERAL unnest(regexp_split_to_array(rs.resume_only_experience, E'\\n')) AS chunk
    WHERE {doc_date_filter}
      AND a.deleted_at IS NULL
      AND a.id != {seed_id}
      AND d.is_resume = TRUE
      AND d.resume_text IS NOT NULL
      AND LENGTH(d.resume_text) > 500
      AND rs.resume_only_experience IS NOT NULL
      AND LENGTH(TRIM(chunk)) >= {min_chunk_length}{req_filter}
)

SELECT
    oc.applicant_id,
    oc.first_name,
    oc.last_name,
    oc.email,
    COUNT(DISTINCT oc.chunk_hash) AS shared_chunk_count,
    LEFT(MIN(oc.chunk_text), 200) AS sample_shared_text,
    oc.created_at
FROM other_chunks oc
JOIN seed_chunks sc ON oc.chunk_hash = sc.chunk_hash
GROUP BY
    oc.applicant_id, oc.first_name, oc.last_name, oc.email, oc.created_at
HAVING COUNT(DISTINCT oc.chunk_hash) >= 1
ORDER BY shared_chunk_count DESC, oc.applicant_id;
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
            columns = list(result.keys())
            rows = result.fetchall()
            df = pd.DataFrame(rows, columns=columns)
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


def build_detail_string(applicant_id, emp_df, multi_df, summary_df, shared_blocks_df=None):
    """Build a human-readable detail string for the combined report."""
    details = []

    if emp_df is not None:
        rows = emp_df[emp_df["applicant_id"] == applicant_id]
        if not rows.empty:
            row = rows.iloc[0]
            ring = row.get("ring_size", "?")
            details.append(f"Employment match (ring={ring})")

    if shared_blocks_df is not None:
        rows = shared_blocks_df[shared_blocks_df["applicant_id"] == applicant_id]
        if not rows.empty:
            row = rows.iloc[0]
            chunks = row.get("shared_chunk_count", "?")
            if "matched_first_name" in shared_blocks_df.columns:
                matched = f"{row.get('matched_first_name', '')} {row.get('matched_last_name', '')}".strip()
                details.append(f"Shared text blocks ({chunks} chunks with {matched})")
            else:
                details.append(f"Shared text blocks ({chunks} chunks)")
        # Also check if this applicant appears as a matched_applicant_id (general scan pairs)
        elif "matched_applicant_id" in shared_blocks_df.columns:
            matched_rows = shared_blocks_df[shared_blocks_df["matched_applicant_id"] == applicant_id]
            if not matched_rows.empty:
                row = matched_rows.iloc[0]
                chunks = row.get("shared_chunk_count", "?")
                matched = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
                details.append(f"Shared text blocks ({chunks} chunks with {matched})")

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


def classify_signals(emp_df, multi_df, summary_df, shared_blocks_df=None):
    """Combine detection results into unified signal classification."""
    results = []

    # Build lookup sets
    emp_ids = set(emp_df["applicant_id"].unique()) if emp_df is not None and len(emp_df) > 0 else set()
    summary_ids = set(summary_df["applicant_id"].unique()) if summary_df is not None and len(summary_df) > 0 else set()

    # Shared text blocks — collect both sides of each pair
    shared_block_ids = set()
    if shared_blocks_df is not None and len(shared_blocks_df) > 0:
        shared_block_ids.update(shared_blocks_df["applicant_id"].unique())
        if "matched_applicant_id" in shared_blocks_df.columns:
            shared_block_ids.update(shared_blocks_df["matched_applicant_id"].unique())

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

    all_ids = emp_ids | summary_ids | multi_applicant_ids | shared_block_ids

    if not all_ids:
        print("  No signals detected across any method")
        return pd.DataFrame(columns=[
            "applicant_id", "first_name", "last_name", "email",
            "severity", "signal_type", "details"
        ])

    for applicant_id in all_ids:
        in_emp = applicant_id in emp_ids
        in_shared = applicant_id in shared_block_ids
        in_summary = applicant_id in summary_ids
        in_multi = applicant_id in multi_applicant_ids

        # Classification logic (priority order)
        if in_emp and in_summary:
            severity = "HIGH"
            signal = "employment_and_summary_match"
        elif in_emp:
            severity = "HIGH"
            signal = "employment_history_match"
        elif in_shared:
            severity = "HIGH"
            signal = "shared_text_block_match"
        elif in_multi:
            severity = "MEDIUM"
            signal = "multi_resume_divergence"
        elif in_summary:
            severity = "LOW"
            signal = "summary_match_only"
        else:
            continue

        # Gather applicant info
        info = get_applicant_info(applicant_id, emp_df, shared_blocks_df, summary_df)
        # If not found in emp/summary/shared, check multi-resume email
        if not info["email"] and applicant_id in multi_email_lookup:
            info["email"] = multi_email_lookup[applicant_id]

        details = build_detail_string(applicant_id, emp_df, multi_df, summary_df, shared_blocks_df)

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

def generate_report(config, results, method_results, output_dir, seed_info=None):
    """Generate markdown fraud detection report."""
    report_lines = []
    detection = config["detection"]

    # Header
    report_lines.append("# Resume Fraud Detection Report")
    report_lines.append("")
    report_lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}  ")
    req_key = config.get("_req_filter")
    if req_key:
        report_lines.append(f"**Requisition:** #{req_key}  ")
    date_range = config.get("_date_range")
    if date_range:
        report_lines.append(f"**Analysis Period:** {date_range['start']} to {date_range['end']}  ")
    else:
        report_lines.append(f"**Employment History Lookback:** {detection['employment_history']['lookback_days']} days  ")
        report_lines.append(f"**Multi-Resume Lookback:** {detection['multi_resume']['lookback_days']} days  ")
        report_lines.append(f"**Summary Lookback:** {detection['professional_summary']['lookback_days']} days  ")

    if seed_info:
        report_lines.append(f"**Mode:** Seed Investigation  ")
        report_lines.append("")
        report_lines.append("---")
        report_lines.append("")
        report_lines.append("## Seed Investigation")
        report_lines.append("")
        report_lines.append(f"Investigating fraud ring connected to a specific applicant.")
        report_lines.append("")
        report_lines.append(f"| Field | Value |")
        report_lines.append(f"|-------|-------|")
        report_lines.append(f"| Name | {seed_info['first_name']} {seed_info['last_name']} |")
        report_lines.append(f"| Email | {seed_info['email']} |")
        report_lines.append(f"| Applicant ID | {seed_info['applicant_id']} |")
        report_lines.append(f"| Employment Hash | `{seed_info['employment_hash'] or 'N/A'}` |")
        report_lines.append(f"| Summary Hash | `{seed_info['summary_hash'] or 'N/A'}` |")
        report_lines.append("")
        report_lines.append("*Thresholds reduced to 2 in seed mode — finding even 1 other person sharing the seed's fingerprint is significant.*")

    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")

    # Methodology
    report_lines.append("## Methodology")
    report_lines.append("")
    report_lines.append("This report uses four detection methods to identify potentially fraudulent resumes:")
    report_lines.append("")
    report_lines.append("### Method 1: Employment History Matching (PRIMARY)")
    report_lines.append("")
    report_lines.append("Identifies groups of applicants with identical employment fingerprints by hashing the AI-extracted work experience section from parsed resumes (`resume_only_experience` field).")
    report_lines.append("")
    report_lines.append(f"**Threshold**: {detection['employment_history']['threshold']}+ unique applicants sharing identical fingerprint = flagged")
    report_lines.append("")
    report_lines.append("**Why this works**: Legitimate applicants rarely have identical employment histories. When 3+ people claim the exact same jobs at the same companies with the same dates, it strongly suggests coordinated fraud or resume mills.")
    report_lines.append("")
    report_lines.append("### Method 4: Shared Text Block Detection (PRIMARY)")
    report_lines.append("")
    report_lines.append("Supplements Method 1 by detecting *partial* overlap. Splits each applicant's work experience into paragraph-level chunks, hashes each chunk individually, and finds applicant pairs sharing multiple identical chunks.")
    report_lines.append("")
    report_lines.append(f"**Min chunk length**: {detection['shared_text_blocks']['min_chunk_length']} characters  ")
    report_lines.append(f"**Threshold**: {detection['shared_text_blocks']['min_shared_chunks']}+ shared chunks between two applicants")
    report_lines.append("")
    report_lines.append("**Why this works**: Method 1 misses fraud rings where applicants share word-for-word job descriptions but have different additional employers. By hashing at the paragraph level, this method catches partial resume overlap that full-field hashing misses.")
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
    report_lines.append("| HIGH | Shared text block match (partial resume overlap) | Immediate investigation |")
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
        unique_rings = emp_df.groupby("employment_hash").agg(
            ring_size=("applicant_id", "nunique")
        ).reset_index()
        report_lines.append(f"**Suspicious groups found:** {len(unique_rings)}")
        report_lines.append(f"**Largest ring size:** {unique_rings['ring_size'].max()}")
        report_lines.append(f"**Total applicants flagged:** {emp_df['applicant_id'].nunique()}")
        report_lines.append("")

        # Top rings table
        report_lines.append("### Flagged Applicants")
        report_lines.append("")
        report_lines.append("| Name | Email | Ring Size | Employment Summary |")
        report_lines.append("|------|-------|-----------|-------------------|")

        for _, row in emp_df.head(50).iterrows():
            name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
            email = str(row.get("email", ""))[:30]
            ring = row.get("ring_size", "")
            summary = str(row.get("employment_summary", ""))[:60]
            report_lines.append(f"| {name} | {email} | {ring} | {summary} |")

        report_lines.append("")
    else:
        report_lines.append("No employment history matches detected.")
        report_lines.append("")

    # Method 4: Shared Text Blocks
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## Method 4: Shared Text Block Detection")
    report_lines.append("")

    shared_df = method_results.get("shared_text_blocks")
    if shared_df is not None and len(shared_df) > 0:
        report_lines.append(f"**Applicant pairs flagged:** {len(shared_df)}")
        report_lines.append(f"**Total applicants involved:** {shared_df['applicant_id'].nunique()}")
        report_lines.append("")

        report_lines.append("### Flagged Pairs")
        report_lines.append("")
        if "matched_applicant_id" in shared_df.columns:
            # General scan — pairs table
            report_lines.append("| Applicant | Matched With | Shared Chunks | Sample Text |")
            report_lines.append("|-----------|-------------|---------------|-------------|")
            for _, row in shared_df.head(50).iterrows():
                name1 = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
                name2 = f"{row.get('matched_first_name', '')} {row.get('matched_last_name', '')}".strip()
                chunks = row.get("shared_chunk_count", "")
                sample = str(row.get("sample_shared_text", ""))[:80]
                report_lines.append(f"| {name1} | {name2} | {chunks} | {sample} |")
        else:
            # Seeded scan — matches to seed
            report_lines.append("| Name | Email | Shared Chunks | Sample Text |")
            report_lines.append("|------|-------|---------------|-------------|")
            for _, row in shared_df.head(50).iterrows():
                name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
                email = str(row.get("email", ""))[:30]
                chunks = row.get("shared_chunk_count", "")
                sample = str(row.get("sample_shared_text", ""))[:80]
                report_lines.append(f"| {name} | {email} | {chunks} | {sample} |")

        report_lines.append("")
    else:
        report_lines.append("No shared text block matches detected.")
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
    report_lines.append("- `shared_text_blocks_results.csv`")
    report_lines.append("- `multi_resume_divergence_results.csv`")
    report_lines.append("- `professional_summary_results.csv`")
    report_lines.append("- `combined_signals.csv`")
    report_lines.append("")
    report_lines.append("**SQL Queries:**")
    report_lines.append("- `employment_history_query.sql`")
    report_lines.append("- `shared_text_blocks_query.sql`")
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
    parser.add_argument(
        "--seed",
        help="Investigate a specific applicant's fraud ring by email address",
    )
    parser.add_argument(
        "--req",
        help="Filter to applicants on a specific requisition (by requisition key, e.g. 9175)",
    )

    args = parser.parse_args()

    # Validate date range args
    if args.start_date and not args.end_date:
        parser.error("--start-date requires --end-date")
    if args.end_date and not args.start_date:
        parser.error("--end-date requires --start-date")
    if args.start_date and args.lookback:
        parser.error("Cannot use --lookback with --start-date/--end-date")
    if args.seed and args.dry_run:
        parser.error("--seed requires database access and cannot be used with --dry-run")

    # Load configuration
    print("Loading configuration...")
    config = load_config()

    # Apply requisition filter
    if args.req:
        config["_req_filter"] = args.req
        print(f"  Requisition filter: #{args.req}")
        # Reduce thresholds for req-scoped scans (smaller population)
        config["detection"]["employment_history"]["threshold"] = 2
        config["detection"]["professional_summary"]["threshold"] = 2
        config["detection"]["shared_text_blocks"]["min_shared_chunks"] = 2
        print(f"  Thresholds reduced for requisition-scoped scan")

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
        config["detection"]["shared_text_blocks"]["lookback_days"] = args.lookback
        print(f"  Lookback override: {args.lookback} days for all methods")

    # Set up output directory
    output_dir = setup_output_dir()
    print(f"Output directory: {output_dir}")

    # Seed mode: connect early and look up seed applicant
    seed_info = None
    if args.seed:
        config["_seed_email"] = args.seed
        print(f"\nSeed mode: investigating '{args.seed}'")
        print("Connecting to database...")
        engine = get_db_connection(config)
        seed_info = lookup_seed_applicant(engine, config)

    # Build queries
    print("\nBuilding detection queries...")
    if seed_info:
        emp_query = build_seeded_employment_query(config, seed_info)
        shared_blocks_query = build_seeded_shared_text_blocks_query(config, seed_info)
        multi_query = build_seeded_multi_resume_query(config, seed_info)
        summary_query = build_seeded_summary_query(config, seed_info)
    else:
        emp_query = build_employment_history_query(config)
        shared_blocks_query = build_shared_text_blocks_query(config)
        multi_query = build_multi_resume_query(config)
        summary_query = build_summary_match_query(config)

    if args.dry_run:
        print("\n[DRY RUN - Queries generated but not executed]")
        for name, query in [
            ("employment_history", emp_query),
            ("shared_text_blocks", shared_blocks_query),
            ("multi_resume_divergence", multi_query),
            ("professional_summary", summary_query),
        ]:
            query_file = output_dir / f"{name}_query.sql"
            with open(query_file, "w") as f:
                f.write(query)
            print(f"  Saved: {query_file}")
        return

    # Connect to database (if not already connected for seed mode)
    if not seed_info:
        print("\nConnecting to database...")
        engine = get_db_connection(config)

    # Run all detection methods
    emp_df = run_detection_method(
        engine, emp_query, "employment_history", output_dir
    )
    shared_blocks_df = run_detection_method(
        engine, shared_blocks_query, "shared_text_blocks", output_dir
    )
    multi_df = run_detection_method(
        engine, multi_query, "multi_resume_divergence", output_dir
    )
    summary_df = run_detection_method(
        engine, summary_query, "professional_summary", output_dir
    )

    # Classify combined signals
    print("\nClassifying signals...")
    results = classify_signals(emp_df, multi_df, summary_df, shared_blocks_df)

    # Save combined results
    combined_file = output_dir / "combined_signals.csv"
    results.to_csv(combined_file, index=False)
    print(f"Combined signals saved: {combined_file.name}")

    # Generate report
    method_results = {
        "employment_history": emp_df,
        "shared_text_blocks": shared_blocks_df,
        "multi_resume_divergence": multi_df,
        "professional_summary": summary_df,
    }
    report_file = generate_report(config, results, method_results, output_dir, seed_info=seed_info)

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
