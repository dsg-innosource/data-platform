#!/usr/bin/env python3
"""
Swim Campaign Processor

Runs targeted candidate searches based on campaign configurations.
Outputs CSV files for recruiter use.

Usage:
    python process.py --campaign 2026-02-02-ads-order-processor
    python process.py --campaign 2026-02-02-ads-order-processor --search columbus
    python process.py --list  # List available campaigns
"""

import argparse
import os
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import yaml
from sqlalchemy import create_engine, text


def load_config():
    """Load main configuration."""
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Expand environment variables
    db = config["database"]
    for key in db:
        if isinstance(db[key], str) and db[key].startswith("${"):
            env_var = db[key][2:-1]
            db[key] = os.environ.get(env_var, "")

    return config


def load_campaign(campaign_name):
    """Load a specific campaign configuration."""
    campaign_path = Path(__file__).parent / "campaigns" / campaign_name / "campaign.yaml"
    if not campaign_path.exists():
        raise FileNotFoundError(f"Campaign not found: {campaign_name}")

    with open(campaign_path) as f:
        return yaml.safe_load(f)


def list_campaigns():
    """List all available campaigns."""
    campaigns_dir = Path(__file__).parent / "campaigns"
    campaigns = [d.name for d in campaigns_dir.iterdir() if d.is_dir()]
    return sorted(campaigns)


def get_db_connection(config):
    """Create database connection."""
    db = config["database"]
    connection_string = (
        f"postgresql://{db['username']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['database']}"
        f"?sslmode={db['sslmode']}"
    )
    return create_engine(connection_string)


def build_location_filter(cities):
    """Build SQL LIKE conditions for city matching."""
    conditions = []
    for city in cities:
        # Handle variations
        city_lower = city.lower().strip()
        conditions.append(f"LOWER(rs.resume_only_current_location) LIKE '%{city_lower}%'")
    return " OR ".join(conditions)


def build_skills_filter(skills):
    """Build SQL LIKE conditions for skills matching (for flagging, not filtering)."""
    conditions = []
    for skill in skills:
        skill_lower = skill.lower().strip()
        conditions.append(f"LOWER(rs.resume_only_experience) LIKE '%{skill_lower}%'")
    return " OR ".join(conditions)


def build_query(campaign, search, config):
    """Build the SQL query for a specific search."""
    req_ids = ", ".join(str(r) for r in campaign["requisition_ids"])
    location_filter = build_location_filter(search["location"]["cities"])
    skills_filter = build_skills_filter(search.get("skills_preferred", []))

    # Check if we should exclude employed candidates
    exclude_employed = campaign.get("filters", {}).get("exclude_hired", True)

    # Build the query
    query = f"""
-- Swim Campaign: {campaign['name']}
-- Search: {search['name']}
-- Generated: {datetime.now().isoformat()}
-- Requisitions: {req_ids}
-- Exclude currently employed: {exclude_employed}

WITH campaign_applicants AS (
    -- Get all applicants who applied to the target requisitions
    SELECT DISTINCT jl.applicant_id
    FROM bronze.portal_applicant_job_listings jl
    WHERE jl.requisition_id IN ({req_ids})
),
currently_employed AS (
    -- Get anyone currently employed (via ADP) - exclude from results
    SELECT DISTINCT applicant_id
    FROM bronze.adp_tenure_history
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM bronze.adp_tenure_history)
      AND termination_date IS NULL
      AND applicant_id IS NOT NULL
),
applicant_details AS (
    -- Get applicant info
    SELECT
        a.id as applicant_id,
        a.first_name,
        a.last_name,
        a.email,
        COALESCE(a.cell_phone, a.phone_number) as phone,
        a.last_applied_at,
        a.applicant_status_id,
        ps.name as status
    FROM bronze.portal_applicants a
    JOIN campaign_applicants ca ON a.id = ca.applicant_id
    LEFT JOIN bronze.portal_applicant_statuses ps ON a.applicant_status_id = ps.id
    LEFT JOIN currently_employed ce ON a.id = ce.applicant_id
    WHERE a.deleted_at IS NULL
      AND a.email IS NOT NULL
      AND (a.cell_phone IS NOT NULL OR a.phone_number IS NOT NULL)
      -- Exclude anyone currently employed (if filter enabled)
      {"AND ce.applicant_id IS NULL" if exclude_employed else "-- (not filtering employed)"}
),
resume_data AS (
    -- Get location and experience from parsed resumes
    SELECT
        rs.applicant_id,
        rs.resume_only_current_location as location,
        rs.resume_only_experience as experience_summary
    FROM bronze.portal_resume_scores rs
)
SELECT
    ad.applicant_id,
    ad.first_name,
    ad.last_name,
    ad.email,
    ad.phone,
    rd.location,
    LEFT(rd.experience_summary, 500) as experience_summary,
    ad.last_applied_at,
    ad.status,
    CASE
        WHEN ({skills_filter}) THEN 'Yes'
        ELSE 'No'
    END as skills_match
FROM applicant_details ad
LEFT JOIN resume_data rd ON ad.applicant_id = rd.applicant_id
WHERE (
    {location_filter}
)
ORDER BY
    ad.last_applied_at DESC,
    ad.last_name,
    ad.first_name;
"""
    return query


def run_search(engine, campaign, search, output_dir):
    """Run a single search and save results."""
    print(f"\nRunning search: {search['name']}")
    print(f"  Location: {search['location']['center']} ({search['location']['radius_miles']} mile radius)")
    print(f"  Cities included: {len(search['location']['cities'])}")

    # Build and save query
    query = build_query(campaign, search, None)
    query_file = output_dir / f"{search['name']}_query.sql"
    with open(query_file, "w") as f:
        f.write(query)
    print(f"  Query saved: {query_file.name}")

    # Execute query
    try:
        df = pd.read_sql(text(query), engine)
        print(f"  Results: {len(df)} candidates found")

        # Save CSV
        output_file = output_dir / search["output_file"]
        df.to_csv(output_file, index=False)
        print(f"  Output saved: {output_file.name}")

        # Summary stats
        if len(df) > 0:
            skills_match = df[df["skills_match"] == "Yes"]
            print(f"  With preferred skills: {len(skills_match)}")

        return df

    except Exception as e:
        print(f"  ERROR: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Run swim campaign searches")
    parser.add_argument("--campaign", help="Campaign folder name")
    parser.add_argument("--search", help="Run only a specific search (optional)")
    parser.add_argument("--list", action="store_true", help="List available campaigns")
    parser.add_argument("--dry-run", action="store_true", help="Build queries but don't execute")

    args = parser.parse_args()

    if args.list:
        campaigns = list_campaigns()
        print("Available campaigns:")
        for c in campaigns:
            print(f"  - {c}")
        return

    if not args.campaign:
        parser.print_help()
        return

    # Load configurations
    print(f"Loading campaign: {args.campaign}")
    config = load_config()
    campaign = load_campaign(args.campaign)

    print(f"\nCampaign: {campaign['name']}")
    print(f"Client: {campaign['client']}")
    print(f"Requisitions: {campaign['requisition_ids']}")

    # Set up output directory
    output_dir = Path(__file__).parent / "campaigns" / args.campaign

    if args.dry_run:
        print("\n[DRY RUN - Queries will be generated but not executed]")
        for search in campaign["searches"]:
            if args.search and search["name"] != args.search:
                continue
            query = build_query(campaign, search, config)
            query_file = output_dir / f"{search['name']}_query.sql"
            with open(query_file, "w") as f:
                f.write(query)
            print(f"\nQuery saved: {query_file}")
        return

    # Connect to database
    print("\nConnecting to database...")
    engine = get_db_connection(config)

    # Run searches
    for search in campaign["searches"]:
        if args.search and search["name"] != args.search:
            continue
        run_search(engine, campaign, search, output_dir)

    print("\n" + "=" * 50)
    print("Campaign complete!")
    print(f"Output files in: {output_dir}")


if __name__ == "__main__":
    main()
