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
        conditions.append(f"LOWER(rd.location) LIKE '%{city_lower}%'")
    return " OR ".join(conditions)


def build_skills_filter(skills):
    """Build SQL LIKE conditions for skills matching (for flagging, not filtering)."""
    conditions = []
    for skill in skills:
        skill_lower = skill.lower().strip()
        conditions.append(f"LOWER(rd.experience_summary) LIKE '%{skill_lower}%'")
    return " OR ".join(conditions)


def build_query(campaign, search, config):
    """Build the SQL query for a specific search."""
    req_ids_list = campaign.get("requisition_ids", [])
    req_ids = ", ".join(str(r) for r in req_ids_list) if req_ids_list else None
    has_location = search.get("location", {}).get("cities")
    location_filter = build_location_filter(has_location) if has_location else None
    skills_filter = build_skills_filter(search.get("skills_preferred", []))

    # Check if we should exclude employed candidates
    exclude_employed = campaign.get("filters", {}).get("exclude_hired", True)

    # Check for time window filter
    applied_within_days = campaign.get("filters", {}).get("applied_within_days")
    applied_within_clause = ""
    if applied_within_days:
        applied_within_clause = f"AND a.last_applied_at >= CURRENT_DATE - INTERVAL '{applied_within_days} days'"

    # Check for required skills filter (at least one must match)
    skills_required = search.get("skills_required", [])
    skills_required_filter = ""
    if skills_required:
        skills_required_filter = f"AND ({build_skills_filter(skills_required)})"

    # Build CTEs conditionally
    campaign_applicants_cte = ""
    campaign_applicants_join = ""
    if req_ids:
        campaign_applicants_cte = f"""campaign_applicants AS (
    -- Get all applicants who applied to the target requisitions
    SELECT DISTINCT jl.applicant_id
    FROM bronze.portal_applicant_job_listings jl
    WHERE jl.requisition_id IN ({req_ids})
),"""
        campaign_applicants_join = "JOIN campaign_applicants ca ON a.id = ca.applicant_id"

    # Build WHERE clause for final SELECT
    where_conditions = []
    if location_filter:
        where_conditions.append(f"({location_filter})")
    if skills_required_filter:
        where_conditions.append(skills_required_filter.lstrip("AND "))
    # Default to true if no conditions (all candidates pass)
    where_clause = "\n    AND ".join(where_conditions) if where_conditions else "1=1"

    # Build the query
    query = f"""
-- Swim Campaign: {campaign['name']}
-- Search: {search['name']}
-- Generated: {datetime.now().isoformat()}
-- Requisitions: {req_ids or 'all applicants'}
-- Exclude currently employed: {exclude_employed}
-- Applied within days: {applied_within_days or 'no limit'}
-- Skills required (at least one): {', '.join(skills_required) if skills_required else 'none'}

WITH {campaign_applicants_cte}
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
    {campaign_applicants_join}
    LEFT JOIN bronze.portal_applicant_statuses ps ON a.applicant_status_id = ps.id
    LEFT JOIN currently_employed ce ON a.id = ce.applicant_id
    WHERE a.deleted_at IS NULL
      AND a.email IS NOT NULL
      AND (a.cell_phone IS NOT NULL OR a.phone_number IS NOT NULL)
      -- Exclude anyone currently employed (if filter enabled)
      {"AND ce.applicant_id IS NULL" if exclude_employed else "-- (not filtering employed)"}
      -- Time window filter
      {applied_within_clause}
),
resume_data AS (
    -- Get location and experience from parsed resumes (most recent per applicant)
    SELECT DISTINCT ON (rs.applicant_id)
        rs.applicant_id,
        rs.resume_only_current_location as location,
        rs.resume_only_experience as experience_summary
    FROM bronze.portal_resume_scores rs
    ORDER BY rs.applicant_id, rs.created_at DESC
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
WHERE {where_clause}
ORDER BY
    ad.last_applied_at DESC,
    ad.last_name,
    ad.first_name;
"""
    return query


def run_search(engine, campaign, search, output_dir):
    """Run a single search and save results."""
    print(f"\nRunning search: {search['name']}")
    location = search.get('location', {})
    if location.get('cities'):
        print(f"  Location: {location.get('center', 'N/A')} ({location.get('radius_miles', 0)} mile radius)")
        print(f"  Cities included: {len(location['cities'])}")
    else:
        print(f"  Location: All locations")

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


def generate_summary_report(campaign, search_results, output_dir):
    """Generate a markdown summary report for the campaign."""
    report_lines = []

    # Header
    report_lines.append(f"# Swim Campaign Summary")
    report_lines.append("")
    report_lines.append(f"**Campaign:** {campaign['name']}  ")
    if campaign.get('client'):
        report_lines.append(f"**Client:** {campaign['client']}  ")
    report_lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}  ")
    if campaign.get('requestor'):
        report_lines.append(f"**Requestor:** {campaign.get('requestor')}  ")
    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")

    # Search Criteria
    report_lines.append("## Search Criteria")
    report_lines.append("")
    req_ids = campaign.get('requisition_ids', [])
    if req_ids:
        report_lines.append(f"**Requisition IDs:** {', '.join(str(r) for r in req_ids)}")
    else:
        report_lines.append(f"**Requisition IDs:** All applicants (no requisition filter)")
    report_lines.append("")

    filters = campaign.get('filters', {})
    if filters.get('exclude_hired', True):
        report_lines.append("**Filters Applied:**")
        report_lines.append("- Excluded currently employed candidates (via ADP)")
        report_lines.append("- Required valid email and phone number")
        report_lines.append("- Excluded deleted applicants")
        report_lines.append("")

    # Results Overview
    report_lines.append("## Results Overview")
    report_lines.append("")
    report_lines.append("| Search | Location | Radius | Candidates | With Preferred Skills |")
    report_lines.append("|--------|----------|--------|------------|----------------------|")

    total_candidates = 0
    total_with_skills = 0

    for search in campaign['searches']:
        name = search['name']
        df = search_results.get(name)
        if df is not None:
            count = len(df)
            skills_count = len(df[df['skills_match'] == 'Yes']) if 'skills_match' in df.columns else 0
            pct = f"{(skills_count/count*100):.0f}%" if count > 0 else "N/A"
            total_candidates += count
            total_with_skills += skills_count
            location = search.get('location', {})
            loc_center = location.get('center', 'All locations')
            loc_radius = f"{location.get('radius_miles', 0)} mi" if location.get('cities') else 'N/A'
            report_lines.append(
                f"| {name.title()} | {loc_center} | "
                f"{loc_radius} | {count} | {skills_count} ({pct}) |"
            )
        else:
            location = search.get('location', {})
            loc_center = location.get('center', 'All locations')
            loc_radius = f"{location.get('radius_miles', 0)} mi" if location.get('cities') else 'N/A'
            report_lines.append(f"| {name.title()} | {loc_center} | {loc_radius} | ERROR | - |")

    report_lines.append("")
    total_pct = f"{(total_with_skills/total_candidates*100):.0f}%" if total_candidates > 0 else "N/A"
    report_lines.append(f"**Total:** {total_candidates} candidates ({total_with_skills} with preferred skills, {total_pct})")
    report_lines.append("")

    # Skills Searched
    report_lines.append("### Preferred Skills Flagged")
    report_lines.append("")
    all_skills = set()
    for search in campaign['searches']:
        all_skills.update(search.get('skills_preferred', []))
    for skill in sorted(all_skills):
        report_lines.append(f"- {skill}")
    report_lines.append("")

    # Top Candidates per Search
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## Top Candidates Preview")
    report_lines.append("")

    for search in campaign['searches']:
        name = search['name']
        df = search_results.get(name)
        if df is not None and len(df) > 0:
            report_lines.append(f"### {name.title()} - Top 15")
            report_lines.append("")

            # Sort by skills match (Yes first), then by last_applied_at
            df_sorted = df.copy()
            df_sorted['_skills_sort'] = df_sorted['skills_match'].apply(lambda x: 0 if x == 'Yes' else 1)
            df_sorted = df_sorted.sort_values(['_skills_sort', 'last_applied_at'], ascending=[True, False])

            report_lines.append("| Name | Email | Location | Skills Match | Last Applied | Status |")
            report_lines.append("|------|-------|----------|--------------|--------------|--------|")

            for _, row in df_sorted.head(15).iterrows():
                name_str = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
                email = str(row.get('email', '')) if pd.notna(row.get('email')) else ''
                location = str(row.get('location', ''))[:30] if pd.notna(row.get('location')) else 'Unknown'
                skills = row.get('skills_match', 'No')
                applied = row.get('last_applied_at', '')
                if pd.notna(applied):
                    applied = str(applied)[:10]
                else:
                    applied = 'Unknown'
                status = str(row.get('status', ''))[:20] if pd.notna(row.get('status')) else ''
                report_lines.append(f"| {name_str} | {email} | {location} | {skills} | {applied} | {status} |")

            report_lines.append("")

    # Data Notes
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## Data Notes")
    report_lines.append("")

    for search in campaign['searches']:
        name = search['name']
        df = search_results.get(name)
        if df is not None and len(df) > 0:
            missing_location = df['location'].isna().sum() if 'location' in df.columns else 0
            missing_experience = df['experience_summary'].isna().sum() if 'experience_summary' in df.columns else 0
            if missing_location > 0 or missing_experience > 0:
                report_lines.append(f"**{name.title()}:**")
                if missing_location > 0:
                    report_lines.append(f"- {missing_location} candidates missing location data")
                if missing_experience > 0:
                    report_lines.append(f"- {missing_experience} candidates missing resume/experience data")
                report_lines.append("")

    if not any(search_results.get(s['name']) is not None and
               (search_results[s['name']]['location'].isna().any() or
                search_results[s['name']]['experience_summary'].isna().any())
               for s in campaign['searches'] if search_results.get(s['name']) is not None):
        report_lines.append("No data quality issues detected.")
        report_lines.append("")

    # Deliverables
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## Deliverables")
    report_lines.append("")
    report_lines.append("**CSV Files:**")
    for search in campaign['searches']:
        report_lines.append(f"- `{search['output_file']}`")
    report_lines.append("")
    report_lines.append("**SQL Queries:**")
    for search in campaign['searches']:
        report_lines.append(f"- `{search['name']}_query.sql`")
    report_lines.append("")

    # Write report
    report_content = "\n".join(report_lines)
    report_file = output_dir / "campaign_summary.md"
    with open(report_file, "w") as f:
        f.write(report_content)

    print(f"\nSummary report saved: {report_file.name}")
    return report_file


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
    if campaign.get('client'):
        print(f"Client: {campaign['client']}")
    print(f"Requisitions: {campaign.get('requisition_ids', 'All applicants')}")

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

    # Run searches and collect results
    search_results = {}
    for search in campaign["searches"]:
        if args.search and search["name"] != args.search:
            continue
        df = run_search(engine, campaign, search, output_dir)
        search_results[search["name"]] = df

    # Generate summary report
    generate_summary_report(campaign, search_results, output_dir)

    print("\n" + "=" * 50)
    print("Campaign complete!")
    print(f"Output files in: {output_dir}")


if __name__ == "__main__":
    main()
