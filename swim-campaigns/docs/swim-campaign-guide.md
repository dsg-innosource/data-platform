# Swim Campaign Guide - Data Warehouse Process

## Overview
A "Swim Campaign" is a targeted candidate search where we identify potential applicants from our database who have specific skills/experience but may not have applied to the current role. The goal is to proactively reach out to these candidates for urgent hiring needs.

## Process Flow

### 1. Gather Requirements (Ask Clarifying Questions)
When a swim campaign request comes in, **always ask these questions** before starting:

#### Required Information:
- **Geographic Scope**:
  - Specific city/location for the role
  - Acceptable radius (e.g., 50 miles, 100 miles)
  - Remote work acceptable?

- **Skills/Experience Required**:
  - Primary skill/certification needed (e.g., "Pharmacy Technician", "Forklift Certified", "RN")
  - Years of experience required
  - Specific employers or certifications preferred

- **Recency Requirements**:
  - How recent should applications be? (e.g., last 3 months, last 6 months, last year)
  - Does recent experience matter? (currently working vs. previously worked)

- **Employment Status**:
  - Exclude current employees? (verify via ADP)
  - Exclude candidates at specific clients?

- **Additional Filters** (ask if not specified):
  - Minimum/maximum years of experience
  - Education requirements
  - Certification requirements
  - Pay rate expectations

### 2. Data Extraction Strategy

#### Primary Data Sources (in order of preference):

1. **`bronze.portal_resume_scores`** - AI-parsed resume data
   - Contains: `resume_only_experience`, `resume_only_current_location`, `resume_only_education`
   - Best for skill matching and location identification
   - Use for initial candidate pool

2. **`bronze.portal_applicants`** - Applicant master table
   - Contains: contact info (email, phone), last_applied_at, status
   - Join with resume_scores to get contact details

3. **`bronze.adp_tenure_history`** - Employment verification
   - **ALWAYS use latest snapshot_date**: `WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM bronze.adp_tenure_history)`
   - Filter out active employees: `WHERE termination_date IS NULL`
   - Cross-reference to exclude currently employed candidates

4. **`bronze.portal_applicant_statuses`** - Status lookup
   - Join to get human-readable status names

#### Applicant-to-Requisition Links:

**Direct link**: `portal_applicants.requisition_id → portal_requisitions.id`
- Represents the applicant's current/most recent requisition assignment
- Single foreign key, captures one requisition at a time

**Many-to-many link**: `portal_applicant_job_listings`
- Tracks every application event
- Fields: `applicant_id`, `requisition_id`, `created_at`
- Use this to find candidates who applied to specific requisitions

#### Query Pattern Template:
```sql
-- Step 1: Find candidates with required skills/experience
WITH target_candidates AS (
    SELECT
        rs.applicant_id,
        rs.resume_only_current_location,
        rs.resume_only_experience,
        rs.created_at as resume_scored_at
    FROM bronze.portal_resume_scores rs
    WHERE
        -- LOCATION FILTER (use city names, not exact coordinates)
        (
            LOWER(rs.resume_only_current_location) LIKE '%city_name%'
            OR LOWER(rs.resume_only_current_location) LIKE '%nearby_city%'
            -- Add multiple cities within target radius
        )
        -- SKILL/EXPERIENCE FILTER
        AND (
            LOWER(rs.resume_only_experience) LIKE '%required_skill%'
            OR LOWER(rs.resume_only_experience) LIKE '%certification%'
            OR LOWER(rs.resume_only_experience) LIKE '%job_title%'
        )
),
-- Step 2: Exclude currently employed (via ADP)
active_employees AS (
    SELECT DISTINCT applicant_id
    FROM bronze.adp_tenure_history
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM bronze.adp_tenure_history)
      AND termination_date IS NULL
      AND applicant_id IS NOT NULL
)
-- Step 3: Get contact info and apply filters
SELECT
    a.id as applicant_id,
    a.first_name,
    a.last_name,
    a.email,
    COALESCE(a.cell_phone, a.phone_number) as phone_number,
    tc.resume_only_current_location as location,
    tc.resume_only_experience as experience_summary,
    a.last_applied_at,
    ps.name as current_status
FROM target_candidates tc
JOIN bronze.portal_applicants a ON tc.applicant_id = a.id
LEFT JOIN bronze.portal_applicant_statuses ps ON a.applicant_status_id = ps.id
LEFT JOIN active_employees ae ON a.id = ae.applicant_id
WHERE a.deleted_at IS NULL
  AND a.email IS NOT NULL
  AND (a.cell_phone IS NOT NULL OR a.phone_number IS NOT NULL)
  -- RECENCY FILTER
  AND a.last_applied_at >= CURRENT_DATE - INTERVAL '3 months'
  -- EXCLUDE ACTIVE EMPLOYEES
  AND ae.applicant_id IS NULL
ORDER BY a.last_applied_at DESC;
```

### 3. Location Filtering Best Practices

**For geographic radius searches:**
- Use city name matching (not exact coordinates - we don't have lat/long in the database)
- Include all cities within reasonable commute distance
- For 50-mile radius from City X, include:
  - Primary city and common spelling variations
  - All major suburbs and nearby cities
  - Consider: `LIKE '%dallas%'`, `LIKE '%ft worth%'`, `LIKE '%fort worth%'`

**Example: 50-mile radius from Lewisville, TX:**
```sql
WHERE (
    LOWER(rs.resume_only_current_location) LIKE '%lewisville%'
    OR LOWER(rs.resume_only_current_location) LIKE '%dallas%'
    OR LOWER(rs.resume_only_current_location) LIKE '%fort worth%'
    OR LOWER(rs.resume_only_current_location) LIKE '%denton%'
    OR LOWER(rs.resume_only_current_location) LIKE '%plano%'
    OR LOWER(rs.resume_only_current_location) LIKE '%frisco%'
    OR LOWER(rs.resume_only_current_location) LIKE '%irving%'
    OR LOWER(rs.resume_only_current_location) LIKE '%carrollton%'
    -- Add more cities as appropriate
)
```

### 4. Skills Matching Best Practices

**Use multiple search patterns for the same skill:**
- Variations in spelling: "pharm tech", "pharmacy tech", "pharmacy technician"
- Related roles: "pharmacist", "pharmacy assistant"
- Company indicators: "cvs", "walgreens", "rite aid" + "pharmacy"
- Certification codes: "CPT", "CPhT", "PTCB certified"

**Example: Pharmacy Technician search:**
```sql
WHERE (
    LOWER(rs.resume_only_experience) LIKE '%pharmacy tech%'
    OR LOWER(rs.resume_only_experience) LIKE '%pharm tech%'
    OR (LOWER(rs.resume_only_experience) LIKE '%cvs%'
        AND LOWER(rs.resume_only_experience) LIKE '%pharmacy%')
    OR (LOWER(rs.resume_only_experience) LIKE '%walgreens%'
        AND LOWER(rs.resume_only_experience) LIKE '%pharmacy%')
    OR LOWER(rs.resume_only_experience) LIKE '%ptcb%'
    OR LOWER(rs.resume_only_experience) LIKE '%certified pharmacy%'
)
```

### 5. Prioritization Logic

**Rank candidates by:**
1. **Location** (closest first)
   - Exact city match = Priority 1
   - Within inner radius (e.g., 0-25 miles) = Priority 2
   - Within outer radius (e.g., 25-50 miles) = Priority 3

2. **Recency of Application** (more recent = higher priority)
   - Applied in last month = highest
   - Applied in last 3 months = high
   - Applied in last 6 months = medium

3. **Experience Level**
   - Exact skill match with X+ years
   - Related experience
   - Adjacent skills

4. **Special Flags**
   - Currently certified
   - Multiple relevant employers
   - Specialty experience

### 6. Quality Checks Before Delivery

**Verify the following:**
- All candidates have valid email addresses
- All candidates have phone numbers
- Employment status verified against ADP (if requested)
- No deleted applicants included (`deleted_at IS NULL`)
- Location filtering is accurate
- Skill matching is appropriate (not too broad/narrow)
- Contact information is formatted consistently

### 7. Common Campaign Types

#### A. Requisition-Based Swim
**Goal**: Re-engage candidates who applied to specific requisitions
**Primary Filter**: `portal_applicant_job_listings` by requisition ID
**Example**: "Find all candidates who applied to ADS Order Processor reqs but weren't hired"

#### B. Geographic Swim
**Goal**: Find candidates in a specific location
**Primary Filter**: Location from resume data
**Example**: "Find all warehouse workers within 30 miles of Columbus, OH"

#### C. Skills Swim
**Goal**: Find candidates with specific skills/certifications
**Primary Filter**: Experience/Skills from resume data
**Example**: "Find all Certified Nursing Assistants in Ohio"

#### D. Urgent Fill Swim
**Goal**: Fast turnaround for critical opening
**Primary Filter**: Recency + Availability
**Example**: "Find pharmacy techs in DFW who applied in last 3 months and aren't working"

### 8. Best Practices & Tips

#### DO:
- Ask clarifying questions upfront
- Cast a slightly wider net than requested (can always narrow)
- Include distance estimates when possible
- Verify employment status via ADP when requested
- Make the output professional and actionable
- Use consistent formatting for contact info

#### DON'T:
- Assume location without verifying city names
- Use overly strict skill matching (you'll miss good candidates)
- Forget to filter out deleted applicants
- Include candidates without contact information
- Forget to check ADP for current employment status
- Include candidates who are currently placed (unless specifically asked)

### 9. Troubleshooting Common Issues

**Issue**: Too few candidates returned
**Solution**: Broaden search criteria - check for:
- Spelling variations in skills
- Wider geographic radius
- Longer time window for applications
- Related/adjacent skills

**Issue**: Too many candidates returned
**Solution**: Tighten criteria:
- Reduce geographic radius
- Add experience level requirements
- Reduce time window
- Add certification requirements

**Issue**: Location matching not working
**Solution**:
- Check actual data in `resume_only_current_location`
- Look for variations: "Ft Worth" vs "Fort Worth"
- Include ZIP codes if available in data
- Use broader region names: "DFW", "Dallas area"

---

## Quick Reference: Key Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `bronze.portal_resume_scores` | Skills/experience matching | `resume_only_experience`, `resume_only_current_location`, `applicant_id` |
| `bronze.portal_applicants` | Contact information | `email`, `cell_phone`, `phone_number`, `last_applied_at`, `first_name`, `last_name` |
| `bronze.portal_applicant_job_listings` | Applicant-to-requisition history | `applicant_id`, `requisition_id`, `created_at` |
| `bronze.adp_tenure_history` | Employment verification | `applicant_id`, `termination_date`, `snapshot_date` |
| `bronze.portal_applicant_statuses` | Status lookup | `id`, `name` |
| `bronze.portal_requisitions` | Job requisitions | `position`, `client_id` |

---

## Conclusion

Swim campaigns are about **speed and precision**. The goal is to quickly identify qualified candidates who may not have applied to the current role but have the right background. By following this guide, you can deliver professional, actionable candidate lists that help recruiters fill urgent positions faster.

**Remember**: Always ask clarifying questions first, cast an appropriate net, and deliver a polished output!
