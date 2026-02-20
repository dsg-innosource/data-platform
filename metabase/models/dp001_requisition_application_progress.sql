-- -------------------------------------------------------------------------------------------
-- DP001 - Requisition Application Progress with Recruiter Names
-- -------------------------------------------------------------------------------------------
-- METABASE URL: https://innosource.metabaseapp.com/model/305-requisition-application-progress
--
-- CHANGE LOG:
-- 2025-12-31 - Added fall-off tracking (fall_off_date, fall_off_reason, fall_off_recruiter_name)
-- 2025-12-31 - Added is_resume_only flag (Resume Only Scoring requisitions per Data Governance)
-- 2026-01-16 - Added First Contact SMS tracking (first_contact_sms_date, first_contact_sms_recruiter_name)
--              Source: bronze.portal_applicant_texts (Sense SMS system)
--              Logic: First outbound SMS sent AFTER application, within 30-day window
--              Reference: DSG-3013, ClickUp Doc 88p1h-8871
-- 2026-02-20 - Added source_name field from earliest application to specific requisition
--              Reference: DSG-3891
-- -------------------------------------------------------------------------------------------
with reqs as (
  select
    c.id as client_id,
    c.name as client_name,
    r.id as requisition_id,
    r.requisition_key,
    r.fill_by,
    r.use_jakib_ai,
    -- Resume Only Requisition: use_jakib_ai=FALSE, ai_context empty, ai_scoring_context populated
    CASE
      WHEN r.use_jakib_ai = FALSE
      AND (
        r.ai_context IS NULL
        OR TRIM(r.ai_context) = ''
      )
      AND (
        r.ai_scoring_context IS NOT NULL
        AND TRIM(r.ai_scoring_context) != ''
      ) THEN TRUE
      ELSE FALSE
    END as is_resume_only,
    case
      when r.requisition_key like '%Pipeline%' then 1
      else 0
    end as is_pipeline
  from
    bronze.portal_requisitions r
    inner join bronze.portal_clients c on r.client_id = c.id
  where
    extract (
      'Year'
      from
        r.fill_by
    ) >= 2024
),
req_applicant_list as (
  select
    distinct r.client_id,
    r.client_name,
    r.requisition_id,
    r.requisition_key,
    r.is_pipeline,
    r.use_jakib_ai,
    r.is_resume_only,
    r.fill_by,
    a.id as applicant_id,
    a.first_name,
    a.last_name,
    a.email,
    a.phone_number
  from
    reqs r
    inner join bronze.portal_requisition_statistics rs on r.requisition_id = rs.requisition_id
    inner join bronze.portal_applicants a on rs.applicant_id = a.id
),
req_applicants as (
  select
    ra.*,
    jr.score as jakib_score,
    jr.conversation_type,
    rs.resume_only_score,
    jr.created_at as score_created_at,
    rs.created_at as resume_only_score_created_at
  from
    req_applicant_list ra
    left join bronze.portal_jakib_results jr on ra.requisition_id = jr.requisition_id
    and ra.applicant_id = jr.applicant_id
    left join bronze.portal_resume_scores rs on ra.requisition_id = rs.requisition_id
    and ra.applicant_id = rs.applicant_id
),
application_dates as (
  select
    jl.applicant_id,
    jl.requisition_id,
    min(jl.created_at) as application_date
  from
    bronze.portal_applicant_job_listings jl
    inner join reqs r on jl.requisition_id = r.requisition_id
  group by
    jl.applicant_id,
    jl.requisition_id
),
application_sources as (
  select
    distinct on (jl.applicant_id, jl.requisition_id)
    jl.applicant_id,
    jl.requisition_id,
    COALESCE(s.name, 'Unknown') as source_name
  from
    bronze.portal_applicant_job_listings jl
    inner join reqs r on jl.requisition_id = r.requisition_id
    left join bronze.portal_applicant_sources s on jl.applicant_source_id = s.id
  order by
    jl.applicant_id,
    jl.requisition_id,
    jl.created_at  -- Earliest application first
),
recruiters as (
  select
    p.id as recruiter_id,
    concat(p.first_name, ' ', p.last_name) as recruiter_name
  from
    bronze.portal_users p
),
-- -------------------------------------------------------------------------------------------
-- First Contact SMS: First outbound text message sent to applicant AFTER their application
-- -------------------------------------------------------------------------------------------
-- Attribution Logic:
--   For each applicant-requisition pair, find the FIRST outbound SMS that occurred
--   AFTER that specific application. This ensures each application gets its own
--   first contact tracking, even if an applicant applied to multiple requisitions.
--
-- Time Window:
--   SMS must occur within 30 days of the application to be attributed to that
--   requisition. Texts sent more than 30 days after application are not linked.
--
-- Source:
--   bronze.portal_applicant_texts (Sense SMS system logs)
--   - status = 'Delivered' indicates outbound messages from recruiters
--   - from_user links to portal_users for recruiter identification
--
-- Note:
--   Includes both individual and bulk/template texts. All outbound SMS represents
--   legitimate recruiter outreach regardless of whether templates were used.
-- -------------------------------------------------------------------------------------------
first_contact_sms as (
  select distinct on (jl.applicant_id, jl.requisition_id)
    jl.requisition_id,
    jl.applicant_id,
    rc.recruiter_name as first_contact_sms_recruiter_name,
    t.created_at as first_contact_sms_date
  from
    bronze.portal_applicant_job_listings jl
    inner join reqs r on jl.requisition_id = r.requisition_id
    inner join bronze.portal_applicant_texts t
      on jl.applicant_id = t.applicant_id
      and t.status = 'Delivered'  -- Outbound SMS only
      and t.created_at > jl.created_at  -- SMS must be AFTER application
      and t.created_at <= jl.created_at + interval '30 days'  -- Within 30-day window
    left join recruiters rc on t.from_user = rc.recruiter_id
  order by
    jl.applicant_id,
    jl.requisition_id,
    t.created_at  -- First SMS for this applicant-requisition pair
),
applicant_views as (
  select
    distinct on (av.applicant_id, av.requisition_id) av.applicant_id,
    av.requisition_id,
    rc.recruiter_name as first_view_recruiter_name,
    av.created_at as first_applicant_view_date
  from
    bronze.portal_applicant_views av
    inner join reqs r on av.requisition_id = r.requisition_id
    left join recruiters rc on av.recruiter_id = rc.recruiter_id
  where
    av.requisition_id is not null
  order by
    av.applicant_id,
    av.requisition_id,
    av.created_at
),
phone_screens as (
  select
    distinct on (rs.requisition_id, rs.applicant_id) rs.requisition_id,
    rs.applicant_id,
    rc.recruiter_name as phone_screen_recruiter_name,
    rs.created_at as first_phone_screen_date
  from
    bronze.portal_requisition_statistics rs
    inner join reqs r on rs.requisition_id = r.requisition_id
    left join recruiters rc on rs.created_by_recruiter_id = rc.recruiter_id
  where
    rs.requisition_statistic_type_id = 2 -- 2 = phone screen
    and extract (
      'Year'
      from
        rs.created_at
    ) >= 2024
  order by
    rs.requisition_id,
    rs.applicant_id,
    rs.created_at
),
inno_interviews as (
  select
    distinct on (rs.requisition_id, rs.applicant_id) rs.requisition_id,
    rs.applicant_id,
    rc.recruiter_name as inno_interview_recruiter_name,
    rs.created_at as first_interview_date
  from
    bronze.portal_requisition_statistics rs
    inner join reqs r on rs.requisition_id = r.requisition_id
    left join recruiters rc on rs.created_by_recruiter_id = rc.recruiter_id
  where
    rs.requisition_statistic_type_id = 3 -- 3 = interview
    and extract (
      'Year'
      from
        rs.created_at
    ) >= 2024
  order by
    rs.requisition_id,
    rs.applicant_id,
    rs.created_at
),
client_interviews as (
  select
    distinct on (rs.requisition_id, rs.applicant_id) rs.requisition_id,
    rs.applicant_id,
    rc.recruiter_name as client_interview_recruiter_name,
    rs.created_at as first_client_interview_date
  from
    bronze.portal_requisition_statistics rs
    inner join reqs r on rs.requisition_id = r.requisition_id
    left join recruiters rc on rs.created_by_recruiter_id = rc.recruiter_id
  where
    rs.requisition_statistic_type_id = 4 -- 4 = client interview
    and extract (
      'Year'
      from
        rs.created_at
    ) >= 2024
  order by
    rs.requisition_id,
    rs.applicant_id,
    rs.created_at
),
offers as (
  select
    distinct on (rs.requisition_id, rs.applicant_id) rs.requisition_id,
    rs.applicant_id,
    rc.recruiter_name as offer_recruiter_name,
    rs.created_at as first_offer_date
  from
    bronze.portal_requisition_statistics rs
    inner join reqs r on rs.requisition_id = r.requisition_id
    left join recruiters rc on rs.created_by_recruiter_id = rc.recruiter_id
  where
    rs.requisition_statistic_type_id = 5 -- 5 = offer
    and extract (
      'Year'
      from
        rs.created_at
    ) >= 2024
  order by
    rs.requisition_id,
    rs.applicant_id,
    rs.created_at
),
hires as (
  select
    jo.requisition_id,
    jo.applicant_id,
    min(jo.created_at) as first_hire_date
  from
    bronze.portal_applicant_job_offer_responses jo
    inner join reqs r on jo.requisition_id = r.requisition_id
  where
    extract (
      'Year'
      from
        jo.created_at
    ) >= 2024
    and not(jo.job_offer_response_id in (2, 3, 4, 5, 6))
  group by
    jo.requisition_id,
    jo.applicant_id
),
rejected as (
  select
    distinct on (rs.requisition_id, rs.applicant_id) rs.requisition_id,
    rs.applicant_id,
    rc.recruiter_name as rejected_recruiter_name,
    rs.created_at as first_rejected_date
  from
    bronze.portal_requisition_statistics rs
    inner join reqs r on rs.requisition_id = r.requisition_id
    left join recruiters rc on rs.created_by_recruiter_id = rc.recruiter_id
  where
    rs.requisition_statistic_type_id = 7 -- 7 = rejected to regional pool
    and extract (
      'Year'
      from
        rs.created_at
    ) >= 2024
  order by
    rs.requisition_id,
    rs.applicant_id,
    rs.created_at
),
offers_accepted as (
  select
    distinct on (rs.requisition_id, rs.applicant_id) rs.requisition_id,
    rs.applicant_id,
    rc.recruiter_name as offer_accepted_recruiter_name,
    rs.created_at as first_offer_acccepted_date
  from
    bronze.portal_requisition_statistics rs
    inner join reqs r on rs.requisition_id = r.requisition_id
    left join recruiters rc on rs.created_by_recruiter_id = rc.recruiter_id
  where
    rs.requisition_statistic_type_id = 8 -- 8 = offer accepted
    and extract (
      'Year'
      from
        rs.created_at
    ) >= 2024
  order by
    rs.requisition_id,
    rs.applicant_id,
    rs.created_at
),
fall_offs as (
  select
    distinct on (fo.requisition_id, fo.applicant_id) fo.requisition_id,
    fo.applicant_id,
    rc.recruiter_name as fall_off_recruiter_name,
    fo.reason as fall_off_reason,
    fo.created_at as first_fall_off_date
  from
    bronze.portal_applicant_fall_offs fo
    inner join reqs r on fo.requisition_id = r.requisition_id
    left join recruiters rc on fo.created_by_user_id = rc.recruiter_id
  order by
    fo.requisition_id,
    fo.applicant_id,
    fo.created_at
),
final as (
  select
    ra.client_id,
    ra.client_name,
    ra.requisition_id,
    ra.requisition_key,
    ra.is_pipeline,
    ra.use_jakib_ai,
    ra.is_resume_only,
    ra.fill_by,
    case
      ra.use_jakib_ai
      when true then 'MINNIE'
      else 'NO AI'
    end as ai_method,
    ra.applicant_id,
    ra.first_name || ' ' || ra.last_name as applicant_name,
    ra.email,
    ra.phone_number,
    asrc.source_name,
    ra.jakib_score,
    ra.conversation_type,
    ra.resume_only_score,
    case
      when ra.jakib_score > 86
      or ra.resume_only_score > 81 then 'GREEN'
      when ra.jakib_score is null
      and ra.resume_only_score > 81 then 'GREEN'
      when ra.jakib_score > 67
      or ra.resume_only_score > 57 then 'YELLOW'
      when ra.jakib_score is null
      and ra.resume_only_score > 57 then 'YELLOW'
      when ra.jakib_score >= 0
      or ra.resume_only_score >= 0 then 'RED'
      when ra.jakib_score is null
      and ra.resume_only_score >= 0 then 'RED'
      else 'NONE'
    end as ai_category,
    ad.application_date,
    av.first_applicant_view_date,
    av.first_view_recruiter_name,
    ra.score_created_at,
    ra.resume_only_score_created_at,
    sms.first_contact_sms_date,
    sms.first_contact_sms_recruiter_name,
    ps.first_phone_screen_date,
    ps.phone_screen_recruiter_name,
    i.first_interview_date,
    i.inno_interview_recruiter_name,
    ci.first_client_interview_date,
    ci.client_interview_recruiter_name,
    rj.first_rejected_date,
    rj.rejected_recruiter_name,
    o.first_offer_date,
    o.offer_recruiter_name,
    oa.first_offer_acccepted_date,
    oa.offer_accepted_recruiter_name,
    h.first_hire_date,
    fo.first_fall_off_date,
    fo.fall_off_reason,
    fo.fall_off_recruiter_name,
    case
      when av.first_applicant_view_date is not null then case
        when av.first_applicant_view_date >= coalesce(
          ra.score_created_at,
          ra.resume_only_score_created_at,
          av.first_applicant_view_date
        ) then false
        else true
      end
      else false
    end as is_prescore_view
  from
    req_applicants ra
    inner join application_dates ad on ra.requisition_id = ad.requisition_id
    and ra.applicant_id = ad.applicant_id
    left join application_sources asrc on ra.requisition_id = asrc.requisition_id
    and ra.applicant_id = asrc.applicant_id
    left join applicant_views av on ra.requisition_id = av.requisition_id
    and ra.applicant_id = av.applicant_id
    left join first_contact_sms sms on ra.requisition_id = sms.requisition_id
    and ra.applicant_id = sms.applicant_id
    left join phone_screens ps on ra.requisition_id = ps.requisition_id
    and ra.applicant_id = ps.applicant_id
    left join inno_interviews i on ra.requisition_id = i.requisition_id
    and ra.applicant_id = i.applicant_id
    left join client_interviews ci on ra.requisition_id = ci.requisition_id
    and ra.applicant_id = ci.applicant_id
    left join offers o on ra.requisition_id = o.requisition_id
    and ra.applicant_id = o.applicant_id
    left join offers_accepted oa on ra.requisition_id = oa.requisition_id
    and ra.applicant_id = oa.applicant_id
    left join hires h on ra.requisition_id = h.requisition_id
    and ra.applicant_id = h.applicant_id
    left join rejected rj on ra.requisition_id = rj.requisition_id
    and ra.applicant_id = rj.applicant_id
    left join fall_offs fo on ra.requisition_id = fo.requisition_id
    and ra.applicant_id = fo.applicant_id
)
select * from final
