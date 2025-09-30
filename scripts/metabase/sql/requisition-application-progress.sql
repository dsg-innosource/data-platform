-- -------------------------------------------------------------------------------------------
-- Requisition Application Progress
-- -------------------------------------------------------------------------------------------
-- METABASE URL: https://innosource.metabaseapp.com/model/305-requisition-application-progress
with reqs as (
  select
    c.id as client_id,
    c.name as client_name,
    r.id as requisition_id,
    r.requisition_key,
    r.fill_by,
    r.use_jakib_ai,
    case when r.requisition_key like '%Pipeline%' then 1 else 0 end as is_pipeline
  from
    bronze.portal_requisitions r
    inner join bronze.portal_clients c on r.client_id = c.id
  where
    extract (
      'Year'
      from
        r.fill_by
    ) >= 2024
)
,req_applicant_list as (select distinct r.client_id,
                                    r.client_name,
                                    r.requisition_id,
                                    r.requisition_key,
                                    r.is_pipeline,
                                    r.use_jakib_ai,
                                    r.fill_by,
                                    a.id as applicant_id,
                                    a.first_name,
                                    a.last_name,
                                    a.email,
                                    a.phone_number
                    from reqs r
                             inner join bronze.portal_requisition_statistics rs on
                        r.requisition_id = rs.requisition_id
                             inner join bronze.portal_applicants a on
                        rs.applicant_id = a.id
)
,req_applicants as (select ra.*
                         , jr.score as jakib_score
                         , jr.conversation_type
                         , rs.resume_only_score
                         , jr.created_at as score_created_at
                         , rs.created_at as resume_only_score_created_at
                    from req_applicant_list ra
                             left join
                         bronze.portal_jakib_results jr on
                             ra.requisition_id = jr.requisition_id
                                 and ra.applicant_id = jr.applicant_id
                             left join
                         bronze.portal_resume_scores rs on
                             ra.requisition_id = rs.requisition_id
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
applicant_views as (
  select
    av.applicant_id,
    av.requisition_id,
    min(av.created_at) as first_applicant_view_date
  from
    bronze.portal_applicant_views av
    inner join reqs r on av.requisition_id = r.requisition_id
  where
    av.requisition_id is not null
  group by
    av.applicant_id,
    av.requisition_id
),
phone_screens as (
  select
    rs.requisition_id,
    rs.applicant_id,
    min(rs.created_at) as first_phone_screen_date
  from
    bronze.portal_requisition_statistics rs
    inner join reqs r on rs.requisition_id = r.requisition_id
  where
    rs.requisition_statistic_type_id = 2 -- 2 = phone screen
    and extract (
      'Year'
      from
        rs.created_at
    ) >= 2024
  group by
    rs.requisition_id,
    rs.applicant_id
),
inno_interviews as (
  select
    rs.requisition_id,
    rs.applicant_id,
    min(rs.created_at) as first_interview_date
  from
    bronze.portal_requisition_statistics rs
    inner join reqs r on rs.requisition_id = r.requisition_id
  where
    rs.requisition_statistic_type_id = 3 -- 3 = interview
    and extract (
      'Year'
      from
        rs.created_at
    ) >= 2024
  group by
    rs.requisition_id,
    rs.applicant_id
),
client_interviews as (
  select
    rs.requisition_id,
    rs.applicant_id,
    min(rs.created_at) as first_client_interview_date
  from
    bronze.portal_requisition_statistics rs
    inner join reqs r on rs.requisition_id = r.requisition_id
  where
    rs.requisition_statistic_type_id = 4 -- 4 = client interview
    and extract (
      'Year'
      from
        rs.created_at
    ) >= 2024
  group by
    rs.requisition_id,
    rs.applicant_id
),
offers as (
  select
    rs.requisition_id,
    rs.applicant_id,
    min(rs.created_at) as first_offer_date
  from
    bronze.portal_requisition_statistics rs
    inner join reqs r on rs.requisition_id = r.requisition_id
  where
    rs.requisition_statistic_type_id = 5 -- 5 = offer
    and extract (
      'Year'
      from
        rs.created_at
    ) >= 2024
  group by
    rs.requisition_id,
    rs.applicant_id
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

AND not(jo.job_offer_response_id in (2, 3, 4, 5, 6))
  group by
    jo.requisition_id,
    jo.applicant_id
),
rejected as (
  select
    rs.requisition_id,
    rs.applicant_id,
    min(rs.created_at) as first_rejected_date
  from
    bronze.portal_requisition_statistics rs
    inner join reqs r on rs.requisition_id = r.requisition_id
  where
    rs.requisition_statistic_type_id = 7 -- 7 = rejected to regional pool
    and extract (
      'Year'
      from
        rs.created_at
    ) >= 2024
  group by
    rs.requisition_id,
    rs.applicant_id
),
offers_accepted as (
  select
    rs.requisition_id,
    rs.applicant_id,
    min(rs.created_at) as first_offer_acccepted_date
  from
    bronze.portal_requisition_statistics rs
    inner join reqs r on rs.requisition_id = r.requisition_id
  where
    rs.requisition_statistic_type_id = 8 -- 8 = offer accepted
    and extract (
      'Year'
      from
        rs.created_at
    ) >= 2024
  group by
    rs.requisition_id,
    rs.applicant_id
),
final as (
  select
    ra.client_id,
    ra.client_name,
    ra.requisition_id,
    ra.requisition_key,
    ra.is_pipeline,
    ra.use_jakib_ai,
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
    ra.jakib_score,
    ra.conversation_type,
    ra.resume_only_score,
    case
        	when ra.jakib_score > 86 or ra.resume_only_score > 81  then 'GREEN'
			when ra.jakib_score is null and ra.resume_only_score > 81  then 'GREEN'
			when ra.jakib_score > 67 or ra.resume_only_score > 57 then 'YELLOW'
			when ra.jakib_score is null and ra.resume_only_score > 57 then 'YELLOW'
			when ra.jakib_score >= 0 or ra.resume_only_score >= 0 then 'RED'
			when ra.jakib_score is null and ra.resume_only_score >= 0 then 'RED'
			else 'NONE'
    end as ai_category,
    ad.application_date,
    av.first_applicant_view_date, ra.score_created_at, ra.resume_only_score_created_at,
    ps.first_phone_screen_date,
    i.first_interview_date,
    ci.first_client_interview_date,
    rj.first_rejected_date,
    o.first_offer_date,
    oa.first_offer_acccepted_date,
    h.first_hire_date,
    case
        when av.first_applicant_view_date is not null then
            case
                when av.first_applicant_view_date >= coalesce(ra.score_created_at, ra.resume_only_score_created_at, av.first_applicant_view_date) then false else true
            end
        else false
    end as is_prescore_view
  from
    req_applicants ra
    inner join application_dates ad on ra.requisition_id = ad.requisition_id
    and ra.applicant_id = ad.applicant_id
    left join applicant_views av on ra.requisition_id = av.requisition_id
    and ra.applicant_id = av.applicant_id
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
)
select
  *
from
  final;
