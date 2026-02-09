-- -------------------------------------------------------------------------------------------
-- Requisitions
-- -------------------------------------------------------------------------------------------
-- METABASE URL: https://innosource.metabaseapp.com/model/545-requisitions
-- DESCRIPTION: The base requisition model. Includes requisition details, client info,
--              AI usage flags, and hired count per requisition.
-- NOTE: No DP-ID assigned yet
-- -------------------------------------------------------------------------------------------
with hires as (
  select
    jo.requisition_id,
    count(distinct jo.applicant_id) as hired_count
  from
    bronze.portal_applicant_job_offer_responses jo
  where
    not(jo.job_offer_response_id in (2, 3, 4, 5, 6))
  group by
    jo.requisition_id
)
select
       r.id as requisition_id,
       fill_by,
       pay_rate,
       position,
       client_id,
       closed_at,
       is_closed,
       open_date,
       agent_type,
       class_size,
       r.created_at as requisition_created_at,
       is_pipeline,
       requested_by,
       use_jakib_ai,
       department_id,
       client_manager,
       requisition_key,
       use_voice_call_ai,
       number_of_openings,
       l.name as requisition_type,
       number_of_other_vendors,
       h.hired_count
from
    bronze.portal_requisitions r
left join bronze.portal_dashboard_lines_of_business l on r.requisition_type_id = l.id
left join hires h on r.id = h.requisition_id
where r.deleted_at is null;
