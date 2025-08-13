/*
 * Metabase Model: Applicant Sources
 * URL: https://innosource.metabaseapp.com/model/506-applicant-sources
 * Description: Application-level source tracking model that captures each application instance
 *              with the source used for that specific application
 */

select
    ajl.applicant_id,
    ajl.requisition_id,
    r.client_id,
    ajl.applicant_source_id,
    s.name as applicant_source_name,
    s.url_code as source_url_code,
    ajl.created_at as application_date,
    ajl.job_listing_id,
    ajl.is_from_api,
    ajl.is_internal_move,
    -- Additional context fields
    a.first_name,
    a.last_name,
    a.email,
    a.created_at as applicant_first_created_at
from bronze.portal_applicant_job_listings ajl
inner join bronze.portal_requisitions r
    on ajl.requisition_id = r.id
inner join bronze.portal_applicant_sources s
    on ajl.applicant_source_id = s.id
inner join bronze.portal_applicants a
    on ajl.applicant_id = a.id
where a.deleted_at is null
