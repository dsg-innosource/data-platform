/*
 * Metabase Model: ADP Tenure
 * URL: [Add Metabase URL here]
 * Description: Basic ADP tenure data with calculated hire/termination dates and report date
 * Created: [Add creation date]
 * Last Updated: [Add last updated date]
 */

select file_number,
       payroll_name,
       hire_date,
       rehire_date,
       previous_termination_date,
       termination_date,
       coalesce(h.rehire_date, h.hire_date) as calc_hire_date,
       coalesce(h.termination_date, h.previous_termination_date) as calc_termination_date,
       termination_reason,
       position_status,
       leave_of_absence_start_date,
       leave_of_absence_return_date,
       home_department_code,
       home_department_description,
       payroll_company_code,
       position_id,
       client_code,
       client,
       regular_pay_rate,
       recruited_by,
       business_unit,
       requisition_key,
       email,
       adp_id,
       requisition_id,
       applicant_id,
       regular_hours,
       ot_hours,
       pto_sick_hours,
       holiday_hours,
       voluntary_involuntary_flag,
       home_phone,
       snapshot_date,
       (snapshot_date - interval '7 days')::date as report_date
from bronze.adp_tenure_history h
where h.snapshot_date = (
    select max(snapshot_date) from bronze.adp_tenure_history
    )
