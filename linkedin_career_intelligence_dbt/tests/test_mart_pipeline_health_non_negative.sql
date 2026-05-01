select *
from {{ ref('mart_pipeline_health_summary') }}
where
    total_inventory_files < 0
    or successful_reads < 0
    or failed_reads < 0
    or total_connections < 0
    or total_positions < 0
    or total_education_records < 0
    or total_certifications < 0
    or total_languages < 0
    or total_endorsements < 0
    or total_company_follows < 0
    or total_recommendations < 0
    or total_skills < 0
    or total_invitations < 0
    or total_events < 0
    or total_learning_records < 0
    or total_job_applications < 0
    or total_saved_job_alerts < 0
    or total_volunteering < 0
    or total_email_addresses < 0
    or total_phone_numbers < 0
