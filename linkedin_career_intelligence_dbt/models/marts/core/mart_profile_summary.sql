select
    first_name,
    last_name,
    headline,
    industry,
    geo_location,
    profile_track,
    summary_length,
    summary_size_category,
    primary_contact_url,
    primary_contact_label,
    portfolio_website

from {{ ref('int_profile_enriched') }}
