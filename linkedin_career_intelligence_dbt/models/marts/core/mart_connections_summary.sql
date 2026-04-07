select
    connection_year,
    connection_month,
    connection_year_month,

    count(*) as total_connections,

    sum(case when has_email then 1 else 0 end) as connections_with_email,

    count(distinct company_clean) as unique_companies,
    count(distinct position_clean) as unique_positions

from {{ ref('int_connections_enriched') }}

group by
    connection_year,
    connection_month,
    connection_year_month

order by
    connection_year,
    connection_month