with audit as (
    select *
    from {{ ref('stg_ingestion_audit') }}
),
null_rates as (
    select
        audit.loaded_at_utc,
        audit.loaded_on,
        audit.loaded_year_month,
        audit.table_key,
        audit.bronze_table,
        audit.export_type,
        audit.source_file,
        audit.source_row_count,
        audit.row_count_after_transform,
        audit.rows_removed_during_transform,
        rates.key as monitored_column,
        try_cast(rates.value as double) as null_rate_after_transform,
        try_cast(source_rates.value as double) as null_rate_before_transform
    from audit
    left join json_each(audit.null_rate_by_column_json) as rates
        on true
    left join json_each(audit.source_null_rate_by_column_json) as source_rates
        on rates.key = source_rates.key
)

select
    loaded_at_utc,
    loaded_on,
    loaded_year_month,
    table_key,
    bronze_table,
    export_type,
    source_file,
    source_row_count,
    row_count_after_transform,
    rows_removed_during_transform,
    monitored_column,
    null_rate_before_transform,
    null_rate_after_transform,
    null_rate_after_transform - null_rate_before_transform as null_rate_delta,
    null_rate_after_transform - lag(null_rate_after_transform) over (
        partition by table_key, monitored_column
        order by loaded_at_utc
    ) as null_rate_change_vs_previous_load,
    coalesce(null_rate_after_transform, 0) > 0 as null_rate_alert_flag
from null_rates
where monitored_column is not null
