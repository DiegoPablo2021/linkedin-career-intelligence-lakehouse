with audit as (
    select *
    from {{ ref('stg_ingestion_audit') }}
),
enriched as (
    select
        loaded_at_utc,
        loaded_on,
        loaded_year_month,
        table_key,
        bronze_table,
        source_file,
        export_type,
        source_row_count,
        source_column_count,
        row_count_after_transform,
        column_count_after_transform,
        rows_removed_during_transform,
        duplicate_rows_after_transform,
        contract_owner,
        contract_description,
        required_columns,
        non_empty_columns,
        unique_columns,
        null_rate_by_column_json,
        source_null_rate_by_column_json,

        case
            when source_row_count > 0
                then round(row_count_after_transform * 1.0 / source_row_count, 4)
        end as row_retention_rate,

        row_count_after_transform - lag(row_count_after_transform) over (
            partition by table_key
            order by loaded_at_utc
        ) as row_count_change_vs_previous_load,

        rows_removed_during_transform - lag(rows_removed_during_transform) over (
            partition by table_key
            order by loaded_at_utc
        ) as rows_removed_change_vs_previous_load

    from audit
)

select
    loaded_at_utc,
    loaded_on,
    loaded_year_month,
    table_key,
    bronze_table,
    source_file,
    export_type,
    source_row_count,
    source_column_count,
    row_count_after_transform,
    column_count_after_transform,
    rows_removed_during_transform,
    duplicate_rows_after_transform,
    row_retention_rate,
    row_count_change_vs_previous_load,
    rows_removed_change_vs_previous_load,
    contract_owner,
    contract_description,
    required_columns,
    non_empty_columns,
    unique_columns,
    null_rate_by_column_json,
    source_null_rate_by_column_json,
    duplicate_rows_after_transform > 0 as duplicate_alert_flag,
    rows_removed_during_transform > 0 as row_removal_alert_flag,

    case
        when source_row_count = 0 then 'sem_dados'
        when row_count_after_transform = 0 then 'esvaziado'
        when rows_removed_during_transform > 0 then 'atencao'
        else 'saudavel'
    end as health_status,

    1 as successful_load_flag
from enriched
