select
    table_key,
    bronze_table,
    source_file,
    source_path,
    export_type,
    cast(coalesce(source_row_count, row_count) as bigint) as source_row_count,
    cast(coalesce(source_column_count, column_count) as bigint) as source_column_count,
    cast(coalesce(row_count_after_transform, row_count) as bigint) as row_count_after_transform,
    cast(
        coalesce(column_count_after_transform, source_column_count, column_count) as bigint
    ) as column_count_after_transform,
    cast(coalesce(rows_removed_during_transform, 0) as bigint) as rows_removed_during_transform,
    cast(duplicate_rows_after_transform as bigint) as duplicate_rows_after_transform,
    contract_owner,
    contract_description,
    required_columns,
    coalesce(non_empty_columns, '[]') as non_empty_columns,
    coalesce(unique_columns, '[]') as unique_columns,
    coalesce(null_rate_by_column, '{}') as null_rate_by_column_json,
    coalesce(source_null_rate_by_column, '{}') as source_null_rate_by_column_json,
    sensitive_columns,
    cast(loaded_at_utc as timestamp) as loaded_at_utc,
    cast(cast(loaded_at_utc as timestamp) as date) as loaded_on,
    strftime(cast(loaded_at_utc as timestamp), '%Y-%m') as loaded_year_month
from {{ source('bronze', 'ingestion_audit') }}
