select
    export_name,
    export_type,
    folder_name,
    file_name,
    file_stem,

    row_count,
    column_count,
    file_size_kb,

    case
        when row_count is null then 'erro_leitura'
        when row_count = 0 then 'vazio'
        when row_count < 10 then 'muito_pequeno'
        when row_count < 100 then 'pequeno'
        when row_count < 1000 then 'medio'
        else 'grande'
    end as volume_categoria,

    case
        when read_success = false then 'erro'
        else 'ok'
    end as status_leitura,

    case
        when folder_name = 'Jobs' then 'job_data'
        when folder_name = 'root' then 'core_profile'
        when folder_name = 'Services Marketplace' then 'services'
        when folder_name = 'Verifications' then 'trust'
        else 'outros'
    end as categoria_dado,

    inventory_timestamp

from {{ ref('stg_file_inventory') }}