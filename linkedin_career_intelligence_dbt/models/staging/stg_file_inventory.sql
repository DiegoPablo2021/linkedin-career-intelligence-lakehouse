select
    export_name,
    export_type,
    relative_path,
    file_name,
    file_stem,
    folder_name,
    file_size_bytes,
    file_size_kb,
    row_count,
    column_count,
    column_names,
    detected_encoding,
    read_success,
    error_message,
    cast(inventory_timestamp as timestamp) as inventory_timestamp
from bronze.file_inventory