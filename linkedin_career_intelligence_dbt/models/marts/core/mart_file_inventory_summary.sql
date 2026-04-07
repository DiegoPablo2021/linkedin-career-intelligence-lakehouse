select
    export_type,
    categoria_dado,
    status_leitura,
    volume_categoria,
    count(*) as total_arquivos,
    sum(coalesce(row_count, 0)) as total_linhas,
    sum(coalesce(column_count, 0)) as total_colunas,
    round(sum(coalesce(file_size_kb, 0)), 2) as total_tamanho_kb,
    min(inventory_timestamp) as primeira_execucao_inventario,
    max(inventory_timestamp) as ultima_execucao_inventario
from {{ ref('int_file_inventory_enriched') }}
group by
    export_type,
    categoria_dado,
    status_leitura,
    volume_categoria