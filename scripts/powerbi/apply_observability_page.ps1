param(
    [string]$DataSource = "localhost:64360",
    [string]$Catalog = "ddc1c5c0-0fd6-4db0-8b78-00331a136ffc",
    [string]$ProjectRoot = "C:\Projetos\linkedIn_career_intelligence_lakehouse"
)

$ErrorActionPreference = "Stop"

[void][System.Reflection.Assembly]::LoadFrom("C:\Program Files\DAX Studio\bin\Microsoft.AnalysisServices.Core.dll")
[void][System.Reflection.Assembly]::LoadFrom("C:\Program Files\DAX Studio\bin\Microsoft.AnalysisServices.Tabular.dll")

$script:Changes = [System.Collections.Generic.List[string]]::new()

function Add-Change {
    param([string]$Message)
    $script:Changes.Add($Message)
}

function Connect-Model {
    param(
        [string]$ServerName,
        [string]$DatabaseName
    )

    $server = New-Object Microsoft.AnalysisServices.Tabular.Server
    $server.Connect("DataSource=$ServerName")
    $database = $server.Databases.FindByName($DatabaseName)
    if (-not $database) {
        throw "Database '$DatabaseName' not found on $ServerName."
    }

    [PSCustomObject]@{
        Server   = $server
        Database = $database
        Model    = $database.Model
    }
}

function Save-Model {
    param($Context)
    $Context.Model.SaveChanges()
}

function Refresh-TableFull {
    param(
        $Context,
        [string]$TableName
    )

    $table = $Context.Model.Tables.Find($TableName)
    if (-not $table) {
        throw "Table '$TableName' not found for refresh."
    }

    $table.RequestRefresh([Microsoft.AnalysisServices.Tabular.RefreshType]::Full)
    Save-Model -Context $Context
    Add-Change "Refreshed table '$TableName'."
}

function Refresh-ModelCalculate {
    param($Context)
    $Context.Model.RequestRefresh([Microsoft.AnalysisServices.Tabular.RefreshType]::Calculate)
    Save-Model -Context $Context
    Add-Change "Calculated model."
}

function Ensure-ImportTable {
    param(
        $Context,
        [string]$TableName,
        [string]$MExpression,
        [string]$Description
    )

    $table = $Context.Model.Tables.Find($TableName)
    if ($table) {
        $partition = $table.Partitions.Find($TableName)
        if ($partition -and $partition.Source -is [Microsoft.AnalysisServices.Tabular.CalculatedPartitionSource]) {
            $Context.Model.Tables.Remove($table)
            Save-Model -Context $Context
            Add-Change "Removed calculated placeholder table '$TableName' to recreate it as import table."
            $table = $null
        }
    }

    if ($table) {
        $partition = $table.Partitions.Find($TableName)
        if (-not $partition) {
            $partition = New-Object Microsoft.AnalysisServices.Tabular.Partition
            $partition.Name = $TableName
            $partition.Source = New-Object Microsoft.AnalysisServices.Tabular.MPartitionSource
            $table.Partitions.Add($partition)
        }
        if ($partition.Source -isnot [Microsoft.AnalysisServices.Tabular.MPartitionSource]) {
            $partition.Source = New-Object Microsoft.AnalysisServices.Tabular.MPartitionSource
        }
        $partition.Source.Expression = $MExpression
        if ($table.Description -ne $Description) {
            $table.Description = $Description
            Add-Change "Updated description on table '$TableName'."
        }
        Save-Model -Context $Context
        Add-Change "Updated import expression for table '$TableName'."
        return $true
    }

    $table = New-Object Microsoft.AnalysisServices.Tabular.Table
    $table.Name = $TableName
    $table.Description = $Description

    $partition = New-Object Microsoft.AnalysisServices.Tabular.Partition
    $partition.Name = $TableName
    $partition.Source = New-Object Microsoft.AnalysisServices.Tabular.MPartitionSource
    $partition.Source.Expression = $MExpression
    $table.Partitions.Add($partition)

    $Context.Model.Tables.Add($table)
    Save-Model -Context $Context
    Add-Change "Created import table '$TableName'."
    return $true
}

function Ensure-CalculatedTable {
    param(
        $Context,
        [string]$TableName,
        [string]$Expression,
        [string]$Description
    )

    $table = $Context.Model.Tables.Find($TableName)
    if ($table) {
        $partition = $table.Partitions.Find($TableName)
        if ($partition -and $partition.Source -is [Microsoft.AnalysisServices.Tabular.CalculatedPartitionSource]) {
            $partition.Source.Expression = $Expression
        }
        $table.Description = $Description
        Add-Change "Updated calculated table '$TableName'."
        return
    }

    $table = New-Object Microsoft.AnalysisServices.Tabular.Table
    $table.Name = $TableName
    $table.Description = $Description

    $partition = New-Object Microsoft.AnalysisServices.Tabular.Partition
    $partition.Name = $TableName
    $partition.Source = New-Object Microsoft.AnalysisServices.Tabular.CalculatedPartitionSource
    $partition.Source.Expression = $Expression
    $table.Partitions.Add($partition)

    $Context.Model.Tables.Add($table)
    Save-Model -Context $Context
    Add-Change "Created calculated table '$TableName'."
}

function Ensure-DataColumn {
    param(
        $Context,
        [string]$TableName,
        [string]$ColumnName,
        [string]$SourceColumn,
        [string]$DataType,
        [string]$SummarizeBy
    )

    $table = $Context.Model.Tables.Find($TableName)
    if (-not $table) {
        throw "Table '$TableName' not found while creating data column '$ColumnName'."
    }

    $column = $table.Columns.Find($ColumnName)
    if (-not $column) {
        $column = New-Object Microsoft.AnalysisServices.Tabular.DataColumn
        $column.Name = $ColumnName
        $table.Columns.Add($column)
        Add-Change "Created column '$TableName[$ColumnName]'."
    }

    $column.SourceColumn = $SourceColumn
    $column.DataType = [Microsoft.AnalysisServices.Tabular.DataType]::$DataType
    $column.SummarizeBy = [Microsoft.AnalysisServices.Tabular.AggregateFunction]::$SummarizeBy
}

function Ensure-ColumnMetadata {
    param(
        $Context,
        [string]$TableName,
        [string]$ColumnName,
        [string]$Description,
        [bool]$Hidden
    )

    $table = $Context.Model.Tables.Find($TableName)
    if (-not $table) {
        throw "Table '$TableName' not found while updating column metadata."
    }

    $column = $table.Columns.Find($ColumnName)
    if (-not $column) {
        throw "Column '$TableName[$ColumnName]' not found."
    }

    $changed = $false
    if ($column.Description -ne $Description) {
        $column.Description = $Description
        $changed = $true
    }
    if ($column.IsHidden -ne $Hidden) {
        $column.IsHidden = $Hidden
        $changed = $true
    }
    if ($changed) {
        Add-Change "Updated metadata for '$TableName[$ColumnName]'."
    }
}

function Find-Relationship {
    param(
        $Context,
        [string]$FromTable,
        [string]$FromColumn,
        [string]$ToTable,
        [string]$ToColumn
    )

    foreach ($relationship in $Context.Model.Relationships) {
        if ($relationship -is [Microsoft.AnalysisServices.Tabular.SingleColumnRelationship]) {
            if (
                $relationship.FromColumn.Table.Name -eq $FromTable -and
                $relationship.FromColumn.Name -eq $FromColumn -and
                $relationship.ToColumn.Table.Name -eq $ToTable -and
                $relationship.ToColumn.Name -eq $ToColumn
            ) {
                return $relationship
            }
        }
    }

    return $null
}

function Ensure-Relationship {
    param(
        $Context,
        [string]$FromTable,
        [string]$FromColumn,
        [string]$ToTable,
        [string]$ToColumn
    )

    $existing = Find-Relationship -Context $Context -FromTable $FromTable -FromColumn $FromColumn -ToTable $ToTable -ToColumn $ToColumn
    if ($existing) {
        $existing.IsActive = $true
        $existing.CrossFilteringBehavior = [Microsoft.AnalysisServices.Tabular.CrossFilteringBehavior]::OneDirection
        Add-Change "Validated relationship '$FromTable[$FromColumn]' -> '$ToTable[$ToColumn]'."
        return
    }

    $relationship = New-Object Microsoft.AnalysisServices.Tabular.SingleColumnRelationship
    $relationship.FromColumn = $Context.Model.Tables.Find($FromTable).Columns.Find($FromColumn)
    $relationship.ToColumn = $Context.Model.Tables.Find($ToTable).Columns.Find($ToColumn)
    $relationship.IsActive = $true
    $relationship.CrossFilteringBehavior = [Microsoft.AnalysisServices.Tabular.CrossFilteringBehavior]::OneDirection
    $Context.Model.Relationships.Add($relationship)
    Add-Change "Created relationship '$FromTable[$FromColumn]' -> '$ToTable[$ToColumn]'."
}

function Remove-ProhibitedRelationships {
    param($Context)

    $toRemove = @()
    foreach ($relationship in $Context.Model.Relationships) {
        if ($relationship -isnot [Microsoft.AnalysisServices.Tabular.SingleColumnRelationship]) {
            continue
        }

        $fromTable = $relationship.FromColumn.Table.Name
        $toTable = $relationship.ToColumn.Table.Name
        $pair = @($fromTable, $toTable) | Sort-Object

        if ($pair[0] -eq "fact_ingestion_audit_health_timeline" -and $pair[1] -eq "fact_ingestion_audit_null_rate_timeline") {
            $toRemove += $relationship
        }
    }

    foreach ($relationship in $toRemove) {
        $Context.Model.Relationships.Remove($relationship)
        Add-Change "Removed prohibited fact-to-fact relationship."
    }
}

function Ensure-Measure {
    param(
        $Context,
        [string]$Name,
        [string]$Expression,
        [string]$Description,
        [string]$FormatString,
        [string]$DisplayFolder,
        [bool]$NormalizeQuotes = $true
    )

    $table = $Context.Model.Tables.Find("_Measures")
    if (-not $table) {
        throw "Table '_Measures' not found."
    }

    $measure = $table.Measures.Find($Name)
    if (-not $measure) {
        $measure = New-Object Microsoft.AnalysisServices.Tabular.Measure
        $measure.Name = $Name
        $table.Measures.Add($measure)
        Add-Change "Created measure '$Name'."
    } else {
        Add-Change "Updated measure '$Name'."
    }

    if ($NormalizeQuotes) {
        $measure.Expression = $Expression.Replace('""', '"')
    } else {
        $measure.Expression = $Expression
    }
    $measure.Description = $Description
    $measure.FormatString = $FormatString
    $measure.DisplayFolder = $DisplayFolder
}

$healthCsv = Join-Path $ProjectRoot "powerbi\exports\fact_ingestion_audit_health_timeline.csv"
$nullRateCsv = Join-Path $ProjectRoot "powerbi\exports\fact_ingestion_audit_null_rate_timeline.csv"
$tableDimCsv = Join-Path $ProjectRoot "powerbi\exports\dim_observability_table.csv"
$exportDimCsv = Join-Path $ProjectRoot "powerbi\exports\dim_observability_export_type.csv"
$healthStatusDimCsv = Join-Path $ProjectRoot "powerbi\exports\dim_observability_health_status.csv"
$columnDimCsv = Join-Path $ProjectRoot "powerbi\exports\dim_observability_column.csv"

$healthExpression = @"
let
    Source = Csv.Document(File.Contents("$healthCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"loaded_at_utc", type datetime}, {"loaded_on", type date}, {"loaded_year_month", type text}, {"table_key", type text}, {"bronze_table", type text}, {"source_file", type text}, {"export_type", type text}, {"source_row_count", Int64.Type}, {"source_column_count", Int64.Type}, {"row_count_after_transform", Int64.Type}, {"column_count_after_transform", Int64.Type}, {"rows_removed_during_transform", Int64.Type}, {"duplicate_rows_after_transform", Int64.Type}, {"row_retention_rate", type number}, {"row_count_change_vs_previous_load", Int64.Type}, {"rows_removed_change_vs_previous_load", Int64.Type}, {"duplicate_alert_flag", type logical}, {"row_removal_alert_flag", type logical}, {"health_status", type text}, {"successful_load_flag", Int64.Type}}, "en-US")
in
    #"Changed Type"
"@

$nullRateExpression = @"
let
    Source = Csv.Document(File.Contents("$nullRateCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"loaded_at_utc", type datetime}, {"loaded_on", type date}, {"loaded_year_month", type text}, {"table_key", type text}, {"bronze_table", type text}, {"export_type", type text}, {"source_file", type text}, {"source_row_count", Int64.Type}, {"row_count_after_transform", Int64.Type}, {"rows_removed_during_transform", Int64.Type}, {"monitored_column", type text}, {"null_rate_before_transform", type number}, {"null_rate_after_transform", type number}, {"null_rate_delta", type number}, {"null_rate_change_vs_previous_load", type number}, {"null_rate_alert_flag", type logical}}, "en-US")
in
    #"Changed Type"
"@

$tableDimensionExpression = @"
let
    Source = Csv.Document(File.Contents("$tableDimCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"table_key", type text}})
in
    #"Changed Type"
"@

$exportTypeDimensionExpression = @"
let
    Source = Csv.Document(File.Contents("$exportDimCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"export_type", type text}})
in
    #"Changed Type"
"@

$healthStatusDimensionExpression = @"
let
    Source = Csv.Document(File.Contents("$healthStatusDimCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"health_status", type text}})
in
    #"Changed Type"
"@

$columnDimensionExpression = @"
let
    Source = Csv.Document(File.Contents("$columnDimCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"monitored_column", type text}})
in
    #"Changed Type"
"@

$measureDefinitions = @(
    @{
        Name = "Monitored Loads"
        Expression = "COUNTROWS ( fact_ingestion_audit_health_timeline )"
        Description = "Conta o total de cargas monitoradas no contexto de filtro atual da página de observability."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Total Successful Loads"
        Expression = "SUM ( fact_ingestion_audit_health_timeline[successful_load_flag] )"
        Description = "Soma o total de cargas bem-sucedidas registradas no contexto atual."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Successful Load Rate %"
        Expression = "DIVIDE ( [Total Successful Loads], [Monitored Loads] )"
        Description = "Calcula a taxa de sucesso das cargas monitoradas no contexto atual."
        FormatString = "0.0%"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Total Source Rows"
        Expression = "SUM ( fact_ingestion_audit_health_timeline[source_row_count] )"
        Description = "Soma o total de linhas brutas recebidas pelas cargas monitoradas."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Total Rows After Transform"
        Expression = "SUM ( fact_ingestion_audit_health_timeline[row_count_after_transform] )"
        Description = "Soma o total de linhas preservadas após a transformação."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Total Rows Removed"
        Expression = "SUM ( fact_ingestion_audit_health_timeline[rows_removed_during_transform] )"
        Description = "Soma o volume de linhas removidas durante a transformação."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Rows Removed Rate %"
        Expression = "DIVIDE ( [Total Rows Removed], [Total Source Rows] )"
        Description = "Calcula a taxa de remoção de linhas em relação ao volume bruto de origem."
        FormatString = "0.0%"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Average Row Retention Rate"
        Expression = "AVERAGE ( fact_ingestion_audit_health_timeline[row_retention_rate] )"
        Description = "Calcula a retenção média de linhas após transformação nas cargas monitoradas."
        FormatString = "0.0%"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Health Issue Load Count"
        Expression = @"
CALCULATE (
    COUNTROWS ( fact_ingestion_audit_health_timeline ),
    fact_ingestion_audit_health_timeline[health_status] <> ""saudavel""
)
"@
        Description = "Conta quantas cargas monitoradas apresentam status de saúde diferente de saudável."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Tables With Health Issues"
        Expression = @"
CALCULATE (
    DISTINCTCOUNT ( fact_ingestion_audit_health_timeline[table_key] ),
    fact_ingestion_audit_health_timeline[health_status] <> ""saudavel""
)
"@
        Description = "Conta quantas tabelas monitoradas apresentam pelo menos uma carga com problema."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Duplicate Alerts"
        Expression = @"
CALCULATE (
    COUNTROWS ( fact_ingestion_audit_health_timeline ),
    fact_ingestion_audit_health_timeline[duplicate_alert_flag] = TRUE ()
)
"@
        Description = "Conta quantas cargas geraram alerta de duplicidade."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Row Removal Alerts"
        Expression = @"
CALCULATE (
    COUNTROWS ( fact_ingestion_audit_health_timeline ),
    fact_ingestion_audit_health_timeline[row_removal_alert_flag] = TRUE ()
)
"@
        Description = "Conta quantas cargas geraram alerta de remoção excessiva de linhas."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Average Null Rate"
        Expression = "AVERAGE ( fact_ingestion_audit_null_rate_timeline[null_rate_after_transform] )"
        Description = "Calcula o null rate médio após transformação nas colunas monitoradas."
        FormatString = "0.0%"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Max Null Rate"
        Expression = "MAX ( fact_ingestion_audit_null_rate_timeline[null_rate_after_transform] )"
        Description = "Retorna o pior null rate observado após transformação no contexto atual."
        FormatString = "0.0%"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Average Null Rate Delta"
        Expression = "AVERAGE ( fact_ingestion_audit_null_rate_timeline[null_rate_delta] )"
        Description = "Calcula a variação média do null rate antes e depois da transformação."
        FormatString = "0.0%"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Null Rate Alerts"
        Expression = @"
CALCULATE (
    COUNTROWS ( fact_ingestion_audit_null_rate_timeline ),
    fact_ingestion_audit_null_rate_timeline[null_rate_alert_flag] = TRUE ()
)
"@
        Description = "Conta quantas colunas monitoradas dispararam alerta de null rate."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Monitored Tables"
        Expression = @"
COUNTROWS (
    DISTINCT (
        UNION (
            SELECTCOLUMNS ( fact_ingestion_audit_health_timeline, ""table_key"", fact_ingestion_audit_health_timeline[table_key] ),
            SELECTCOLUMNS ( fact_ingestion_audit_null_rate_timeline, ""table_key"", fact_ingestion_audit_null_rate_timeline[table_key] )
        )
    )
)
"@
        Description = "Conta quantas tabelas estão presentes nas camadas de observability no contexto atual."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Monitored Columns"
        Expression = "DISTINCTCOUNT ( fact_ingestion_audit_null_rate_timeline[monitored_column] )"
        Description = "Conta quantas colunas estão monitoradas para null rate no contexto atual."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Latest Load Date"
        Expression = "MAX ( fact_ingestion_audit_health_timeline[loaded_at_utc] )"
        Description = "Retorna o timestamp mais recente de carga observado na camada de observability."
        FormatString = "dd/mm/yyyy hh:mm"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Freshness Days"
        Expression = @"
VAR _latest = [Latest Load Date]
RETURN
    IF (
        NOT ISBLANK ( _latest ),
        DATEDIFF ( DATEVALUE ( _latest ), TODAY (), DAY )
    )
"@
        Description = "Calcula o número de dias transcorridos desde a última carga observada."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Critical Alert Count"
        Expression = "[Duplicate Alerts] + [Row Removal Alerts] + [Null Rate Alerts]"
        Description = "Consolida alertas críticos de duplicidade, remoção e null rate."
        FormatString = "#,##0"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Observability Health Score %"
        Expression = @"
VAR _success = COALESCE ( [Successful Load Rate %], 0 )
VAR _retention = COALESCE ( [Average Row Retention Rate], 0 )
VAR _nullPenalty = 1 - COALESCE ( [Max Null Rate], 0 )
VAR _alertPenalty =
    1 - DIVIDE ( [Critical Alert Count], [Monitored Loads] + [Monitored Columns], 0 )
VAR _score =
    ( _success * 0.35 ) +
    ( _retention * 0.30 ) +
    ( _nullPenalty * 0.20 ) +
    ( _alertPenalty * 0.15 )
RETURN
    MAX ( 0, MIN ( 1, _score ) )
"@
        Description = "Calcula um score sintético de saúde de observability combinando confiabilidade, retenção, null rate e alertas."
        FormatString = "0.0%"
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Quality Checklist Status"
        Expression = @"
VAR _critical = [Critical Alert Count]
VAR _fresh = [Freshness Days]
RETURN
    SWITCH (
        TRUE (),
        _critical > 0, ""Atencao"",
        _fresh > 7, ""Desatualizado"",
        ""Saudavel""
    )
"@
        Description = "Retorna um status textual resumido para o checklist visual de qualidade da página."
        FormatString = ""
        DisplayFolder = "15 | Observability"
    },
    @{
        Name = "Total Followers"
        Expression = "SUM ( fact_company_follows[follow_count] )"
        Description = "Soma a quantidade de relacionamentos de follow registrados no ecossistema profissional monitorado."
        FormatString = "#,##0"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Unique Companies In Network"
        Expression = "MAX ( fact_connections_timeline[unique_companies] )"
        Description = "Retorna a quantidade distinta de empresas representadas na rede no contexto atual."
        FormatString = "#,##0"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Unique Positions In Network"
        Expression = "MAX ( fact_connections_timeline[unique_positions] )"
        Description = "Retorna a quantidade distinta de cargos representados na rede no contexto atual."
        FormatString = "#,##0"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Total Languages"
        Expression = "MAX ( dim_language_proficiency[total_languages] )"
        Description = "Retorna a quantidade total de idiomas registrados no perfil."
        FormatString = "#,##0"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Unique Languages"
        Expression = "MAX ( dim_language_proficiency[unique_languages] )"
        Description = "Retorna a quantidade distinta de idiomas registrados no perfil."
        FormatString = "#,##0"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Primary Geo Location"
        Expression = "SELECTEDVALUE ( dim_profile[geo_location], ""Global"" )"
        Description = "Retorna a localidade principal do perfil no contexto atual."
        FormatString = ""
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Invitations Sent"
        Expression = @"
CALCULATE (
    [Total Invitations],
    fact_invitations_summary[direction] = ""SENT""
)
"@
        Description = "Conta o volume de convites enviados no contexto filtrado."
        FormatString = "#,##0"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Invitations Received"
        Expression = @"
CALCULATE (
    [Total Invitations],
    fact_invitations_summary[direction] = ""RECEIVED""
)
"@
        Description = "Conta o volume de convites recebidos no contexto filtrado."
        FormatString = "#,##0"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Recommendation Density %"
        Expression = "DIVIDE ( [Total Recommendations], [Total Connections] )"
        Description = "Calcula a densidade de recomendações em relação ao total de conexões."
        FormatString = "0.0%"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Networking Reach Score %"
        Expression = @"
VAR _email = COALESCE ( [Connections with Email %], 0 )
VAR _companies = MIN ( 1, DIVIDE ( [Unique Companies In Network], 50, 0 ) )
VAR _positions = MIN ( 1, DIVIDE ( [Unique Positions In Network], 40, 0 ) )
VAR _languages = MIN ( 1, DIVIDE ( [Unique Languages], 5, 0 ) )
RETURN
    MAX (
        0,
        MIN (
            1,
            ( _email * 0.45 ) +
            ( _companies * 0.20 ) +
            ( _positions * 0.20 ) +
            ( _languages * 0.15 )
        )
    )
"@
        Description = "Score sintético de alcance da rede combinando cobertura de contato, diversidade de empresas, cargos e idiomas."
        FormatString = "0.0%"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Engagement Signal %"
        Expression = @"
VAR _recommendationDensity = MIN ( 1, DIVIDE ( [Total Recommendations], [Total Connections], 0 ) * 20 )
VAR _messageRate = COALESCE ( [Invitations with Message %], 0 )
VAR _inviteActivity = MIN ( 1, DIVIDE ( [Total Invitations], 50, 0 ) )
RETURN
    MAX (
        0,
        MIN (
            1,
            ( _recommendationDensity * 0.45 ) +
            ( _messageRate * 0.35 ) +
            ( _inviteActivity * 0.20 )
        )
    )
"@
        Description = "Score sintético de engajamento combinando densidade de recomendações, qualidade de convite e volume de interação."
        FormatString = "0.0%"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Network Growth Score %"
        Expression = @"
VAR _mom = COALESCE ( [Connections MoM %], 0 )
VAR _momNormalized = MAX ( 0, MIN ( 1, DIVIDE ( _mom, 0.10, 0 ) ) )
VAR _inboundInterest = MIN ( 1, DIVIDE ( [Invitations Received], 25, 0 ) )
VAR _reach = COALESCE ( [Networking Reach Score %], 0 )
RETURN
    MAX (
        0,
        MIN (
            1,
            ( _momNormalized * 0.50 ) +
            ( _inboundInterest * 0.20 ) +
            ( _reach * 0.30 )
        )
    )
"@
        Description = "Score sintético de crescimento da rede combinando expansão mensal, atração de convites recebidos e amplitude do alcance."
        FormatString = "0.0%"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Networking Maturity %"
        Expression = @"
VAR _reach = COALESCE ( [Networking Reach Score %], 0 )
VAR _email = COALESCE ( [Connections with Email %], 0 )
VAR _companies = MIN ( 1, DIVIDE ( [Unique Companies In Network], 50, 0 ) )
RETURN
    MAX (
        0,
        MIN (
            1,
            ( _reach * 0.50 ) +
            ( _email * 0.25 ) +
            ( _companies * 0.25 )
        )
    )
"@
        Description = "Score de maturidade da rede com foco em amplitude, qualidade de contato e diversidade organizacional."
        FormatString = "0.0%"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Professional Visibility %"
        Expression = @"
VAR _followers = MIN ( 1, DIVIDE ( [Total Followers], 40, 0 ) )
VAR _recommendations = MIN ( 1, DIVIDE ( [Total Recommendations], 15, 0 ) )
VAR _received = MIN ( 1, DIVIDE ( [Invitations Received], 20, 0 ) )
RETURN
    MAX (
        0,
        MIN (
            1,
            ( _followers * 0.35 ) +
            ( _recommendations * 0.40 ) +
            ( _received * 0.25 )
        )
    )
"@
        Description = "Score de visibilidade profissional combinando follows, recomendações e convites recebidos."
        FormatString = "0.0%"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Relationship Strength %"
        Expression = @"
VAR _recommendationDensity = MIN ( 1, DIVIDE ( [Total Recommendations], [Total Connections], 0 ) * 20 )
VAR _messageRate = COALESCE ( [Invitations with Message %], 0 )
VAR _email = COALESCE ( [Connections with Email %], 0 )
RETURN
    MAX (
        0,
        MIN (
            1,
            ( _recommendationDensity * 0.40 ) +
            ( _messageRate * 0.30 ) +
            ( _email * 0.30 )
        )
    )
"@
        Description = "Score de força relacional combinando recomendação relativa, qualidade de convite e riqueza de contato."
        FormatString = "0.0%"
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Networking Growth Insight"
        Expression = @"
VAR _score = [Network Growth Score %]
RETURN
    SWITCH (
        TRUE (),
        _score >= 0.75, ""Professional network shows healthy expansion."",
        _score >= 0.50, ""Professional network growth remains stable with room to accelerate."",
        ""Network expansion is currently modest and may require stronger outbound momentum.""
    )
"@
        Description = "Insight narrativo sobre a expansão da rede profissional no contexto atual."
        FormatString = ""
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Networking Reputation Insight"
        Expression = @"
VAR _recommendations = [Total Recommendations]
VAR _visibility = [Professional Visibility %]
RETURN
    SWITCH (
        TRUE (),
        _recommendations > 0 && _visibility >= 0.65, ""Recommendation activity indicates positive professional reputation."",
        _recommendations > 0, ""Recommendation signals are present and reinforce professional credibility."",
        ""Recommendation volume is still limited, reducing visible proof of reputation.""
    )
"@
        Description = "Insight narrativo sobre reputação profissional e prova social da rede."
        FormatString = ""
        DisplayFolder = "02 | Networking"
    },
    @{
        Name = "Networking Participation Insight"
        Expression = @"
VAR _invites = [Total Invitations]
VAR _messageRate = [Invitations with Message %]
RETURN
    SWITCH (
        TRUE (),
        _invites > 0 && _messageRate >= 0.50, ""Invitation flow suggests active ecosystem participation."",
        _invites > 0, ""Invitation activity exists, though the quality of outreach can still improve."",
        ""Invitation flow is limited, suggesting low ecosystem interaction in the current context.""
    )
"@
        Description = "Insight narrativo sobre participação no ecossistema por meio de convites e personalização."
        FormatString = ""
        DisplayFolder = "02 | Networking"
    }
)

$htmlMeasureExpression = @'
VAR _score = FORMAT ( [Observability Health Score %], "0.0%" )
VAR _loads = FORMAT ( [Monitored Loads], "#,##0" )
VAR _success = FORMAT ( [Successful Load Rate %], "0.0%" )
VAR _sourceRows = FORMAT ( [Total Source Rows], "#,##0" )
VAR _afterRows = FORMAT ( [Total Rows After Transform], "#,##0" )
VAR _removedRows = FORMAT ( [Total Rows Removed], "#,##0" )
VAR _retention = FORMAT ( [Average Row Retention Rate], "0.0%" )
VAR _avgNull = FORMAT ( [Average Null Rate], "0.0%" )
VAR _maxNull = FORMAT ( [Max Null Rate], "0.0%" )
VAR _nullAlerts = FORMAT ( [Null Rate Alerts], "#,##0" )
VAR _dupAlerts = FORMAT ( [Duplicate Alerts], "#,##0" )
VAR _removalAlerts = FORMAT ( [Row Removal Alerts], "#,##0" )
VAR _issues = FORMAT ( [Health Issue Load Count], "#,##0" )
VAR _critical = FORMAT ( [Critical Alert Count], "#,##0" )
VAR _tables = FORMAT ( [Monitored Tables], "#,##0" )
VAR _columns = FORMAT ( [Monitored Columns], "#,##0" )
VAR _freshness = FORMAT ( [Freshness Days], "#,##0" )
VAR _latest = FORMAT ( [Latest Load Date], "dd/mm/yyyy hh:mm" )
VAR _status = [Quality Checklist Status]
VAR _statusLabel =
    SWITCH (
        _status,
        "Saudavel", "Saud&aacute;vel",
        "Atencao", "Aten&ccedil;&atilde;o",
        "Desatualizado", "Desatualizado",
        _status
    )
VAR _statusColor =
    SWITCH (
        _status,
        "Saudavel", "#10b981",
        "Desatualizado", "#f59e0b",
        "#ef4444"
    )
VAR _statusBg =
    SWITCH (
        _status,
        "Saudavel", "rgba(16,185,129,.16)",
        "Desatualizado", "rgba(245,158,11,.16)",
        "rgba(239,68,68,.16)"
    )
RETURN
"<div style='width:100%;height:100%;min-height:100%;max-height:100%;overflow:hidden;box-sizing:border-box;background:#0b1220;color:#f9fafb;font-family:Segoe UI,Arial,sans-serif;padding:11px;'>
<div style='width:100%;height:100%;min-height:100%;max-height:100%;overflow:hidden;box-sizing:border-box;display:flex;flex-direction:column;background:#0b1220;border-radius:14px;'>
<div style='width:100%;overflow:hidden;box-sizing:border-box;display:flex;flex-direction:column;gap:7px;'>
  <div style='min-height:88px;box-sizing:border-box;border:1px solid #1f2937;border-radius:15px;background:
    linear-gradient(135deg,#0f172a 0%,#111827 58%,#0b1220 100%),
    linear-gradient(90deg,rgba(37,99,235,.12) 0 1px,transparent 1px),
    linear-gradient(180deg,rgba(255,255,255,.04) 0 1px,transparent 1px);
    background-size:auto,36px 36px,36px 36px;
    padding:11px 14px;display:flex;align-items:center;justify-content:space-between;gap:10px;overflow:hidden;position:relative;'>
    <div style='position:absolute;inset:0;background:
      radial-gradient(circle at 10% 16%, rgba(37,99,235,.18), transparent 24%),
      radial-gradient(circle at 76% 8%, rgba(16,185,129,.08), transparent 18%);
      pointer-events:none;'></div>
    <div style='min-width:0;overflow:hidden;flex:1;display:flex;flex-direction:column;justify-content:center;position:relative;z-index:1;'>
      <div style='font-size:9px;line-height:1;letter-spacing:.16em;text-transform:uppercase;color:#94a3b8;margin-bottom:5px;'>Observability Layer</div>
      <div style='font-size:24px;line-height:1.02;font-weight:800;color:#f9fafb;white-space:nowrap;'>Pipeline &amp; Data Quality Observability</div>
      <div style='display:flex;align-items:center;gap:7px;flex-wrap:wrap;margin-top:8px;'>
        <div style='padding:5px 9px;border-radius:999px;border:1px solid #1f2937;background:rgba(37,99,235,.10);color:#bfdbfe;font-size:9px;font-weight:600;'>Data Quality</div>
        <div style='padding:5px 9px;border-radius:999px;border:1px solid #1f2937;background:rgba(16,185,129,.10);color:#a7f3d0;font-size:9px;font-weight:600;'>Monitoring</div>
        <div style='padding:5px 9px;border-radius:999px;border:1px solid #1f2937;background:rgba(245,158,11,.10);color:#fde68a;font-size:9px;font-weight:600;'>Observability</div>
        <div style='padding:5px 9px;border-radius:999px;border:1px solid #1f2937;background:rgba(37,99,235,.08);color:#dbeafe;font-size:9px;font-weight:600;'>Pipeline Health</div>
      </div>
    </div>
    <div style='display:flex;align-items:stretch;gap:7px;min-width:0;overflow:hidden;position:relative;z-index:1;'>
      <div style='box-sizing:border-box;padding:8px 10px;border-radius:11px;border:1px solid " & _statusColor & ";background:" & _statusBg & ";overflow:hidden;min-width:130px;display:flex;flex-direction:column;justify-content:center;'>
        <div style='font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;margin-bottom:4px;'>Health Status</div>
        <div style='font-size:14px;font-weight:800;color:" & _statusColor & ";white-space:nowrap;'>" & _statusLabel & "</div>
      </div>
      <div style='box-sizing:border-box;padding:8px 10px;border-radius:11px;border:1px solid #1f2937;background:#0f172a;min-width:162px;overflow:hidden;display:flex;flex-direction:column;justify-content:center;'>
        <div style='font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;margin-bottom:4px;'>Latest Load</div>
        <div style='font-size:14px;font-weight:700;color:#f9fafb;white-space:nowrap;'>" & _latest & "</div>
      </div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:7px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:69px;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:9px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Health Score</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f9fafb;margin-top:8px;text-shadow:0 0 18px rgba(37,99,235,.10);'>" & _score & "</div>
    </div>
    <div style='min-height:69px;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:9px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Loads Monitorados</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f9fafb;margin-top:8px;'>" & _loads & "</div>
    </div>
    <div style='min-height:69px;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:9px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Taxa de Sucesso</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#10b981;margin-top:8px;text-shadow:0 0 16px rgba(16,185,129,.12);'>" & _success & "</div>
    </div>
    <div style='min-height:69px;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:9px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Freshness</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f9fafb;margin-top:8px;'>" & _freshness & "d</div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:7px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:71px;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:9px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Source Rows</div>
      <div style='font-size:23px;line-height:1;font-weight:800;color:#f9fafb;margin-top:8px;white-space:nowrap;'>" & _sourceRows & "</div>
    </div>
    <div style='min-height:71px;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:9px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>After Transform</div>
      <div style='font-size:23px;line-height:1;font-weight:800;color:#f9fafb;margin-top:8px;white-space:nowrap;'>" & _afterRows & "</div>
    </div>
    <div style='min-height:71px;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:9px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Rows Removed</div>
      <div style='font-size:23px;line-height:1;font-weight:800;color:#f59e0b;margin-top:8px;white-space:nowrap;text-shadow:0 0 16px rgba(245,158,11,.10);'>" & _removedRows & "</div>
    </div>
    <div style='min-height:71px;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:9px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Retention</div>
      <div style='font-size:23px;line-height:1;font-weight:800;color:#2563eb;margin-top:8px;white-space:nowrap;text-shadow:0 0 16px rgba(37,99,235,.10);'>" & _retention & "</div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:7px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:71px;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:9px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Avg Null Rate</div>
      <div style='font-size:23px;line-height:1;font-weight:800;color:#f9fafb;margin-top:8px;white-space:nowrap;'>" & _avgNull & "</div>
    </div>
    <div style='min-height:71px;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:9px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Max Null Rate</div>
      <div style='font-size:23px;line-height:1;font-weight:800;color:#ef4444;margin-top:8px;white-space:nowrap;text-shadow:0 0 16px rgba(239,68,68,.10);'>" & _maxNull & "</div>
    </div>
    <div style='min-height:71px;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:9px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Null Alerts</div>
      <div style='font-size:23px;line-height:1;font-weight:800;color:#ef4444;margin-top:8px;white-space:nowrap;text-shadow:0 0 16px rgba(239,68,68,.10);'>" & _nullAlerts & "</div>
    </div>
    <div style='min-height:71px;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:9px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Columns</div>
      <div style='font-size:23px;line-height:1;font-weight:800;color:#f9fafb;margin-top:8px;white-space:nowrap;'>" & _columns & "</div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr) minmax(0,.92fr);gap:7px;flex:1;min-height:0;box-sizing:border-box;overflow:hidden;'>
    <div style='display:flex;flex-direction:column;gap:7px;min-height:0;box-sizing:border-box;overflow:hidden;'>
      <div style='border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:11px;box-sizing:border-box;overflow-x:hidden;overflow-y:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px;'>
        <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;'>Alertas Operacionais</div>
        <div style='padding:5px 8px;border-radius:999px;border:1px solid rgba(239,68,68,.28);background:rgba(239,68,68,.10);color:#fecaca;font-size:10px;font-weight:700;'>Incident Panel</div>
      </div>
      <div style='display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:7px;'>
        <div style='border:1px solid rgba(239,68,68,.18);border-radius:10px;background:#0f172a;padding:9px;box-sizing:border-box;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);'>
          <div style='display:flex;align-items:center;justify-content:space-between;gap:8px;'>
            <div style='font-size:10px;color:#94a3b8;'>Duplicate Alerts</div>
            <div style='width:8px;height:8px;border-radius:50%;background:#ef4444;box-shadow:0 0 10px rgba(239,68,68,.35);'></div>
          </div>
          <div style='font-size:25px;font-weight:800;color:#ef4444;margin-top:6px;'>" & _dupAlerts & "</div>
        </div>
        <div style='border:1px solid rgba(245,158,11,.18);border-radius:10px;background:#0f172a;padding:9px;box-sizing:border-box;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);'>
          <div style='display:flex;align-items:center;justify-content:space-between;gap:8px;'>
            <div style='font-size:10px;color:#94a3b8;'>Row Removal Alerts</div>
            <div style='width:8px;height:8px;border-radius:50%;background:#f59e0b;box-shadow:0 0 10px rgba(245,158,11,.30);'></div>
          </div>
          <div style='font-size:25px;font-weight:800;color:#f59e0b;margin-top:6px;'>" & _removalAlerts & "</div>
        </div>
        <div style='border:1px solid rgba(37,99,235,.18);border-radius:10px;background:#0f172a;padding:9px;box-sizing:border-box;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);'>
          <div style='display:flex;align-items:center;justify-content:space-between;gap:8px;'>
            <div style='font-size:10px;color:#94a3b8;'>Loads com Problema</div>
            <div style='width:8px;height:8px;border-radius:50%;background:#2563eb;box-shadow:0 0 10px rgba(37,99,235,.30);'></div>
          </div>
          <div style='font-size:25px;font-weight:800;color:#f9fafb;margin-top:6px;'>" & _issues & "</div>
        </div>
        <div style='border:1px solid rgba(239,68,68,.18);border-radius:10px;background:#0f172a;padding:9px;box-sizing:border-box;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);'>
          <div style='display:flex;align-items:center;justify-content:space-between;gap:8px;'>
            <div style='font-size:10px;color:#94a3b8;'>Alertas Cr&iacute;ticos</div>
            <div style='padding:3px 6px;border-radius:999px;background:rgba(239,68,68,.12);color:#fecaca;font-size:9px;font-weight:700;'>Critical</div>
          </div>
          <div style='font-size:25px;font-weight:800;color:#ef4444;margin-top:6px;'>" & _critical & "</div>
        </div>
      </div>
      </div>
      <div style='border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.92));padding:10px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
        <div style='display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px;'>
          <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;'>Data Quality Dimensions</div>
          <div style='padding:4px 8px;border-radius:999px;border:1px solid rgba(37,99,235,.24);background:rgba(37,99,235,.08);color:#dbeafe;font-size:10px;font-weight:700;'>Healthy</div>
        </div>
        <div style='display:flex;flex-direction:column;gap:7px;'>
          <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:7px 9px;box-sizing:border-box;'>
            <span style='font-size:11px;color:#94a3b8;'>Completeness</span>
            <span style='font-size:11px;font-weight:700;color:#10b981;'>Healthy</span>
          </div>
          <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:7px 9px;box-sizing:border-box;'>
            <span style='font-size:11px;color:#94a3b8;'>Freshness</span>
            <span style='font-size:11px;font-weight:700;color:#10b981;'>Healthy</span>
          </div>
          <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:7px 9px;box-sizing:border-box;'>
            <span style='font-size:11px;color:#94a3b8;'>Consistency</span>
            <span style='font-size:11px;font-weight:700;color:#2563eb;'>Stable</span>
          </div>
          <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:7px 9px;box-sizing:border-box;'>
            <span style='font-size:11px;color:#94a3b8;'>Accuracy</span>
            <span style='font-size:11px;font-weight:700;color:#2563eb;'>Stable</span>
          </div>
        </div>
      </div>
    </div>

    <div style='display:flex;flex-direction:column;gap:7px;min-height:0;box-sizing:border-box;overflow:hidden;'>
      <div style='border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:11px;box-sizing:border-box;overflow-x:hidden;overflow-y:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
        <div style='display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px;'>
          <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;'>Cobertura Monitorada</div>
          <div style='padding:5px 8px;border-radius:999px;border:1px solid rgba(37,99,235,.24);background:rgba(37,99,235,.08);color:#dbeafe;font-size:10px;font-weight:700;'>Monitoring Scope</div>
        </div>
        <div style='display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:7px;'>
          <div style='border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px;box-sizing:border-box;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);'>
            <div style='font-size:10px;color:#94a3b8;'>Tabelas</div>
            <div style='font-size:25px;font-weight:800;color:#f9fafb;margin-top:6px;'>" & _tables & "</div>
          </div>
          <div style='border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px;box-sizing:border-box;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);'>
            <div style='font-size:10px;color:#94a3b8;'>Colunas</div>
            <div style='font-size:25px;font-weight:800;color:#f9fafb;margin-top:6px;'>" & _columns & "</div>
          </div>
          <div style='grid-column:1 / span 2;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);'>
            <div style='font-size:10px;color:#94a3b8;'>Atualizado em</div>
            <div style='font-size:14px;font-weight:700;color:#f9fafb;margin-top:6px;white-space:nowrap;'>" & _latest & "</div>
          </div>
        </div>
      </div>

      <div style='flex:1;border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.92));padding:10px 11px;box-sizing:border-box;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);display:flex;flex-direction:column;'>
        <div style='display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px;'>
          <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;'>Executive Operational Insights</div>
          <div style='padding:4px 8px;border-radius:999px;border:1px solid rgba(16,185,129,.24);background:rgba(16,185,129,.08);color:#a7f3d0;font-size:10px;font-weight:700;'>Active</div>
        </div>
        <div style='display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:7px;flex:1;align-content:stretch;'>
          <div style='border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:8px 9px;box-sizing:border-box;display:flex;align-items:flex-start;gap:8px;'>
            <div style='width:8px;height:8px;border-radius:50%;background:#10b981;box-shadow:0 0 10px rgba(16,185,129,.35);margin-top:4px;flex:0 0 auto;'></div>
            <div style='font-size:11px;line-height:1.35;color:#f9fafb;'>Pipeline operating within SLA</div>
          </div>
          <div style='border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:8px 9px;box-sizing:border-box;display:flex;align-items:flex-start;gap:8px;'>
            <div style='width:8px;height:8px;border-radius:50%;background:#10b981;box-shadow:0 0 10px rgba(16,185,129,.35);margin-top:4px;flex:0 0 auto;'></div>
            <div style='font-size:11px;line-height:1.35;color:#f9fafb;'>No critical incidents detected</div>
          </div>
          <div style='border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:8px 9px;box-sizing:border-box;display:flex;align-items:flex-start;gap:8px;'>
            <div style='width:8px;height:8px;border-radius:50%;background:#2563eb;box-shadow:0 0 10px rgba(37,99,235,.30);margin-top:4px;flex:0 0 auto;'></div>
            <div style='font-size:11px;line-height:1.35;color:#f9fafb;'>Data quality checks stable</div>
          </div>
          <div style='border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:8px 9px;box-sizing:border-box;display:flex;align-items:flex-start;gap:8px;'>
            <div style='width:8px;height:8px;border-radius:50%;background:#10b981;box-shadow:0 0 10px rgba(16,185,129,.35);margin-top:4px;flex:0 0 auto;'></div>
            <div style='font-size:11px;line-height:1.35;color:#f9fafb;'>Monitoring active and healthy</div>
          </div>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:linear-gradient(180deg,rgba(17,24,39,.98),rgba(17,24,39,.90));padding:11px;box-sizing:border-box;overflow-x:hidden;overflow-y:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);'>
      <div style='display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px;'>
        <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;'>Checklist Visual</div>
        <div style='padding:5px 8px;border-radius:999px;border:1px solid rgba(148,163,184,.18);background:rgba(148,163,184,.07);color:#e2e8f0;font-size:10px;font-weight:700;'>Readiness</div>
      </div>
      <div style='display:flex;flex-direction:column;gap:7px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:7px 9px;box-sizing:border-box;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);'>
          <span style='font-size:11px;color:#94a3b8;'>Pipeline status</span>
          <span style='font-size:11px;font-weight:700;color:" & _statusColor & ";white-space:nowrap;'>" & _statusLabel & "</span>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:7px 9px;box-sizing:border-box;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);'>
          <span style='font-size:11px;color:#94a3b8;'>Freshness</span>
          <span style='font-size:11px;font-weight:700;color:#f9fafb;white-space:nowrap;'>" & _freshness & " dias</span>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:7px 9px;box-sizing:border-box;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);'>
          <span style='font-size:11px;color:#94a3b8;'>Null alerts</span>
          <span style='font-size:11px;font-weight:700;color:#ef4444;'>" & _nullAlerts & "</span>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:7px 9px;box-sizing:border-box;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);'>
          <span style='font-size:11px;color:#94a3b8;'>Duplicate alerts</span>
          <span style='font-size:11px;font-weight:700;color:#ef4444;'>" & _dupAlerts & "</span>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:7px 9px;box-sizing:border-box;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);'>
          <span style='font-size:11px;color:#94a3b8;'>Retention rate</span>
          <span style='font-size:11px;font-weight:700;color:#2563eb;'>" & _retention & "</span>
        </div>
      </div>
    </div>
  </div>
</div>
</div>
</div>"
'@

$overviewHtmlMeasureExpression = @'
VAR _totalConnections = FORMAT ( [Total Connections], "#,##0" )
VAR _connectionsMoM = FORMAT ( [Connections MoM %], "0.0%" )
VAR _totalApplications = FORMAT ( [Total Applications], "#,##0" )
VAR _totalRecommendations = FORMAT ( [Total Recommendations], "#,##0" )
VAR _readSuccess = FORMAT ( [Read Success Rate %], "0.0%" )
VAR _inventoryFreshness = FORMAT ( [Inventory Freshness (Days)], "#,##0" )
VAR _lastRefresh = FORMAT ( [Last Refresh Date], "dd/mm/yyyy" )
VAR _emailCoverage = FORMAT ( [Connections with Email %], "0.0%" )
VAR _positions = FORMAT ( [Total Positions Started], "#,##0" )
VAR _education = FORMAT ( [Total Education Started], "#,##0" )
VAR _certifications = FORMAT ( [Total Certifications], "#,##0" )
VAR _events = FORMAT ( [Total Events], "#,##0" )
VAR _invitations = FORMAT ( [Total Invitations], "#,##0" )
VAR _qualityScore = FORMAT ( [Data Quality Score %], "0.0%" )
RETURN
"<div style='width:100%;height:100%;max-height:100%;overflow-x:hidden;overflow-y:hidden;box-sizing:border-box;background:#0b1220;color:#f9fafb;font-family:Segoe UI,Arial,sans-serif;padding:11px;'>
<div style='width:100%;height:100%;max-height:100%;overflow-x:hidden;overflow-y:hidden;box-sizing:border-box;display:flex;flex-direction:column;gap:9px;'>
  <div style='min-height:110px;box-sizing:border-box;border:1px solid #1f2937;border-radius:16px;background:
    linear-gradient(135deg,#0f172a 0%,#111827 58%,#0b1220 100%),
    linear-gradient(90deg,rgba(37,99,235,.12) 0 1px,transparent 1px),
    linear-gradient(180deg,rgba(255,255,255,.04) 0 1px,transparent 1px);
    background-size:auto,36px 36px,36px 36px;
    padding:13px 17px;display:flex;align-items:stretch;justify-content:space-between;gap:14px;overflow:hidden;position:relative;'>
    <div style='position:absolute;inset:0;background:
      radial-gradient(circle at 12% 18%, rgba(37,99,235,.16), transparent 24%),
      radial-gradient(circle at 70% 10%, rgba(16,185,129,.08), transparent 18%);
      pointer-events:none;'></div>
    <div style='display:flex;align-items:center;gap:14px;min-width:0;overflow:hidden;flex:1;position:relative;z-index:1;'>
      <div style='width:82px;height:82px;border-radius:50%;padding:3px;background:linear-gradient(135deg,#2563eb,#60a5fa);box-sizing:border-box;flex:0 0 auto;box-shadow:0 10px 28px rgba(37,99,235,.22),0 0 0 1px rgba(37,99,235,.28);'>
        <img src='https://diego-pablo.vercel.app/assets/img/profile.png' alt='Profile' style='width:100%;height:100%;border-radius:50%;object-fit:cover;display:block;background:#111827;' />
      </div>
      <div style='min-width:0;overflow:hidden;flex:1;display:grid;grid-template-columns:minmax(0,1.35fr) minmax(300px,.95fr);gap:14px;align-items:center;'>
        <div style='min-width:0;overflow:hidden;'>
          <div style='font-size:10px;line-height:1;letter-spacing:.16em;text-transform:uppercase;color:#94a3b8;margin-bottom:7px;'>Executive Overview</div>
          <div style='font-size:28px;line-height:1.02;font-weight:800;color:#f9fafb;white-space:nowrap;'>LinkedIn Career Intelligence</div>
          <div style='font-size:12px;line-height:1.34;color:#94a3b8;margin-top:6px;max-width:620px;'>An&aacute;lise executiva da trajet&oacute;ria, networking, aplica&ccedil;&otilde;es e presen&ccedil;a profissional.</div>
          <div style='display:flex;align-items:center;gap:7px;margin-top:9px;flex-wrap:wrap;'>
            <div style='padding:6px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(37,99,235,.10);color:#bfdbfe;font-size:11px;font-weight:600;'>Networking intelligence</div>
            <div style='padding:6px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(16,185,129,.10);color:#a7f3d0;font-size:11px;font-weight:600;'>Career analytics</div>
            <div style='padding:6px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(245,158,11,.10);color:#fde68a;font-size:11px;font-weight:600;'>Pipeline monitoring</div>
          </div>
        </div>
        <div style='min-width:0;overflow:hidden;border-left:1px solid rgba(148,163,184,.14);padding-left:14px;display:flex;flex-direction:column;gap:7px;'>
          <div style='display:grid;grid-template-columns:92px 1fr;gap:10px;align-items:start;'>
            <div style='font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:#64748b;'>Role</div>
            <div style='font-size:12px;line-height:1.35;color:#f9fafb;font-weight:600;'>Data Analyst | BI &amp; Analytics Engineer</div>
          </div>
          <div style='display:grid;grid-template-columns:92px 1fr;gap:10px;align-items:start;'>
            <div style='font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:#64748b;'>Stack</div>
            <div style='display:flex;flex-wrap:wrap;gap:6px;'>
              <span style='padding:5px 8px;border-radius:999px;border:1px solid #1f2937;background:#0f172a;color:#dbeafe;font-size:10px;font-weight:600;'>Power BI</span>
              <span style='padding:5px 8px;border-radius:999px;border:1px solid #1f2937;background:#0f172a;color:#dbeafe;font-size:10px;font-weight:600;'>SQL</span>
              <span style='padding:5px 8px;border-radius:999px;border:1px solid #1f2937;background:#0f172a;color:#dbeafe;font-size:10px;font-weight:600;'>Python</span>
              <span style='padding:5px 8px;border-radius:999px;border:1px solid #1f2937;background:#0f172a;color:#dbeafe;font-size:10px;font-weight:600;'>Databricks</span>
              <span style='padding:5px 8px;border-radius:999px;border:1px solid #1f2937;background:#0f172a;color:#dbeafe;font-size:10px;font-weight:600;'>Azure</span>
              <span style='padding:5px 8px;border-radius:999px;border:1px solid #1f2937;background:#0f172a;color:#dbeafe;font-size:10px;font-weight:600;'>ETL</span>
            </div>
          </div>
          <div style='display:grid;grid-template-columns:92px 1fr;gap:10px;align-items:start;'>
            <div style='font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:#64748b;'>Specialties</div>
            <div style='display:flex;flex-wrap:wrap;gap:6px;'>
              <span style='padding:5px 8px;border-radius:999px;border:1px solid #1f2937;background:rgba(37,99,235,.08);color:#e2e8f0;font-size:10px;font-weight:600;'>Analytics Engineering</span>
              <span style='padding:5px 8px;border-radius:999px;border:1px solid #1f2937;background:rgba(37,99,235,.08);color:#e2e8f0;font-size:10px;font-weight:600;'>Executive Dashboards</span>
              <span style='padding:5px 8px;border-radius:999px;border:1px solid #1f2937;background:rgba(37,99,235,.08);color:#e2e8f0;font-size:10px;font-weight:600;'>Data Quality</span>
              <span style='padding:5px 8px;border-radius:999px;border:1px solid #1f2937;background:rgba(37,99,235,.08);color:#e2e8f0;font-size:10px;font-weight:600;'>Observability</span>
              <span style='padding:5px 8px;border-radius:999px;border:1px solid #1f2937;background:rgba(37,99,235,.08);color:#e2e8f0;font-size:10px;font-weight:600;'>Automation</span>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div style='display:flex;flex-direction:column;justify-content:space-between;gap:9px;min-width:220px;overflow:hidden;position:relative;z-index:1;'>
      <div style='box-sizing:border-box;padding:10px 12px;border-radius:12px;border:1px solid #1f2937;background:#0f172a;overflow:hidden;'>
        <div style='font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;margin-bottom:4px;'>Last Refresh</div>
        <div style='font-size:16px;font-weight:700;color:#f9fafb;white-space:nowrap;'>" & _lastRefresh & "</div>
        <div style='font-size:11px;color:#94a3b8;margin-top:6px;'>Base executiva atualizada</div>
      </div>
      <div style='box-sizing:border-box;padding:10px 12px;border-radius:12px;border:1px solid #1f2937;background:#0f172a;overflow:hidden;'>
        <div style='font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;margin-bottom:4px;'>Pipeline Readiness</div>
        <div style='font-size:18px;font-weight:800;color:#10b981;white-space:nowrap;'>" & _readSuccess & "</div>
        <div style='font-size:11px;color:#94a3b8;margin-top:6px;'>Sa&uacute;de operacional consolidada</div>
      </div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Total Connections</div>
      <div style='font-size:29px;line-height:1;font-weight:800;color:#f9fafb;margin-top:11px;'>" & _totalConnections & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Base consolidada de networking</div>
    </div>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Connections MoM</div>
      <div style='font-size:29px;line-height:1;font-weight:800;color:#2563eb;margin-top:11px;'>" & _connectionsMoM & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Varia&ccedil;&atilde;o mensal da rede</div>
    </div>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Total Applications</div>
      <div style='font-size:29px;line-height:1;font-weight:800;color:#f9fafb;margin-top:11px;'>" & _totalApplications & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Candidaturas registradas</div>
    </div>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Total Recommendations</div>
      <div style='font-size:29px;line-height:1;font-weight:800;color:#f9fafb;margin-top:11px;'>" & _totalRecommendations & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Sinais p&uacute;blicos de reputa&ccedil;&atilde;o</div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(3,minmax(0,1fr)) minmax(0,1.08fr);gap:10px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Read Success Rate</div>
      <div style='font-size:29px;line-height:1;font-weight:800;color:#10b981;margin-top:11px;'>" & _readSuccess & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Confiabilidade operacional</div>
    </div>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Inventory Freshness</div>
      <div style='font-size:29px;line-height:1;font-weight:800;color:#f59e0b;margin-top:11px;'>" & _inventoryFreshness & "d</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Dias desde a &uacute;ltima atualiza&ccedil;&atilde;o</div>
    </div>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Last Refresh Date</div>
      <div style='font-size:29px;line-height:1;font-weight:800;color:#f9fafb;margin-top:11px;white-space:nowrap;'>" & _lastRefresh & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Refer&ecirc;ncia operacional do modelo</div>
    </div>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Executive Signal</div>
      <div style='display:flex;align-items:end;justify-content:space-between;gap:8px;margin-top:11px;'>
        <div style='font-size:29px;line-height:1;font-weight:800;color:#2563eb;'>" & _qualityScore & "</div>
        <div style='font-size:11px;color:#94a3b8;text-align:right;'>Data Quality Score<br/>pipeline readiness</div>
      </div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;flex:1;min-height:0;box-sizing:border-box;overflow:hidden;'>
    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow-x:hidden;overflow-y:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Networking</div>
      <div style='font-size:26px;font-weight:800;color:#f9fafb;line-height:1;'>" & _totalConnections & "</div>
      <div style='font-size:11px;color:#2563eb;margin-top:8px;'>MoM " & _connectionsMoM & "</div>
      <div style='margin-top:12px;display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
        <span style='font-size:11px;color:#94a3b8;'>Email coverage</span>
        <strong style='font-size:12px;color:#f9fafb;'>" & _emailCoverage & "</strong>
      </div>
      <div style='margin-top:11px;font-size:11px;line-height:1.34;color:#94a3b8;'>Vis&atilde;o resumida da escala e cobertura da rede profissional.</div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow-x:hidden;overflow-y:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Applications</div>
      <div style='font-size:26px;font-weight:800;color:#f9fafb;line-height:1;'>" & _totalApplications & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Pipeline de oportunidades acompanhadas</div>
      <div style='margin-top:12px;display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
        <span style='font-size:11px;color:#94a3b8;'>Recommendations</span>
        <strong style='font-size:12px;color:#f9fafb;'>" & _totalRecommendations & "</strong>
      </div>
      <div style='margin-top:11px;font-size:11px;line-height:1.34;color:#94a3b8;'>Leitura executiva do movimento de candidatura e tracionamento profissional.</div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow-x:hidden;overflow-y:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Career &amp; Education</div>
      <div style='display:grid;grid-template-columns:1fr;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Positions</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _positions & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Education</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _education & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Certifications</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _certifications & "</strong>
        </div>
      </div>
      <div style='margin-top:11px;font-size:11px;line-height:1.34;color:#94a3b8;'>Resumo da progress&atilde;o profissional e da forma&ccedil;&atilde;o registrada no perfil.</div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow-x:hidden;overflow-y:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Professional Presence</div>
      <div style='display:grid;grid-template-columns:1fr;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Events</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _events & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Invitations</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _invitations & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Recommendations</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _totalRecommendations & "</strong>
        </div>
      </div>
      <div style='margin-top:11px;font-size:11px;line-height:1.34;color:#94a3b8;'>Indicadores de exposi&ccedil;&atilde;o, convites e presen&ccedil;a p&uacute;blica no ecossistema.</div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow-x:hidden;overflow-y:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Pipeline Health</div>
      <div style='font-size:26px;font-weight:800;color:#10b981;line-height:1;'>" & _readSuccess & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Read success rate consolidada</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Quality score</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _qualityScore & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Freshness</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _inventoryFreshness & " dias</strong>
        </div>
      </div>
      <div style='margin-top:11px;font-size:11px;line-height:1.34;color:#94a3b8;'>Sa&uacute;de operacional do pipeline que sustenta a leitura executiva do dashboard.</div>
    </div>
  </div>
</div>
</div>"
'@

$networkingHtmlMeasureExpression = @'
VAR _totalConnections = FORMAT ( [Total Connections], "#,##0" )
VAR _growth = FORMAT ( [Connections MoM %], "0.0%" )
VAR _connectionsYTD = FORMAT ( [Connections YTD], "#,##0" )
VAR _connectionsWithEmail = FORMAT ( [Connections with Email], "#,##0" )
VAR _recommendations = FORMAT ( [Total Recommendations], "#,##0" )
VAR _refresh = FORMAT ( [Last Refresh Date], "dd/mm/yyyy" )
VAR _emailCoverage = FORMAT ( [Connections with Email %], "0.0%" )
VAR _growthScore = FORMAT ( [Network Growth Score %], "0.0%" )
VAR _reach = FORMAT ( [Networking Reach Score %], "0.0%" )
VAR _engagement = FORMAT ( [Engagement Signal %], "0.0%" )
VAR _maturity = FORMAT ( [Networking Maturity %], "0.0%" )
VAR _relationship = FORMAT ( [Relationship Strength %], "0.0%" )
VAR _insight1 = [Networking Growth Insight]
VAR _insight2 = [Networking Reputation Insight]
VAR _insight3 = [Networking Participation Insight]
RETURN
"<div style='width:100%;height:100%;max-height:100%;overflow:hidden;box-sizing:border-box;background:#0b1220;color:#f9fafb;font-family:Segoe UI,Arial,sans-serif;padding:11px;'>
<div style='width:100%;height:100%;max-height:100%;overflow:hidden;box-sizing:border-box;display:flex;flex-direction:column;gap:8px;'>
  <div style='min-height:112px;box-sizing:border-box;border:1px solid #1f2937;border-radius:16px;background:
    linear-gradient(135deg,#0f172a 0%,#111827 58%,#0b1220 100%),
    linear-gradient(90deg,rgba(37,99,235,.12) 0 1px,transparent 1px),
    linear-gradient(180deg,rgba(255,255,255,.04) 0 1px,transparent 1px);
    background-size:auto,36px 36px,36px 36px;
    padding:14px 17px;display:flex;align-items:stretch;justify-content:space-between;gap:14px;overflow:hidden;position:relative;'>
    <div style='position:absolute;inset:0;background:
      radial-gradient(circle at 10% 18%, rgba(37,99,235,.16), transparent 24%),
      radial-gradient(circle at 74% 10%, rgba(16,185,129,.08), transparent 18%);
      pointer-events:none;'></div>
    <div style='display:flex;align-items:center;gap:14px;min-width:0;overflow:hidden;flex:1;position:relative;z-index:1;'>
      <div style='width:82px;height:82px;border-radius:50%;padding:3px;background:linear-gradient(135deg,#2563eb,#60a5fa);box-sizing:border-box;flex:0 0 auto;box-shadow:0 0 0 1px rgba(37,99,235,.25);'>
        <img src='https://diego-pablo.vercel.app/assets/img/profile.png' alt='Profile' style='width:100%;height:100%;border-radius:50%;object-fit:cover;display:block;background:#111827;' />
      </div>
      <div style='min-width:0;overflow:hidden;flex:1;'>
        <div style='font-size:10px;line-height:1;letter-spacing:.16em;text-transform:uppercase;color:#94a3b8;margin-bottom:7px;'>Networking Intelligence</div>
        <div style='font-size:28px;line-height:1.02;font-weight:800;color:#f9fafb;white-space:nowrap;'>Networking Intelligence</div>
        <div style='font-size:12px;line-height:1.35;color:#94a3b8;margin-top:7px;max-width:700px;'>Executive view of professional connections, reach and relationship quality.</div>
        <div style='display:flex;align-items:center;gap:8px;margin-top:10px;flex-wrap:wrap;'>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(37,99,235,.10);color:#bfdbfe;font-size:11px;font-weight:600;'>Connections</div>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(16,185,129,.10);color:#a7f3d0;font-size:11px;font-weight:600;'>Email Coverage</div>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(245,158,11,.10);color:#fde68a;font-size:11px;font-weight:600;'>Growth</div>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(239,68,68,.10);color:#fecaca;font-size:11px;font-weight:600;'>Relationship Signals</div>
        </div>
      </div>
    </div>
    <div style='display:flex;flex-direction:column;justify-content:space-between;gap:9px;min-width:220px;overflow:hidden;position:relative;z-index:1;'>
      <div style='box-sizing:border-box;padding:10px 12px;border-radius:12px;border:1px solid #1f2937;background:#0f172a;overflow:hidden;'>
        <div style='font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;margin-bottom:4px;'>Last Refresh Date</div>
        <div style='font-size:16px;font-weight:700;color:#f9fafb;white-space:nowrap;'>" & _refresh & "</div>
        <div style='font-size:11px;color:#94a3b8;margin-top:6px;'>Reference snapshot of the network base</div>
      </div>
      <div style='box-sizing:border-box;padding:10px 12px;border-radius:12px;border:1px solid #1f2937;background:#0f172a;overflow:hidden;'>
        <div style='font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;margin-bottom:4px;'>Networking Score</div>
        <div style='font-size:18px;font-weight:800;color:#10b981;white-space:nowrap;'>" & _growthScore & "</div>
        <div style='font-size:11px;color:#94a3b8;margin-top:6px;'>Expansion signal and relationship pull</div>
      </div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Total Connections</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f9fafb;margin-top:10px;'>" & _totalConnections & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Consolidated relationship base</div>
    </div>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Connections MoM</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#2563eb;margin-top:10px;'>" & _growth & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Monthly growth signal</div>
    </div>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Connections YTD</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#10b981;margin-top:10px;'>" & _connectionsYTD & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Year-to-date expansion</div>
    </div>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Connections With Email</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f9fafb;margin-top:10px;'>" & _connectionsWithEmail & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Traceable contact layer</div>
    </div>
    <div style='min-height:84px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Email Coverage</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f59e0b;margin-top:10px;'>" & _emailCoverage & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Relationship traceability rate</div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;flex:1;min-height:0;box-sizing:border-box;overflow:hidden;'>
    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow-x:hidden;overflow-y:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Network Growth</div>
      <div style='font-size:26px;font-weight:800;color:#2563eb;line-height:1;'>" & _growthScore & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Expansion pace and continuity</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Connections MoM</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _growth & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Connections YTD</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _connectionsYTD & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Network score</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _growthScore & "</strong>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow-x:hidden;overflow-y:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Email Coverage</div>
      <div style='font-size:26px;font-weight:800;color:#10b981;line-height:1;'>" & _emailCoverage & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Reachability and traceability of contacts</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Connections with email</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _connectionsWithEmail & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Coverage rate</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _emailCoverage & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Base size</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _totalConnections & "</strong>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow-x:hidden;overflow-y:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Relationship Quality</div>
      <div style='font-size:26px;font-weight:800;color:#f59e0b;line-height:1;'>" & _relationship & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Quality signal of professional links</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Recommendations</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _recommendations & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Engagement signal</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _engagement & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Relationship strength</span>
          <strong style='font-size:12px;color:#f9fafb;white-space:nowrap;'>" & _relationship & "</strong>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow-x:hidden;overflow-y:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Executive Signal</div>
      <div style='font-size:26px;font-weight:800;color:#2563eb;line-height:1;'>" & _reach & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Consolidated executive interpretation layer</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Reach score</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _reach & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Networking maturity</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _maturity & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Relationship layer</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _relationship & "</strong>
        </div>
      </div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:94px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Networking Insights</div>
      <div style='font-size:13px;line-height:1.45;color:#f9fafb;margin-top:10px;'>" & _insight1 & "</div>
    </div>
    <div style='min-height:94px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Data Interpretation</div>
      <div style='font-size:13px;line-height:1.45;color:#f9fafb;margin-top:10px;'>" & _insight2 & "</div>
    </div>
    <div style='min-height:94px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Suggested Narrative</div>
      <div style='font-size:13px;line-height:1.45;color:#f9fafb;margin-top:10px;'>" & _insight3 & "</div>
    </div>
  </div>
</div>
</div>"
'@

$careerEducationHtmlMeasureExpression = @'
VAR _totalPositionsValue = [Total Positions Started]
VAR _currentPositionsValue = [Current Positions Started]
VAR _avgPositionDurationValue = [Average Position Duration (Months)]
VAR _totalEducationValue = [Total Education Started]
VAR _currentEducationValue = [Current Education Started]
VAR _avgEducationDurationValue = [Average Education Duration (Months)]
VAR _totalCertificationsValue = [Total Certifications]
VAR _avgCertificationDurationValue = [Average Certification Duration (Months)]
VAR _refresh = FORMAT ( [Last Refresh Date], "dd/mm/yyyy" )
VAR _totalPositions = FORMAT ( _totalPositionsValue, "#,##0" )
VAR _currentPositions = FORMAT ( _currentPositionsValue, "#,##0" )
VAR _avgPositionDuration = FORMAT ( _avgPositionDurationValue, "0.0" )
VAR _totalEducation = FORMAT ( _totalEducationValue, "#,##0" )
VAR _currentEducation = FORMAT ( _currentEducationValue, "#,##0" )
VAR _avgEducationDuration = FORMAT ( _avgEducationDurationValue, "0.0" )
VAR _totalCertifications = FORMAT ( _totalCertificationsValue, "#,##0" )
VAR _avgCertificationDuration = FORMAT ( _avgCertificationDurationValue, "0.0" )
VAR _careerMaturityScoreValue =
    MAX (
        0,
        MIN (
            1,
            ( MIN ( 1, DIVIDE ( _totalPositionsValue, 8, 0 ) ) * 0.35 ) +
            ( MIN ( 1, DIVIDE ( _totalEducationValue, 4, 0 ) ) * 0.20 ) +
            ( MIN ( 1, DIVIDE ( _totalCertificationsValue, 10, 0 ) ) * 0.20 ) +
            ( MIN ( 1, DIVIDE ( _avgPositionDurationValue, 24, 0 ) ) * 0.15 ) +
            ( MIN ( 1, DIVIDE ( _avgEducationDurationValue, 36, 0 ) ) * 0.10 )
        )
    )
VAR _careerMaturityScore = FORMAT ( _careerMaturityScoreValue, "0.0%" )
VAR _careerInsight =
    SWITCH (
        TRUE (),
        _totalPositionsValue >= 5 && _avgPositionDurationValue >= 18, "Professional trajectory shows continuous evolution.",
        _totalPositionsValue > 0, "Professional trajectory is established and continues to mature over time.",
        "Professional trajectory is still building a broader historical base."
    )
VAR _learningInsight =
    SWITCH (
        TRUE (),
        _totalEducationValue > 0 && _totalCertificationsValue > 0, "Education and certifications reinforce analytical maturity.",
        _totalEducationValue > 0, "Education layer provides a structured foundation for professional growth.",
        "Learning signals are still limited in the current filtered context."
    )
VAR _narrativeInsight =
    SWITCH (
        TRUE (),
        _totalCertificationsValue >= 3, "Certification layer supports technical credibility.",
        _currentPositionsValue > 0 && _totalPositionsValue >= 3, "Career progression indicates transition into data and analytics.",
        "Career narrative suggests an evolving journey with room for stronger technical signaling."
    )
RETURN
"<div style='width:100%;height:100%;max-height:100%;overflow:hidden;box-sizing:border-box;background:#0b1220;color:#f9fafb;font-family:Segoe UI,Arial,sans-serif;padding:11px;'>
<div style='width:100%;height:100%;max-height:100%;overflow:hidden;box-sizing:border-box;display:flex;flex-direction:column;gap:8px;'>
  <div style='min-height:112px;box-sizing:border-box;border:1px solid #1f2937;border-radius:16px;background:
    linear-gradient(135deg,#0f172a 0%,#111827 58%,#0b1220 100%),
    linear-gradient(90deg,rgba(37,99,235,.12) 0 1px,transparent 1px),
    linear-gradient(180deg,rgba(255,255,255,.04) 0 1px,transparent 1px);
    background-size:auto,36px 36px,36px 36px;
    padding:14px 17px;display:flex;align-items:stretch;justify-content:space-between;gap:14px;overflow:hidden;position:relative;'>
    <div style='position:absolute;inset:0;background:
      radial-gradient(circle at 10% 18%, rgba(37,99,235,.16), transparent 24%),
      radial-gradient(circle at 74% 10%, rgba(16,185,129,.08), transparent 18%);
      pointer-events:none;'></div>
    <div style='display:flex;align-items:center;gap:14px;min-width:0;overflow:hidden;flex:1;position:relative;z-index:1;'>
      <div style='width:82px;height:82px;border-radius:50%;padding:3px;background:linear-gradient(135deg,#2563eb,#60a5fa);box-sizing:border-box;flex:0 0 auto;box-shadow:0 0 0 1px rgba(37,99,235,.25);'>
        <img src='https://diego-pablo.vercel.app/assets/img/profile.png' alt='Profile' style='width:100%;height:100%;border-radius:50%;object-fit:cover;display:block;background:#111827;' />
      </div>
      <div style='min-width:0;overflow:hidden;flex:1;'>
        <div style='font-size:10px;line-height:1;letter-spacing:.16em;text-transform:uppercase;color:#94a3b8;margin-bottom:7px;'>Career &amp; Education Intelligence</div>
        <div style='font-size:28px;line-height:1.02;font-weight:800;color:#f9fafb;white-space:nowrap;'>Career &amp; Education Intelligence</div>
        <div style='font-size:12px;line-height:1.35;color:#94a3b8;margin-top:7px;max-width:760px;'>Executive view of professional evolution, education, certifications and career maturity.</div>
        <div style='display:flex;align-items:center;gap:8px;margin-top:10px;flex-wrap:wrap;'>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(37,99,235,.10);color:#bfdbfe;font-size:11px;font-weight:600;'>Career</div>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(16,185,129,.10);color:#a7f3d0;font-size:11px;font-weight:600;'>Education</div>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(245,158,11,.10);color:#fde68a;font-size:11px;font-weight:600;'>Certifications</div>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(239,68,68,.10);color:#fecaca;font-size:11px;font-weight:600;'>Growth</div>
        </div>
      </div>
    </div>
    <div style='display:flex;flex-direction:column;justify-content:space-between;gap:9px;min-width:220px;overflow:hidden;position:relative;z-index:1;'>
      <div style='box-sizing:border-box;padding:10px 12px;border-radius:12px;border:1px solid #1f2937;background:#0f172a;overflow:hidden;'>
        <div style='font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;margin-bottom:4px;'>Last Refresh Date</div>
        <div style='font-size:16px;font-weight:700;color:#f9fafb;white-space:nowrap;'>" & _refresh & "</div>
        <div style='font-size:11px;color:#94a3b8;margin-top:6px;'>Reference view of the professional record</div>
      </div>
      <div style='box-sizing:border-box;padding:10px 12px;border-radius:12px;border:1px solid #1f2937;background:#0f172a;overflow:hidden;'>
        <div style='font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;margin-bottom:4px;'>Career Maturity Score</div>
        <div style='font-size:18px;font-weight:800;color:#10b981;white-space:nowrap;'>" & _careerMaturityScore & "</div>
        <div style='font-size:11px;color:#94a3b8;margin-top:6px;'>Composite view of trajectory depth and learning layer</div>
      </div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Total Positions</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f9fafb;margin-top:10px;'>" & _totalPositions & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Career milestones started</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Current Positions</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#2563eb;margin-top:10px;'>" & _currentPositions & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Active professional engagements</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Avg Position Duration</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#10b981;margin-top:10px;'>" & _avgPositionDuration & "m</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Average tenure length</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Total Education</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f9fafb;margin-top:10px;'>" & _totalEducation & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Formal learning records</div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Current Education</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#2563eb;margin-top:10px;'>" & _currentEducation & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Ongoing education paths</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Avg Education Duration</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#10b981;margin-top:10px;'>" & _avgEducationDuration & "m</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Average learning cycle length</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Total Certifications</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f59e0b;margin-top:10px;'>" & _totalCertifications & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Technical credibility markers</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Avg Certification Duration</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f59e0b;margin-top:10px;'>" & _avgCertificationDuration & "m</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Average credential cycle</div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;flex:1;min-height:0;box-sizing:border-box;overflow:hidden;'>
    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Career Progression</div>
      <div style='font-size:26px;font-weight:800;color:#2563eb;line-height:1;'>" & _careerMaturityScore & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Trajectory continuity and progression depth</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Positions started</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _totalPositions & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Current positions</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _currentPositions & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Avg duration</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _avgPositionDuration & " months</strong>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Education Journey</div>
      <div style='font-size:26px;font-weight:800;color:#10b981;line-height:1;'>" & _totalEducation & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Academic structure and continuity</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Education started</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _totalEducation & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Current education</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _currentEducation & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Avg duration</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _avgEducationDuration & " months</strong>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Certification Layer</div>
      <div style='font-size:26px;font-weight:800;color:#f59e0b;line-height:1;'>" & _totalCertifications & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Credential-backed technical signaling</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Certifications</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _totalCertifications & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Avg duration</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _avgCertificationDuration & " months</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Career maturity</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _careerMaturityScore & "</strong>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Executive Signal</div>
      <div style='font-size:26px;font-weight:800;color:#2563eb;line-height:1;'>" & _careerMaturityScore & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Composite reading of trajectory and learning maturity</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Career score</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _careerMaturityScore & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Education layer</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _totalEducation & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Certification layer</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _totalCertifications & "</strong>
        </div>
      </div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:94px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Career Insights</div>
      <div style='font-size:13px;line-height:1.45;color:#f9fafb;margin-top:10px;'>" & _careerInsight & "</div>
    </div>
    <div style='min-height:94px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Learning Interpretation</div>
      <div style='font-size:13px;line-height:1.45;color:#f9fafb;margin-top:10px;'>" & _learningInsight & "</div>
    </div>
    <div style='min-height:94px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Suggested Narrative</div>
      <div style='font-size:13px;line-height:1.45;color:#f9fafb;margin-top:10px;'>" & _narrativeInsight & "</div>
    </div>
  </div>
</div>
</div>"
'@

$applicationsPresenceHtmlMeasureExpression = @'
VAR _totalApplicationsValue = [Total Applications]
VAR _applicationsPreviousMonth =
    CALCULATE (
        [Total Applications],
        DATEADD ( dCalendario[date], -1, MONTH )
    )
VAR _applicationsMoMValue =
    IF (
        NOT ISBLANK ( _applicationsPreviousMonth ) && _applicationsPreviousMonth <> 0,
        DIVIDE ( _totalApplicationsValue - _applicationsPreviousMonth, _applicationsPreviousMonth )
    )
VAR _totalEventsValue = [Total Events]
VAR _totalInvitationsValue = [Total Invitations]
VAR _totalRecommendationsValue = [Total Recommendations]
VAR _resumeCoverageValue = COALESCE ( [Applications with Resume %], 0 )
VAR _questionnaireCoverageValue = COALESCE ( [Applications with Questionnaire %], 0 )
VAR _eventUrlValue = COALESCE ( [Events with URL %], 0 )
VAR _invitationMessageValue = COALESCE ( [Invitations with Message %], 0 )
VAR _recommendationsDataValue = COALESCE ( [Recommendations Mention Data %], 0 )
VAR _engagementScoreValue =
    MAX (
        0,
        MIN (
            1,
            ( MIN ( 1, DIVIDE ( _totalRecommendationsValue, 15, 0 ) ) * 0.35 ) +
            ( MIN ( 1, DIVIDE ( _totalInvitationsValue, 40, 0 ) ) * 0.20 ) +
            ( _invitationMessageValue * 0.20 ) +
            ( MIN ( 1, DIVIDE ( _totalEventsValue, 20, 0 ) ) * 0.25 )
        )
    )
VAR _presenceCoverageValue =
    MAX (
        0,
        MIN (
            1,
            ( _eventUrlValue * 0.30 ) +
            ( _invitationMessageValue * 0.25 ) +
            ( _recommendationsDataValue * 0.25 ) +
            ( _resumeCoverageValue * 0.20 )
        )
    )
VAR _opportunitySignalValue =
    MAX (
        0,
        MIN (
            1,
            ( MIN ( 1, DIVIDE ( _totalApplicationsValue, 25, 0 ) ) * 0.45 ) +
            ( MIN ( 1, DIVIDE ( [Applications YTD], 25, 0 ) ) * 0.20 ) +
            ( _resumeCoverageValue * 0.20 ) +
            ( _questionnaireCoverageValue * 0.15 )
        )
    )
VAR _presenceScoreValue =
    MAX (
        0,
        MIN (
            1,
            ( _engagementScoreValue * 0.35 ) +
            ( _presenceCoverageValue * 0.35 ) +
            ( MIN ( 1, DIVIDE ( _totalRecommendationsValue, 15, 0 ) ) * 0.15 ) +
            ( MIN ( 1, DIVIDE ( _totalEventsValue, 20, 0 ) ) * 0.15 )
        )
    )
VAR _refresh = FORMAT ( [Last Refresh Date], "dd/mm/yyyy" )
VAR _totalApplications = FORMAT ( _totalApplicationsValue, "#,##0" )
VAR _applicationsMoM =
    IF (
        ISBLANK ( _applicationsMoMValue ),
        "n/a",
        FORMAT ( _applicationsMoMValue, "0.0%" )
    )
VAR _totalEvents = FORMAT ( _totalEventsValue, "#,##0" )
VAR _totalInvitations = FORMAT ( _totalInvitationsValue, "#,##0" )
VAR _totalRecommendations = FORMAT ( _totalRecommendationsValue, "#,##0" )
VAR _engagementScore = FORMAT ( _engagementScoreValue, "0.0%" )
VAR _presenceCoverage = FORMAT ( _presenceCoverageValue, "0.0%" )
VAR _opportunitySignal = FORMAT ( _opportunitySignalValue, "0.0%" )
VAR _presenceScore = FORMAT ( _presenceScoreValue, "0.0%" )
VAR _applicationsYTD = FORMAT ( [Applications YTD], "#,##0" )
VAR _resumeCoverage = FORMAT ( _resumeCoverageValue, "0.0%" )
VAR _questionnaireCoverage = FORMAT ( _questionnaireCoverageValue, "0.0%" )
VAR _eventUrlCoverage = FORMAT ( _eventUrlValue, "0.0%" )
VAR _invitationMessageRate = FORMAT ( _invitationMessageValue, "0.0%" )
VAR _recommendationDataRate = FORMAT ( _recommendationsDataValue, "0.0%" )
VAR _insight1 =
    SWITCH (
        TRUE (),
        _presenceScoreValue >= 0.65, "Professional exposure indicates active ecosystem participation.",
        _totalEventsValue > 0 || _totalInvitationsValue > 0, "Professional presence is visible and continues to build through interaction signals.",
        "Professional presence signals remain limited in the current context."
    )
VAR _insight2 =
    SWITCH (
        TRUE (),
        _totalRecommendationsValue > 0 && _recommendationsDataValue >= 0.40, "Recommendation signals reinforce professional credibility.",
        _totalRecommendationsValue > 0, "Recommendation activity exists and contributes to reputation visibility.",
        "Recommendation volume is still modest for stronger visible credibility."
    )
VAR _insight3 =
    SWITCH (
        TRUE (),
        _totalApplicationsValue > 0 && _totalInvitationsValue > 0, "Opportunity pipeline demonstrates continuous engagement.",
        _totalInvitationsValue > 0 || _totalEventsValue > 0, "Invitations and events suggest growing professional visibility.",
        "Opportunity activity is present but still building momentum."
    )
RETURN
"<div style='width:100%;height:100%;max-height:100%;overflow:hidden;box-sizing:border-box;background:#0b1220;color:#f9fafb;font-family:Segoe UI,Arial,sans-serif;padding:11px;'>
<div style='width:100%;height:100%;max-height:100%;overflow:hidden;box-sizing:border-box;display:flex;flex-direction:column;gap:8px;'>
  <div style='min-height:112px;box-sizing:border-box;border:1px solid #1f2937;border-radius:16px;background:
    linear-gradient(135deg,#0f172a 0%,#111827 58%,#0b1220 100%),
    linear-gradient(90deg,rgba(37,99,235,.12) 0 1px,transparent 1px),
    linear-gradient(180deg,rgba(255,255,255,.04) 0 1px,transparent 1px);
    background-size:auto,36px 36px,36px 36px;
    padding:14px 17px;display:flex;align-items:stretch;justify-content:space-between;gap:14px;overflow:hidden;position:relative;'>
    <div style='position:absolute;inset:0;background:
      radial-gradient(circle at 10% 18%, rgba(37,99,235,.16), transparent 24%),
      radial-gradient(circle at 74% 10%, rgba(16,185,129,.08), transparent 18%);
      pointer-events:none;'></div>
    <div style='display:flex;align-items:center;gap:14px;min-width:0;overflow:hidden;flex:1;position:relative;z-index:1;'>
      <div style='width:82px;height:82px;border-radius:50%;padding:3px;background:linear-gradient(135deg,#2563eb,#60a5fa);box-sizing:border-box;flex:0 0 auto;box-shadow:0 0 0 1px rgba(37,99,235,.25);'>
        <img src='https://diego-pablo.vercel.app/assets/img/profile.png' alt='Profile' style='width:100%;height:100%;border-radius:50%;object-fit:cover;display:block;background:#111827;' />
      </div>
      <div style='min-width:0;overflow:hidden;flex:1;'>
        <div style='font-size:10px;line-height:1;letter-spacing:.16em;text-transform:uppercase;color:#94a3b8;margin-bottom:7px;'>Applications &amp; Presence Intelligence</div>
        <div style='font-size:28px;line-height:1.02;font-weight:800;color:#f9fafb;white-space:nowrap;'>Applications &amp; Presence Intelligence</div>
        <div style='font-size:12px;line-height:1.35;color:#94a3b8;margin-top:7px;max-width:760px;'>Executive monitoring of opportunities, visibility and professional engagement.</div>
        <div style='display:flex;align-items:center;gap:8px;margin-top:10px;flex-wrap:wrap;'>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(37,99,235,.10);color:#bfdbfe;font-size:11px;font-weight:600;'>Applications</div>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(16,185,129,.10);color:#a7f3d0;font-size:11px;font-weight:600;'>Events</div>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(245,158,11,.10);color:#fde68a;font-size:11px;font-weight:600;'>Invitations</div>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(239,68,68,.10);color:#fecaca;font-size:11px;font-weight:600;'>Recommendations</div>
        </div>
      </div>
    </div>
    <div style='display:flex;flex-direction:column;justify-content:space-between;gap:9px;min-width:220px;overflow:hidden;position:relative;z-index:1;'>
      <div style='box-sizing:border-box;padding:10px 12px;border-radius:12px;border:1px solid #1f2937;background:#0f172a;overflow:hidden;'>
        <div style='font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;margin-bottom:4px;'>Last Refresh Date</div>
        <div style='font-size:16px;font-weight:700;color:#f9fafb;white-space:nowrap;'>" & _refresh & "</div>
        <div style='font-size:11px;color:#94a3b8;margin-top:6px;'>Operational reference of activity signals</div>
      </div>
      <div style='box-sizing:border-box;padding:10px 12px;border-radius:12px;border:1px solid #1f2937;background:#0f172a;overflow:hidden;'>
        <div style='font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;margin-bottom:4px;'>Presence Score</div>
        <div style='font-size:18px;font-weight:800;color:#10b981;white-space:nowrap;'>" & _presenceScore & "</div>
        <div style='font-size:11px;color:#94a3b8;margin-top:6px;'>Composite view of visibility and engagement</div>
      </div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Total Applications</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f9fafb;margin-top:10px;'>" & _totalApplications & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Tracked opportunities</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Applications MoM</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#2563eb;margin-top:10px;'>" & _applicationsMoM & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Monthly opportunity movement</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Total Events</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#10b981;margin-top:10px;'>" & _totalEvents & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Presence touchpoints</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Total Invitations</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f59e0b;margin-top:10px;'>" & _totalInvitations & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Relationship activity layer</div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Total Recommendations</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f9fafb;margin-top:10px;'>" & _totalRecommendations & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Public credibility markers</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Engagement Score</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#2563eb;margin-top:10px;'>" & _engagementScore & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Interaction quality signal</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Presence Coverage</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#10b981;margin-top:10px;'>" & _presenceCoverage & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Coverage of visible activity</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Opportunity Signal</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f59e0b;margin-top:10px;'>" & _opportunitySignal & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Pipeline continuity indicator</div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;flex:1;min-height:0;box-sizing:border-box;overflow:hidden;'>
    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Application Pipeline</div>
      <div style='font-size:26px;font-weight:800;color:#2563eb;line-height:1;'>" & _opportunitySignal & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Opportunity flow and execution quality</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Applications</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _totalApplications & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Applications YTD</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _applicationsYTD & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Resume coverage</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _resumeCoverage & "</strong>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Professional Presence</div>
      <div style='font-size:26px;font-weight:800;color:#10b981;line-height:1;'>" & _presenceCoverage & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Visibility across events and interactions</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Events with URL</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _eventUrlCoverage & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Invitations with message</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _invitationMessageRate & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Total events</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _totalEvents & "</strong>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Reputation Layer</div>
      <div style='font-size:26px;font-weight:800;color:#f59e0b;line-height:1;'>" & _engagementScore & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Public credibility and recommendation strength</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Recommendations</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _totalRecommendations & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Mentions data</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _recommendationDataRate & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Engagement score</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _engagementScore & "</strong>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Executive Signal</div>
      <div style='font-size:26px;font-weight:800;color:#2563eb;line-height:1;'>" & _presenceScore & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Consolidated reading of opportunity and visibility layers</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Presence score</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _presenceScore & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Opportunity signal</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _opportunitySignal & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Questionnaire coverage</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _questionnaireCoverage & "</strong>
        </div>
      </div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:94px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Opportunity Insights</div>
      <div style='font-size:13px;line-height:1.45;color:#f9fafb;margin-top:10px;'>" & _insight3 & "</div>
    </div>
    <div style='min-height:94px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Presence Interpretation</div>
      <div style='font-size:13px;line-height:1.45;color:#f9fafb;margin-top:10px;'>" & _insight1 & "</div>
    </div>
    <div style='min-height:94px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Suggested Narrative</div>
      <div style='font-size:13px;line-height:1.45;color:#f9fafb;margin-top:10px;'>" & _insight2 & "</div>
    </div>
  </div>
</div>
</div>"
'@

$pipelineGovernanceHtmlMeasureExpression = @'
VAR _successfulReadsValue = [Successful Reads]
VAR _failedReadsValue = [Failed Reads]
VAR _readSuccessRateValue = COALESCE ( [Read Success Rate %], 0 )
VAR _readFailureRateValue = COALESCE ( [Read Failure Rate %], 0 )
VAR _dataQualityScoreValue = COALESCE ( [Data Quality Score %], 0 )
VAR _inventoryFilesValue = [Total Inventory Files]
VAR _inventoryRowsValue = [Total Inventory Rows]
VAR _inventorySizeValue = [Total Inventory Size (KB)]
VAR _inventoryFreshnessValue = [Inventory Freshness (Days)]
VAR _governanceScoreValue =
    MAX (
        0,
        MIN (
            1,
            ( _readSuccessRateValue * 0.35 ) +
            ( _dataQualityScoreValue * 0.35 ) +
            ( MAX ( 0, MIN ( 1, 1 - DIVIDE ( _inventoryFreshnessValue, 30, 0 ) ) ) * 0.20 ) +
            ( MIN ( 1, DIVIDE ( _inventoryFilesValue, 50, 0 ) ) * 0.10 )
        )
    )
VAR _refresh = FORMAT ( [Last Refresh Date], "dd/mm/yyyy" )
VAR _successfulReads = FORMAT ( _successfulReadsValue, "#,##0" )
VAR _failedReads = FORMAT ( _failedReadsValue, "#,##0" )
VAR _readSuccessRate = FORMAT ( _readSuccessRateValue, "0.0%" )
VAR _readFailureRate = FORMAT ( _readFailureRateValue, "0.0%" )
VAR _dataQualityScore = FORMAT ( _dataQualityScoreValue, "0.0%" )
VAR _inventoryFiles = FORMAT ( _inventoryFilesValue, "#,##0" )
VAR _inventoryRows = FORMAT ( _inventoryRowsValue, "#,##0" )
VAR _inventorySize = FORMAT ( _inventorySizeValue, "#,##0" )
VAR _inventoryFreshness = FORMAT ( _inventoryFreshnessValue, "#,##0" )
VAR _governanceScore = FORMAT ( _governanceScoreValue, "0.0%" )
VAR _insight1 =
    SWITCH (
        TRUE (),
        _readSuccessRateValue >= 0.95, "Pipeline reliability supports executive reporting confidence.",
        _successfulReadsValue > 0, "Pipeline reliability is acceptable, with room for tighter operational resilience.",
        "Pipeline reliability signals remain limited in the current context."
    )
VAR _insight2 =
    SWITCH (
        TRUE (),
        _inventoryFilesValue > 0 && _inventoryRowsValue > 0, "Inventory footprint provides traceability of processed assets.",
        _inventoryFilesValue > 0, "Inventory layer exists and supports partial traceability of processed assets.",
        "Inventory traceability is currently limited in the active filter context."
    )
VAR _insight3 =
    SWITCH (
        TRUE (),
        _dataQualityScoreValue >= 0.80, "Data quality score indicates operational readiness.",
        _dataQualityScoreValue >= 0.60, "Governance layer strengthens auditability and analytical trust.",
        "Governance controls exist, but readiness indicators still need reinforcement."
    )
RETURN
"<div style='width:100%;height:100%;max-height:100%;overflow:hidden;box-sizing:border-box;background:#0b1220;color:#f9fafb;font-family:Segoe UI,Arial,sans-serif;padding:11px;'>
<div style='width:100%;height:100%;max-height:100%;overflow:hidden;box-sizing:border-box;display:flex;flex-direction:column;gap:8px;'>
  <div style='min-height:112px;box-sizing:border-box;border:1px solid #1f2937;border-radius:16px;background:
    linear-gradient(135deg,#0f172a 0%,#111827 58%,#0b1220 100%),
    linear-gradient(90deg,rgba(37,99,235,.12) 0 1px,transparent 1px),
    linear-gradient(180deg,rgba(255,255,255,.04) 0 1px,transparent 1px);
    background-size:auto,36px 36px,36px 36px;
    padding:14px 17px;display:flex;align-items:stretch;justify-content:space-between;gap:14px;overflow:hidden;position:relative;'>
    <div style='position:absolute;inset:0;background:
      radial-gradient(circle at 10% 18%, rgba(37,99,235,.16), transparent 24%),
      radial-gradient(circle at 74% 10%, rgba(16,185,129,.08), transparent 18%);
      pointer-events:none;'></div>
    <div style='display:flex;align-items:center;gap:14px;min-width:0;overflow:hidden;flex:1;position:relative;z-index:1;'>
      <div style='width:82px;height:82px;border-radius:50%;padding:3px;background:linear-gradient(135deg,#2563eb,#60a5fa);box-sizing:border-box;flex:0 0 auto;box-shadow:0 0 0 1px rgba(37,99,235,.25);'>
        <img src='https://diego-pablo.vercel.app/assets/img/profile.png' alt='Profile' style='width:100%;height:100%;border-radius:50%;object-fit:cover;display:block;background:#111827;' />
      </div>
      <div style='min-width:0;overflow:hidden;flex:1;'>
        <div style='font-size:10px;line-height:1;letter-spacing:.16em;text-transform:uppercase;color:#94a3b8;margin-bottom:7px;'>Pipeline &amp; Governance Intelligence</div>
        <div style='font-size:28px;line-height:1.02;font-weight:800;color:#f9fafb;white-space:nowrap;'>Pipeline &amp; Governance Intelligence</div>
        <div style='font-size:12px;line-height:1.35;color:#94a3b8;margin-top:7px;max-width:760px;'>Executive view of data pipeline reliability, governance, inventory and operational readiness.</div>
        <div style='display:flex;align-items:center;gap:8px;margin-top:10px;flex-wrap:wrap;'>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(37,99,235,.10);color:#bfdbfe;font-size:11px;font-weight:600;'>Pipeline</div>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(16,185,129,.10);color:#a7f3d0;font-size:11px;font-weight:600;'>Governance</div>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(245,158,11,.10);color:#fde68a;font-size:11px;font-weight:600;'>Inventory</div>
          <div style='padding:7px 10px;border-radius:999px;border:1px solid #1f2937;background:rgba(239,68,68,.10);color:#fecaca;font-size:11px;font-weight:600;'>Reliability</div>
        </div>
      </div>
    </div>
    <div style='display:flex;flex-direction:column;justify-content:space-between;gap:9px;min-width:220px;overflow:hidden;position:relative;z-index:1;'>
      <div style='box-sizing:border-box;padding:10px 12px;border-radius:12px;border:1px solid #1f2937;background:#0f172a;overflow:hidden;'>
        <div style='font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;margin-bottom:4px;'>Last Refresh Date</div>
        <div style='font-size:16px;font-weight:700;color:#f9fafb;white-space:nowrap;'>" & _refresh & "</div>
        <div style='font-size:11px;color:#94a3b8;margin-top:6px;'>Reference snapshot of pipeline operations</div>
      </div>
      <div style='box-sizing:border-box;padding:10px 12px;border-radius:12px;border:1px solid #1f2937;background:#0f172a;overflow:hidden;'>
        <div style='font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;margin-bottom:4px;'>Governance Score</div>
        <div style='font-size:18px;font-weight:800;color:#10b981;white-space:nowrap;'>" & _governanceScore & "</div>
        <div style='font-size:11px;color:#94a3b8;margin-top:6px;'>Composite operational readiness indicator</div>
      </div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Successful Reads</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#10b981;margin-top:10px;'>" & _successfulReads & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Successful pipeline executions</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Failed Reads</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#ef4444;margin-top:10px;'>" & _failedReads & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Failed pipeline executions</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Read Success Rate</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#2563eb;margin-top:10px;'>" & _readSuccessRate & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Execution reliability ratio</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Read Failure Rate</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f59e0b;margin-top:10px;'>" & _readFailureRate & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Operational risk signal</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Data Quality Score</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f9fafb;margin-top:10px;'>" & _dataQualityScore & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Quality readiness index</div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Inventory Files</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f9fafb;margin-top:10px;'>" & _inventoryFiles & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Tracked processed assets</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Inventory Rows</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#2563eb;margin-top:10px;'>" & _inventoryRows & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Processed row footprint</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Inventory Size (KB)</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#10b981;margin-top:10px;'>" & _inventorySize & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Physical asset size</div>
    </div>
    <div style='min-height:82px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Freshness (Days)</div>
      <div style='font-size:28px;line-height:1;font-weight:800;color:#f59e0b;margin-top:10px;'>" & _inventoryFreshness & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:7px;'>Days since latest refresh</div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;flex:1;min-height:0;box-sizing:border-box;overflow:hidden;'>
    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Pipeline Health</div>
      <div style='font-size:26px;font-weight:800;color:#2563eb;line-height:1;'>" & _readSuccessRate & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Reliability and execution continuity</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Successful reads</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _successfulReads & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Failed reads</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _failedReads & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Failure rate</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _readFailureRate & "</strong>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Inventory Footprint</div>
      <div style='font-size:26px;font-weight:800;color:#10b981;line-height:1;'>" & _inventoryFiles & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Processed asset inventory and volume</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Inventory files</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _inventoryFiles & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Inventory rows</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _inventoryRows & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Inventory size</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _inventorySize & " KB</strong>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Data Quality &amp; Reliability</div>
      <div style='font-size:26px;font-weight:800;color:#f59e0b;line-height:1;'>" & _dataQualityScore & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Operational quality and trust indicators</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Data quality</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _dataQualityScore & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Success rate</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _readSuccessRate & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Freshness</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _inventoryFreshness & " days</strong>
        </div>
      </div>
    </div>

    <div style='border:1px solid #1f2937;border-radius:12px;background:#111827;padding:14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:10px;'>Governance Signal</div>
      <div style='font-size:26px;font-weight:800;color:#2563eb;line-height:1;'>" & _governanceScore & "</div>
      <div style='font-size:11px;color:#94a3b8;margin-top:8px;'>Auditability, readiness and analytical trust</div>
      <div style='margin-top:12px;display:flex;flex-direction:column;gap:9px;'>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Governance score</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _governanceScore & "</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Inventory traceability</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _inventoryFiles & " files</strong>
        </div>
        <div style='display:flex;align-items:center;justify-content:space-between;border:1px solid #1f2937;border-radius:10px;background:#0f172a;padding:9px 10px;'>
          <span style='font-size:11px;color:#94a3b8;'>Operational trust</span>
          <strong style='font-size:12px;color:#f9fafb;'>" & _dataQualityScore & "</strong>
        </div>
      </div>
    </div>
  </div>

  <div style='display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;box-sizing:border-box;overflow:hidden;'>
    <div style='min-height:94px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Governance Insights</div>
      <div style='font-size:13px;line-height:1.45;color:#f9fafb;margin-top:10px;'>" & _insight1 & "</div>
    </div>
    <div style='min-height:94px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Operational Readiness</div>
      <div style='font-size:13px;line-height:1.45;color:#f9fafb;margin-top:10px;'>" & _insight3 & "</div>
    </div>
    <div style='min-height:94px;border:1px solid #1f2937;border-radius:12px;background:#111827;padding:12px 14px;box-sizing:border-box;overflow:hidden;'>
      <div style='font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#94a3b8;'>Suggested Narrative</div>
      <div style='font-size:13px;line-height:1.45;color:#f9fafb;margin-top:10px;'>" & _insight2 & "</div>
    </div>
  </div>
</div>
</div>"
'@

$context = Connect-Model -ServerName $DataSource -DatabaseName $Catalog
try {
    $healthCreated = Ensure-ImportTable -Context $context -TableName "fact_ingestion_audit_health_timeline" -MExpression $healthExpression -Description "Fato de observability com uma linha por carga e tabela monitorada, concentrando saúde operacional, retenção, duplicidade e volume processado."
    $nullCreated = Ensure-ImportTable -Context $context -TableName "fact_ingestion_audit_null_rate_timeline" -MExpression $nullRateExpression -Description "Fato de observability com uma linha por carga, tabela e coluna monitorada, concentrando métricas de null rate e alertas de qualidade."
    $tableDimCreated = Ensure-ImportTable -Context $context -TableName "dim_observability_table" -MExpression $tableDimensionExpression -Description "Dimensão auxiliar com a lista unificada de tabelas monitoradas pela camada de observability."
    $exportDimCreated = Ensure-ImportTable -Context $context -TableName "dim_observability_export_type" -MExpression $exportTypeDimensionExpression -Description "Dimensão auxiliar com os tipos de exportação ou carga usados na camada de observability."
    $healthStatusDimCreated = Ensure-ImportTable -Context $context -TableName "dim_observability_health_status" -MExpression $healthStatusDimensionExpression -Description "Dimensão auxiliar com os status semânticos de saúde operacional das cargas monitoradas."
    $columnDimCreated = Ensure-ImportTable -Context $context -TableName "dim_observability_column" -MExpression $columnDimensionExpression -Description "Dimensão auxiliar com as colunas monitoradas para avaliação de null rate."

    $schemaDefinitions = @(
        @{ Table = "fact_ingestion_audit_health_timeline"; Columns = @(
            @{ Name = "loaded_at_utc"; Source = "loaded_at_utc"; DataType = "DateTime"; SummarizeBy = "None" },
            @{ Name = "loaded_on"; Source = "loaded_on"; DataType = "DateTime"; SummarizeBy = "None" },
            @{ Name = "loaded_year_month"; Source = "loaded_year_month"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "table_key"; Source = "table_key"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "bronze_table"; Source = "bronze_table"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "source_file"; Source = "source_file"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "export_type"; Source = "export_type"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "source_row_count"; Source = "source_row_count"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "source_column_count"; Source = "source_column_count"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "row_count_after_transform"; Source = "row_count_after_transform"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "column_count_after_transform"; Source = "column_count_after_transform"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "rows_removed_during_transform"; Source = "rows_removed_during_transform"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "duplicate_rows_after_transform"; Source = "duplicate_rows_after_transform"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "row_retention_rate"; Source = "row_retention_rate"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "row_count_change_vs_previous_load"; Source = "row_count_change_vs_previous_load"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "rows_removed_change_vs_previous_load"; Source = "rows_removed_change_vs_previous_load"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "duplicate_alert_flag"; Source = "duplicate_alert_flag"; DataType = "Boolean"; SummarizeBy = "None" },
            @{ Name = "row_removal_alert_flag"; Source = "row_removal_alert_flag"; DataType = "Boolean"; SummarizeBy = "None" },
            @{ Name = "health_status"; Source = "health_status"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "successful_load_flag"; Source = "successful_load_flag"; DataType = "Int64"; SummarizeBy = "Sum" }
        )},
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Columns = @(
            @{ Name = "loaded_at_utc"; Source = "loaded_at_utc"; DataType = "DateTime"; SummarizeBy = "None" },
            @{ Name = "loaded_on"; Source = "loaded_on"; DataType = "DateTime"; SummarizeBy = "None" },
            @{ Name = "loaded_year_month"; Source = "loaded_year_month"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "table_key"; Source = "table_key"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "bronze_table"; Source = "bronze_table"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "export_type"; Source = "export_type"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "source_file"; Source = "source_file"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "source_row_count"; Source = "source_row_count"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "row_count_after_transform"; Source = "row_count_after_transform"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "rows_removed_during_transform"; Source = "rows_removed_during_transform"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "monitored_column"; Source = "monitored_column"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "null_rate_before_transform"; Source = "null_rate_before_transform"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "null_rate_after_transform"; Source = "null_rate_after_transform"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "null_rate_delta"; Source = "null_rate_delta"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "null_rate_change_vs_previous_load"; Source = "null_rate_change_vs_previous_load"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "null_rate_alert_flag"; Source = "null_rate_alert_flag"; DataType = "Boolean"; SummarizeBy = "None" }
        )},
        @{ Table = "dim_observability_table"; Columns = @(
            @{ Name = "table_key"; Source = "table_key"; DataType = "String"; SummarizeBy = "None" }
        )},
        @{ Table = "dim_observability_export_type"; Columns = @(
            @{ Name = "export_type"; Source = "export_type"; DataType = "String"; SummarizeBy = "None" }
        )},
        @{ Table = "dim_observability_health_status"; Columns = @(
            @{ Name = "health_status"; Source = "health_status"; DataType = "String"; SummarizeBy = "None" }
        )},
        @{ Table = "dim_observability_column"; Columns = @(
            @{ Name = "monitored_column"; Source = "monitored_column"; DataType = "String"; SummarizeBy = "None" }
        )}
    )

    foreach ($tableDefinition in $schemaDefinitions) {
        foreach ($columnDefinition in $tableDefinition.Columns) {
            Ensure-DataColumn -Context $context -TableName $tableDefinition.Table -ColumnName $columnDefinition.Name -SourceColumn $columnDefinition.Source -DataType $columnDefinition.DataType -SummarizeBy $columnDefinition.SummarizeBy
        }
    }
    Save-Model -Context $context

    foreach ($tableName in @(
        "fact_ingestion_audit_health_timeline",
        "fact_ingestion_audit_null_rate_timeline",
        "dim_observability_table",
        "dim_observability_export_type",
        "dim_observability_health_status",
        "dim_observability_column"
    )) {
        Refresh-TableFull -Context $context -TableName $tableName
    }

    $tableColumnMetadata = @(
        @{ Table = "dim_observability_table"; Column = "table_key"; Description = "Nome lógico da tabela monitorada para filtros externos e navegação analítica."; Hidden = $false },
        @{ Table = "dim_observability_export_type"; Column = "export_type"; Description = "Tipo de exportação ou carga monitorada pela observability."; Hidden = $false },
        @{ Table = "dim_observability_health_status"; Column = "health_status"; Description = "Status de saúde semântico da carga monitorada."; Hidden = $false },
        @{ Table = "dim_observability_column"; Column = "monitored_column"; Description = "Nome da coluna monitorada para análise de null rate."; Hidden = $false },

        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "loaded_at_utc"; Description = "Timestamp UTC da execução monitorada."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "loaded_on"; Description = "Data normalizada da carga usada no relacionamento com o calendário."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "loaded_year_month"; Description = "Rótulo ano-mês da carga para conferência temporal."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "table_key"; Description = "Nome lógico da tabela monitorada na carga."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "bronze_table"; Description = "Nome técnico da tabela de origem na camada bronze."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "source_file"; Description = "Arquivo fonte associado à carga monitorada."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "export_type"; Description = "Tipo de exportação ou carga associada ao registro monitorado."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "source_row_count"; Description = "Quantidade de linhas recebidas antes da transformação."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "source_column_count"; Description = "Quantidade de colunas na origem bruta da carga."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "row_count_after_transform"; Description = "Quantidade de linhas preservadas após a transformação."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "column_count_after_transform"; Description = "Quantidade de colunas preservadas após a transformação."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "rows_removed_during_transform"; Description = "Quantidade de linhas removidas durante as regras de transformação."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "duplicate_rows_after_transform"; Description = "Quantidade de duplicidades ainda presentes após a transformação."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "row_retention_rate"; Description = "Taxa de retenção de linhas após transformação."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "row_count_change_vs_previous_load"; Description = "Variação da contagem de linhas em relação à carga anterior."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "rows_removed_change_vs_previous_load"; Description = "Variação das remoções de linhas em relação à carga anterior."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "duplicate_alert_flag"; Description = "Indicador de alerta de duplicidade após a transformação."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "row_removal_alert_flag"; Description = "Indicador de alerta de remoção excessiva de linhas."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "health_status"; Description = "Status semântico consolidado de saúde da carga monitorada."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_health_timeline"; Column = "successful_load_flag"; Description = "Indicador de carga concluída com sucesso."; Hidden = $false },

        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "loaded_at_utc"; Description = "Timestamp UTC da execução monitorada para null rate."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "loaded_on"; Description = "Data normalizada da carga usada no relacionamento com o calendário."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "loaded_year_month"; Description = "Rótulo ano-mês da carga para conferência temporal."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "table_key"; Description = "Nome lógico da tabela monitorada na linha de null rate."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "bronze_table"; Description = "Nome técnico da tabela de origem na camada bronze."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "export_type"; Description = "Tipo de exportação ou carga associada ao monitoramento de null rate."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "source_file"; Description = "Arquivo fonte associado ao monitoramento de null rate."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "source_row_count"; Description = "Quantidade de linhas recebidas antes da transformação para a carga monitorada."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "row_count_after_transform"; Description = "Quantidade de linhas preservadas após a transformação para a carga monitorada."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "rows_removed_during_transform"; Description = "Quantidade de linhas removidas no mesmo contexto da análise de null rate."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "monitored_column"; Description = "Nome da coluna monitorada para qualidade e null rate."; Hidden = $true },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "null_rate_before_transform"; Description = "Null rate observado antes da transformação."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "null_rate_after_transform"; Description = "Null rate observado após a transformação."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "null_rate_delta"; Description = "Diferença entre o null rate antes e depois da transformação."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "null_rate_change_vs_previous_load"; Description = "Variação do null rate em relação à carga anterior."; Hidden = $false },
        @{ Table = "fact_ingestion_audit_null_rate_timeline"; Column = "null_rate_alert_flag"; Description = "Indicador de alerta para null rate crítico."; Hidden = $false }
    )

    foreach ($metadata in $tableColumnMetadata) {
        Ensure-ColumnMetadata -Context $context -TableName $metadata.Table -ColumnName $metadata.Column -Description $metadata.Description -Hidden $metadata.Hidden
    }

    Remove-ProhibitedRelationships -Context $context

    Ensure-Relationship -Context $context -FromTable "fact_ingestion_audit_health_timeline" -FromColumn "loaded_on" -ToTable "dCalendario" -ToColumn "date"
    Ensure-Relationship -Context $context -FromTable "fact_ingestion_audit_null_rate_timeline" -FromColumn "loaded_on" -ToTable "dCalendario" -ToColumn "date"
    Ensure-Relationship -Context $context -FromTable "fact_ingestion_audit_health_timeline" -FromColumn "table_key" -ToTable "dim_observability_table" -ToColumn "table_key"
    Ensure-Relationship -Context $context -FromTable "fact_ingestion_audit_null_rate_timeline" -FromColumn "table_key" -ToTable "dim_observability_table" -ToColumn "table_key"
    Ensure-Relationship -Context $context -FromTable "fact_ingestion_audit_health_timeline" -FromColumn "export_type" -ToTable "dim_observability_export_type" -ToColumn "export_type"
    Ensure-Relationship -Context $context -FromTable "fact_ingestion_audit_null_rate_timeline" -FromColumn "export_type" -ToTable "dim_observability_export_type" -ToColumn "export_type"
    Ensure-Relationship -Context $context -FromTable "fact_ingestion_audit_health_timeline" -FromColumn "health_status" -ToTable "dim_observability_health_status" -ToColumn "health_status"
    Ensure-Relationship -Context $context -FromTable "fact_ingestion_audit_null_rate_timeline" -FromColumn "monitored_column" -ToTable "dim_observability_column" -ToColumn "monitored_column"

    foreach ($measureDefinition in $measureDefinitions) {
        Ensure-Measure -Context $context -Name $measureDefinition.Name -Expression $measureDefinition.Expression -Description $measureDefinition.Description -FormatString $measureDefinition.FormatString -DisplayFolder $measureDefinition.DisplayFolder
    }

    Ensure-Measure -Context $context -Name "Pipeline_Data_Quality_Observability_HTML_Page" -Expression $htmlMeasureExpression -Description "Renderiza a página Pipeline & Data Quality Observability em HTML Content, com indicadores de saúde das cargas, qualidade dos dados, null rate, alertas, freshness e observabilidade do pipeline." -FormatString "" -DisplayFolder "14 | HTML Content" -NormalizeQuotes $false
    Ensure-Measure -Context $context -Name "Overview_HTML_LinkedIn_Career_Intelligence" -Expression $overviewHtmlMeasureExpression -Description "Renderiza a página Visão Geral em HTML Content, com leitura executiva de networking, aplicações, trajetória, presença profissional e saúde do pipeline." -FormatString "" -DisplayFolder "14 | HTML Content" -NormalizeQuotes $false
    Ensure-Measure -Context $context -Name "Networking_HTML_Page" -Expression $networkingHtmlMeasureExpression -Description "Renderiza a página Networking em HTML Content, com leitura executiva de conexões, alcance, engajamento e força relacional." -FormatString "" -DisplayFolder "14 | HTML Content" -NormalizeQuotes $false
    Ensure-Measure -Context $context -Name "Career_Education_HTML_Page" -Expression $careerEducationHtmlMeasureExpression -Description "Renderiza a página Carreira & Education em HTML Content, com leitura executiva de evolução profissional, educação, certificações e maturidade da trajetória." -FormatString "" -DisplayFolder "14 | HTML Content" -NormalizeQuotes $false
    Ensure-Measure -Context $context -Name "Applications_Presence_HTML_Page" -Expression $applicationsPresenceHtmlMeasureExpression -Description "Renderiza a página Applications & Presence em HTML Content, com leitura executiva de candidaturas, presença profissional, eventos, convites, recomendações e pipeline de oportunidades." -FormatString "" -DisplayFolder "14 | HTML Content" -NormalizeQuotes $false
    Ensure-Measure -Context $context -Name "Pipeline_Governance_HTML_Page" -Expression $pipelineGovernanceHtmlMeasureExpression -Description "Renderiza a página Pipeline & Governance em HTML Content, com leitura executiva de saúde do pipeline, governança, inventário, qualidade operacional e prontidão analítica." -FormatString "" -DisplayFolder "14 | HTML Content" -NormalizeQuotes $false

    Save-Model -Context $context
    Refresh-ModelCalculate -Context $context

    [PSCustomObject]@{
        AppliedChanges = $script:Changes
        Tables = @(
            "fact_ingestion_audit_health_timeline",
            "fact_ingestion_audit_null_rate_timeline",
            "dim_observability_table",
            "dim_observability_export_type",
            "dim_observability_health_status",
            "dim_observability_column"
        )
        MeasuresCreatedOrUpdated = @($measureDefinitions.Name) + @("Pipeline_Data_Quality_Observability_HTML_Page", "Overview_HTML_LinkedIn_Career_Intelligence", "Networking_HTML_Page", "Career_Education_HTML_Page", "Applications_Presence_HTML_Page", "Pipeline_Governance_HTML_Page")
    } | ConvertTo-Json -Depth 5
}
finally {
    if ($context -and $context.Server) {
        $context.Server.Disconnect()
    }
}
