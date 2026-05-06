param(
    [string]$DataSource = "",
    [string]$Catalog = "",
    [string]$ProjectRoot = "C:\Projetos\linkedIn_career_intelligence_lakehouse"
)

$ErrorActionPreference = "Stop"

[void][System.Reflection.Assembly]::LoadFrom("C:\Program Files\DAX Studio\bin\Microsoft.AnalysisServices.Core.dll")
[void][System.Reflection.Assembly]::LoadFrom("C:\Program Files\DAX Studio\bin\Microsoft.AnalysisServices.Tabular.dll")
[void][System.Reflection.Assembly]::LoadFrom("C:\Program Files\DAX Studio\bin\Microsoft.AnalysisServices.AdomdClient.dll")

$script:Changes = [System.Collections.Generic.List[string]]::new()

function Add-Change {
    param([string]$Message)
    $script:Changes.Add($Message)
}

function Resolve-PortString {
    param([string]$PortFilePath)
    $raw = Get-Content $PortFilePath -Raw
    return (($raw.ToCharArray() | Where-Object { [int]$_ -ne 0 }) -join '').Trim()
}

function Resolve-OpenPowerBIModel {
    param(
        [string]$RequestedDataSource,
        [string]$RequestedCatalog
    )

    if ($RequestedDataSource -and $RequestedCatalog) {
        return [PSCustomObject]@{
            DataSource = $RequestedDataSource
            Catalog = $RequestedCatalog
        }
    }

    $workspaceRoot = Join-Path $env:USERPROFILE "Microsoft\Power BI Desktop Store App\AnalysisServicesWorkspaces"
    $workspaces = @(Get-ChildItem $workspaceRoot -Directory | ForEach-Object {
        $portFile = Join-Path $_.FullName "Data\msmdsrv.port.txt"
        if (Test-Path $portFile) {
            [PSCustomObject]@{
                Workspace = $_.Name
                Port = Resolve-PortString -PortFilePath $portFile
            }
        }
    })

    if ($workspaces.Count -ne 1) {
        throw "Expected exactly one open Power BI Analysis Services workspace, found $($workspaces.Count)."
    }

    $server = New-Object Microsoft.AnalysisServices.Tabular.Server
    try {
        $server.Connect("DataSource=localhost:$($workspaces[0].Port)")
        if ($server.Databases.Count -ne 1) {
            throw "Expected exactly one open semantic model on localhost:$($workspaces[0].Port), found $($server.Databases.Count)."
        }

        return [PSCustomObject]@{
            DataSource = "localhost:$($workspaces[0].Port)"
            Catalog = $server.Databases[0].Name
        }
    }
    finally {
        $server.Disconnect()
    }
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
        $table.Description = $Description
        Save-Model -Context $Context
        Add-Change "Updated import expression for table '$TableName'."
        return
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
    $column = $table.Columns.Find($ColumnName)
    $column.Description = $Description
    $column.IsHidden = $Hidden
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

function Refresh-TableFull {
    param(
        $Context,
        [string]$TableName
    )

    $table = $Context.Model.Tables.Find($TableName)
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

function Ensure-Measure {
    param(
        $Context,
        [string]$Name,
        [string]$Expression,
        [string]$Description,
        [string]$FormatString,
        [string]$DisplayFolder
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

    $measure.Expression = $Expression
    $measure.Description = $Description
    $measure.FormatString = $FormatString
    $measure.DisplayFolder = $DisplayFolder
}

function Invoke-DaxRowCount {
    param(
        [string]$ServerName,
        [string]$DatabaseName,
        [string]$TableName
    )

    $connection = New-Object Microsoft.AnalysisServices.AdomdClient.AdomdConnection("Data Source=$ServerName;Catalog=$DatabaseName")
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = "EVALUATE ROW(""RowCount"", COUNTROWS('$TableName'))"
        $reader = $command.ExecuteReader()
        if ($reader.Read()) {
            return [int64]$reader.GetValue(0)
        }
        return 0
    }
    finally {
        $connection.Close()
    }
}

$resolved = Resolve-OpenPowerBIModel -RequestedDataSource $DataSource -RequestedCatalog $Catalog
$DataSource = $resolved.DataSource
$Catalog = $resolved.Catalog

$factNetworkCsv = Join-Path $ProjectRoot "powerbi\exports\fact_snapshot_network_growth.csv"
$factApplicationsCsv = Join-Path $ProjectRoot "powerbi\exports\fact_snapshot_applications.csv"
$factPresenceCsv = Join-Path $ProjectRoot "powerbi\exports\fact_snapshot_presence.csv"
$factCareerCsv = Join-Path $ProjectRoot "powerbi\exports\fact_snapshot_career_education.csv"
$factDataQualityCsv = Join-Path $ProjectRoot "powerbi\exports\fact_snapshot_data_quality.csv"
$dimMethodCsv = Join-Path $ProjectRoot "powerbi\exports\dim_snapshot_method.csv"
$dimRunCsv = Join-Path $ProjectRoot "powerbi\exports\dim_snapshot_run.csv"

$factNetworkExpression = @"
let
    Source = Csv.Document(File.Contents("$factNetworkCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"snapshot_run_id", type text}, {"snapshot_date", type date}, {"snapshot_year_month", type text}, {"monthly_new_connections", Int64.Type}, {"total_connections_cumulative", Int64.Type}, {"monthly_new_connections_with_email", type number}, {"connections_with_email_cumulative", type number}, {"connections_with_email_pct", type number}, {"unique_companies_in_month", Int64.Type}, {"unique_positions_in_month", Int64.Type}, {"connections_mom_delta", Int64.Type}, {"connections_mom_pct", type number}, {"is_simulated_snapshot", type logical}, {"snapshot_method", type text}, {"snapshot_source", type text}, {"snapshot_created_at", type datetimezone}}, "en-US")
in
    #"Changed Type"
"@

$factApplicationsExpression = @"
let
    Source = Csv.Document(File.Contents("$factApplicationsCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"snapshot_run_id", type text}, {"snapshot_date", type date}, {"snapshot_year_month", type text}, {"total_applications", Int64.Type}, {"cumulative_total_applications", Int64.Type}, {"applications_with_resume", Int64.Type}, {"cumulative_applications_with_resume", Int64.Type}, {"applications_with_questionnaire", Int64.Type}, {"cumulative_applications_with_questionnaire", Int64.Type}, {"applications_with_resume_pct", type number}, {"applications_with_questionnaire_pct", type number}, {"job_family_count", Int64.Type}, {"applications_mom_delta", Int64.Type}, {"applications_mom_pct", type number}, {"is_simulated_snapshot", type logical}, {"snapshot_method", type text}, {"snapshot_source", type text}, {"snapshot_created_at", type datetimezone}}, "en-US")
in
    #"Changed Type"
"@

$factPresenceExpression = @"
let
    Source = Csv.Document(File.Contents("$factPresenceCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"snapshot_run_id", type text}, {"snapshot_date", type date}, {"snapshot_year_month", type text}, {"monthly_events", type number}, {"monthly_events_with_url", type number}, {"events_with_url_pct", type number}, {"monthly_invitations", type number}, {"monthly_invitations_with_message", type number}, {"invitations_with_message_pct", type number}, {"monthly_recommendations", type number}, {"average_recommendation_text_length", type number}, {"mentions_data_count", type number}, {"recommendations_mention_data_pct", type number}, {"cumulative_events", type number}, {"cumulative_invitations", type number}, {"cumulative_recommendations", type number}, {"presence_score", type number}, {"engagement_score", type number}, {"is_simulated_snapshot", type logical}, {"snapshot_method", type text}, {"snapshot_source", type text}, {"snapshot_created_at", type datetimezone}}, "en-US")
in
    #"Changed Type"
"@

$factCareerExpression = @"
let
    Source = Csv.Document(File.Contents("$factCareerCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"snapshot_run_id", type text}, {"snapshot_date", type date}, {"snapshot_year_month", type text}, {"positions_started_in_month", Int64.Type}, {"positions_started_cumulative", Int64.Type}, {"current_positions_started_in_month", Int64.Type}, {"current_positions_cumulative", Int64.Type}, {"avg_position_duration_months", type number}, {"avg_position_duration_cumulative", type number}, {"education_started_in_month", Int64.Type}, {"education_started_cumulative", Int64.Type}, {"current_education_started_in_month", Int64.Type}, {"current_education_cumulative", Int64.Type}, {"avg_education_duration_months", type number}, {"avg_education_duration_cumulative", type number}, {"certifications_started_in_month", Int64.Type}, {"certifications_cumulative", Int64.Type}, {"avg_certification_duration_months", type number}, {"avg_certification_duration_cumulative", type number}, {"career_maturity_score", type number}, {"is_simulated_snapshot", type logical}, {"snapshot_method", type text}, {"snapshot_source", type text}, {"snapshot_created_at", type datetimezone}}, "en-US")
in
    #"Changed Type"
"@

$factDataQualityExpression = @"
let
    Source = Csv.Document(File.Contents("$factDataQualityCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"snapshot_run_id", type text}, {"snapshot_date", type date}, {"snapshot_year_month", type text}, {"table_key", type text}, {"export_type", type text}, {"source_row_count", Int64.Type}, {"row_count_after_transform", Int64.Type}, {"rows_removed_during_transform", Int64.Type}, {"duplicate_rows_after_transform", Int64.Type}, {"row_retention_rate", type number}, {"health_status", type text}, {"duplicate_alert_flag", type logical}, {"row_removal_alert_flag", type logical}, {"successful_load_flag", Int64.Type}, {"monitored_columns_count", type number}, {"avg_null_rate_before_transform", type number}, {"avg_null_rate_after_transform", type number}, {"avg_null_rate_delta", type number}, {"null_rate_alert_count", type number}, {"is_simulated_snapshot", type logical}, {"snapshot_method", type text}, {"snapshot_source", type text}, {"snapshot_created_at", type datetimezone}}, "en-US")
in
    #"Changed Type"
"@

$dimMethodExpression = @"
let
    Source = Csv.Document(File.Contents("$dimMethodCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"snapshot_method", type text}, {"method_group", type text}, {"method_description", type text}, {"is_simulated", type logical}}, "en-US")
in
    #"Changed Type"
"@

$dimRunExpression = @"
let
    Source = Csv.Document(File.Contents("$dimRunCsv"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"snapshot_run_id", type text}, {"run_started_at", type datetimezone}, {"run_finished_at", type datetimezone}, {"phase_name", type text}, {"contains_simulated_data", type logical}, {"source_warehouse_path", type text}, {"project_git_commit", type text}, {"notes", type text}}, "en-US")
in
    #"Changed Type"
"@

$measureDefinitions = @(
    @{ Name = "Snapshot Connections"; Expression = "COALESCE ( MAX ( fact_snapshot_network_growth[total_connections_cumulative] ), 0 )"; Description = "Retorna o total acumulado de conexões no último snapshot visível do contexto atual."; FormatString = "#,##0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Applications"; Expression = "COALESCE ( MAX ( fact_snapshot_applications[cumulative_total_applications] ), 0 )"; Description = "Retorna o total acumulado de aplicações no último snapshot visível do contexto atual."; FormatString = "#,##0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Events"; Expression = "COALESCE ( MAX ( fact_snapshot_presence[cumulative_events] ), 0 )"; Description = "Retorna o total acumulado de eventos no último snapshot visível do contexto atual."; FormatString = "#,##0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Invitations"; Expression = "COALESCE ( MAX ( fact_snapshot_presence[cumulative_invitations] ), 0 )"; Description = "Retorna o total acumulado de convites no último snapshot visível do contexto atual."; FormatString = "#,##0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Recommendations"; Expression = "COALESCE ( MAX ( fact_snapshot_presence[cumulative_recommendations] ), 0 )"; Description = "Retorna o total acumulado de recomendações no último snapshot visível do contexto atual."; FormatString = "#,##0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Career Maturity Score %"; Expression = "COALESCE ( MAX ( fact_snapshot_career_education[career_maturity_score] ), 0 )"; Description = "Retorna o score sintético de maturidade de carreira no último snapshot visível do contexto atual."; FormatString = "0.0%"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Presence Score %"; Expression = "COALESCE ( MAX ( fact_snapshot_presence[presence_score] ), 0 )"; Description = "Retorna o score sintético de presença profissional no último snapshot visível do contexto atual."; FormatString = "0.0%"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Engagement Score %"; Expression = "COALESCE ( MAX ( fact_snapshot_presence[engagement_score] ), 0 )"; Description = "Retorna o score sintético de engajamento profissional no último snapshot visível do contexto atual."; FormatString = "0.0%"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Data Quality Rows"; Expression = "COALESCE ( SUM ( fact_snapshot_data_quality[row_count_after_transform] ), 0 )"; Description = "Soma o volume de linhas pós-transformação observado na camada histórica de data quality."; FormatString = "#,##0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Avg Null Rate %"; Expression = "COALESCE ( AVERAGE ( fact_snapshot_data_quality[avg_null_rate_after_transform] ), 0 )"; Description = "Calcula o null rate médio histórico após a transformação no contexto filtrado."; FormatString = "0.0%"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Max Null Rate %"; Expression = "COALESCE ( MAX ( fact_snapshot_data_quality[avg_null_rate_after_transform] ), 0 )"; Description = "Retorna o pior null rate médio histórico observado no contexto filtrado."; FormatString = "0.0%"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Data Quality Alerts"; Expression = "COALESCE ( SUM ( fact_snapshot_data_quality[null_rate_alert_count] ), 0 ) + COALESCE ( CALCULATE ( COUNTROWS ( fact_snapshot_data_quality ), fact_snapshot_data_quality[duplicate_alert_flag] = TRUE () ), 0 ) + COALESCE ( CALCULATE ( COUNTROWS ( fact_snapshot_data_quality ), fact_snapshot_data_quality[row_removal_alert_flag] = TRUE () ), 0 )"; Description = "Consolida alertas históricos de null rate, duplicidade e remoção de linhas na camada de data quality."; FormatString = "#,##0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Read Success Rate %"; Expression = "COALESCE ( DIVIDE ( SUM ( fact_snapshot_data_quality[successful_load_flag] ), COUNTROWS ( fact_snapshot_data_quality ) ), 0 )"; Description = "Calcula a taxa histórica de sucesso de leitura das cargas monitoradas."; FormatString = "0.0%"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Row Retention Rate %"; Expression = "COALESCE ( AVERAGE ( fact_snapshot_data_quality[row_retention_rate] ), 0 )"; Description = "Calcula a taxa média histórica de retenção de linhas após transformação."; FormatString = "0.0%"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Historical Snapshot Count"; Expression = "COALESCE ( DISTINCTCOUNT ( dim_snapshot_run[snapshot_run_id] ), 0 )"; Description = "Conta o total de execuções históricas registradas na dimensão de snapshot runs."; FormatString = "#,##0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Simulated Snapshot Count"; Expression = "COALESCE ( CALCULATE ( DISTINCTCOUNT ( dim_snapshot_run[snapshot_run_id] ), dim_snapshot_run[contains_simulated_data] = TRUE () ), 0 )"; Description = "Conta o total de execuções históricas marcadas com conteúdo simulado."; FormatString = "#,##0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Actual Snapshot Count"; Expression = "COALESCE ( CALCULATE ( DISTINCTCOUNT ( dim_snapshot_run[snapshot_run_id] ), dim_snapshot_run[contains_simulated_data] = FALSE () ), 0 )"; Description = "Conta o total de execuções históricas compostas apenas por dados reais ou cumulativos derivados de eventos reais."; FormatString = "#,##0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Connections MoM"; Expression = "COALESCE ( [Snapshot Connections] - CALCULATE ( [Snapshot Connections], DATEADD ( dCalendario[date], -1, MONTH ) ), 0 )"; Description = "Calcula a variação mês contra mês do total acumulado de conexões."; FormatString = "+#,##0;-#,##0;0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Applications MoM"; Expression = "COALESCE ( [Snapshot Applications] - CALCULATE ( [Snapshot Applications], DATEADD ( dCalendario[date], -1, MONTH ) ), 0 )"; Description = "Calcula a variação mês contra mês do total acumulado de aplicações."; FormatString = "+#,##0;-#,##0;0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Presence MoM"; Expression = "COALESCE ( [Snapshot Presence Score %] - CALCULATE ( [Snapshot Presence Score %], DATEADD ( dCalendario[date], -1, MONTH ) ), 0 )"; Description = "Calcula a variação mês contra mês do score histórico de presença profissional."; FormatString = "+0.0%;-0.0%;0.0%"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Data Quality Alerts MoM"; Expression = "COALESCE ( [Snapshot Data Quality Alerts] - CALCULATE ( [Snapshot Data Quality Alerts], DATEADD ( dCalendario[date], -1, MONTH ) ), 0 )"; Description = "Calcula a variação mês contra mês do volume consolidado de alertas históricos de data quality."; FormatString = "+#,##0;-#,##0;0"; DisplayFolder = "16 | Historical Snapshots" },
    @{ Name = "Snapshot Avg Null Rate MoM"; Expression = "COALESCE ( [Snapshot Avg Null Rate %] - CALCULATE ( [Snapshot Avg Null Rate %], DATEADD ( dCalendario[date], -1, MONTH ) ), 0 )"; Description = "Calcula a variação mês contra mês do null rate médio histórico após transformação."; FormatString = "+0.0%;-0.0%;0.0%"; DisplayFolder = "16 | Historical Snapshots" }
)

$context = Connect-Model -ServerName $DataSource -DatabaseName $Catalog
try {
    Ensure-ImportTable -Context $context -TableName "fact_snapshot_network_growth" -MExpression $factNetworkExpression -Description "Fato de snapshots históricos de networking com volumetria mensal acumulada e sinais de cobertura."
    Ensure-ImportTable -Context $context -TableName "fact_snapshot_applications" -MExpression $factApplicationsExpression -Description "Fato de snapshots históricos de aplicações e qualidade das candidaturas."
    Ensure-ImportTable -Context $context -TableName "fact_snapshot_presence" -MExpression $factPresenceExpression -Description "Fato de snapshots históricos de presença profissional, eventos, convites e recomendações."
    Ensure-ImportTable -Context $context -TableName "fact_snapshot_career_education" -MExpression $factCareerExpression -Description "Fato de snapshots históricos de carreira, educação e certificações."
    Ensure-ImportTable -Context $context -TableName "fact_snapshot_data_quality" -MExpression $factDataQualityExpression -Description "Fato de snapshots históricos da camada de observability e data quality."
    Ensure-ImportTable -Context $context -TableName "dim_snapshot_method" -MExpression $dimMethodExpression -Description "Dimensão de governança com os métodos usados na geração dos snapshots históricos."
    Ensure-ImportTable -Context $context -TableName "dim_snapshot_run" -MExpression $dimRunExpression -Description "Dimensão de rastreabilidade das execuções de geração dos snapshots históricos."

    $schemaDefinitions = @(
        @{ Table = "fact_snapshot_network_growth"; Columns = @(
            @{ Name = "snapshot_run_id"; Source = "snapshot_run_id"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_date"; Source = "snapshot_date"; DataType = "DateTime"; SummarizeBy = "None" },
            @{ Name = "snapshot_year_month"; Source = "snapshot_year_month"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "monthly_new_connections"; Source = "monthly_new_connections"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "total_connections_cumulative"; Source = "total_connections_cumulative"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "monthly_new_connections_with_email"; Source = "monthly_new_connections_with_email"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "connections_with_email_cumulative"; Source = "connections_with_email_cumulative"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "connections_with_email_pct"; Source = "connections_with_email_pct"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "unique_companies_in_month"; Source = "unique_companies_in_month"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "unique_positions_in_month"; Source = "unique_positions_in_month"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "connections_mom_delta"; Source = "connections_mom_delta"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "connections_mom_pct"; Source = "connections_mom_pct"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "is_simulated_snapshot"; Source = "is_simulated_snapshot"; DataType = "Boolean"; SummarizeBy = "None" },
            @{ Name = "snapshot_method"; Source = "snapshot_method"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_source"; Source = "snapshot_source"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_created_at"; Source = "snapshot_created_at"; DataType = "DateTime"; SummarizeBy = "None" }
        )},
        @{ Table = "fact_snapshot_applications"; Columns = @(
            @{ Name = "snapshot_run_id"; Source = "snapshot_run_id"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_date"; Source = "snapshot_date"; DataType = "DateTime"; SummarizeBy = "None" },
            @{ Name = "snapshot_year_month"; Source = "snapshot_year_month"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "total_applications"; Source = "total_applications"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "cumulative_total_applications"; Source = "cumulative_total_applications"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "applications_with_resume"; Source = "applications_with_resume"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "cumulative_applications_with_resume"; Source = "cumulative_applications_with_resume"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "applications_with_questionnaire"; Source = "applications_with_questionnaire"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "cumulative_applications_with_questionnaire"; Source = "cumulative_applications_with_questionnaire"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "applications_with_resume_pct"; Source = "applications_with_resume_pct"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "applications_with_questionnaire_pct"; Source = "applications_with_questionnaire_pct"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "job_family_count"; Source = "job_family_count"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "applications_mom_delta"; Source = "applications_mom_delta"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "applications_mom_pct"; Source = "applications_mom_pct"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "is_simulated_snapshot"; Source = "is_simulated_snapshot"; DataType = "Boolean"; SummarizeBy = "None" },
            @{ Name = "snapshot_method"; Source = "snapshot_method"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_source"; Source = "snapshot_source"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_created_at"; Source = "snapshot_created_at"; DataType = "DateTime"; SummarizeBy = "None" }
        )},
        @{ Table = "fact_snapshot_presence"; Columns = @(
            @{ Name = "snapshot_run_id"; Source = "snapshot_run_id"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_date"; Source = "snapshot_date"; DataType = "DateTime"; SummarizeBy = "None" },
            @{ Name = "snapshot_year_month"; Source = "snapshot_year_month"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "monthly_events"; Source = "monthly_events"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "monthly_events_with_url"; Source = "monthly_events_with_url"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "events_with_url_pct"; Source = "events_with_url_pct"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "monthly_invitations"; Source = "monthly_invitations"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "monthly_invitations_with_message"; Source = "monthly_invitations_with_message"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "invitations_with_message_pct"; Source = "invitations_with_message_pct"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "monthly_recommendations"; Source = "monthly_recommendations"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "average_recommendation_text_length"; Source = "average_recommendation_text_length"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "mentions_data_count"; Source = "mentions_data_count"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "recommendations_mention_data_pct"; Source = "recommendations_mention_data_pct"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "cumulative_events"; Source = "cumulative_events"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "cumulative_invitations"; Source = "cumulative_invitations"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "cumulative_recommendations"; Source = "cumulative_recommendations"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "presence_score"; Source = "presence_score"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "engagement_score"; Source = "engagement_score"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "is_simulated_snapshot"; Source = "is_simulated_snapshot"; DataType = "Boolean"; SummarizeBy = "None" },
            @{ Name = "snapshot_method"; Source = "snapshot_method"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_source"; Source = "snapshot_source"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_created_at"; Source = "snapshot_created_at"; DataType = "DateTime"; SummarizeBy = "None" }
        )},
        @{ Table = "fact_snapshot_career_education"; Columns = @(
            @{ Name = "snapshot_run_id"; Source = "snapshot_run_id"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_date"; Source = "snapshot_date"; DataType = "DateTime"; SummarizeBy = "None" },
            @{ Name = "snapshot_year_month"; Source = "snapshot_year_month"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "positions_started_in_month"; Source = "positions_started_in_month"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "positions_started_cumulative"; Source = "positions_started_cumulative"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "current_positions_started_in_month"; Source = "current_positions_started_in_month"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "current_positions_cumulative"; Source = "current_positions_cumulative"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "avg_position_duration_months"; Source = "avg_position_duration_months"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "avg_position_duration_cumulative"; Source = "avg_position_duration_cumulative"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "education_started_in_month"; Source = "education_started_in_month"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "education_started_cumulative"; Source = "education_started_cumulative"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "current_education_started_in_month"; Source = "current_education_started_in_month"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "current_education_cumulative"; Source = "current_education_cumulative"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "avg_education_duration_months"; Source = "avg_education_duration_months"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "avg_education_duration_cumulative"; Source = "avg_education_duration_cumulative"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "certifications_started_in_month"; Source = "certifications_started_in_month"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "certifications_cumulative"; Source = "certifications_cumulative"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "avg_certification_duration_months"; Source = "avg_certification_duration_months"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "avg_certification_duration_cumulative"; Source = "avg_certification_duration_cumulative"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "career_maturity_score"; Source = "career_maturity_score"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "is_simulated_snapshot"; Source = "is_simulated_snapshot"; DataType = "Boolean"; SummarizeBy = "None" },
            @{ Name = "snapshot_method"; Source = "snapshot_method"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_source"; Source = "snapshot_source"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_created_at"; Source = "snapshot_created_at"; DataType = "DateTime"; SummarizeBy = "None" }
        )},
        @{ Table = "fact_snapshot_data_quality"; Columns = @(
            @{ Name = "snapshot_run_id"; Source = "snapshot_run_id"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_date"; Source = "snapshot_date"; DataType = "DateTime"; SummarizeBy = "None" },
            @{ Name = "snapshot_year_month"; Source = "snapshot_year_month"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "table_key"; Source = "table_key"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "export_type"; Source = "export_type"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "source_row_count"; Source = "source_row_count"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "row_count_after_transform"; Source = "row_count_after_transform"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "rows_removed_during_transform"; Source = "rows_removed_during_transform"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "duplicate_rows_after_transform"; Source = "duplicate_rows_after_transform"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "row_retention_rate"; Source = "row_retention_rate"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "health_status"; Source = "health_status"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "duplicate_alert_flag"; Source = "duplicate_alert_flag"; DataType = "Boolean"; SummarizeBy = "None" },
            @{ Name = "row_removal_alert_flag"; Source = "row_removal_alert_flag"; DataType = "Boolean"; SummarizeBy = "None" },
            @{ Name = "successful_load_flag"; Source = "successful_load_flag"; DataType = "Int64"; SummarizeBy = "Sum" },
            @{ Name = "monitored_columns_count"; Source = "monitored_columns_count"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "avg_null_rate_before_transform"; Source = "avg_null_rate_before_transform"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "avg_null_rate_after_transform"; Source = "avg_null_rate_after_transform"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "avg_null_rate_delta"; Source = "avg_null_rate_delta"; DataType = "Double"; SummarizeBy = "Average" },
            @{ Name = "null_rate_alert_count"; Source = "null_rate_alert_count"; DataType = "Double"; SummarizeBy = "Sum" },
            @{ Name = "is_simulated_snapshot"; Source = "is_simulated_snapshot"; DataType = "Boolean"; SummarizeBy = "None" },
            @{ Name = "snapshot_method"; Source = "snapshot_method"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_source"; Source = "snapshot_source"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "snapshot_created_at"; Source = "snapshot_created_at"; DataType = "DateTime"; SummarizeBy = "None" }
        )},
        @{ Table = "dim_snapshot_method"; Columns = @(
            @{ Name = "snapshot_method"; Source = "snapshot_method"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "method_group"; Source = "method_group"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "method_description"; Source = "method_description"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "is_simulated"; Source = "is_simulated"; DataType = "Boolean"; SummarizeBy = "None" }
        )},
        @{ Table = "dim_snapshot_run"; Columns = @(
            @{ Name = "snapshot_run_id"; Source = "snapshot_run_id"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "run_started_at"; Source = "run_started_at"; DataType = "DateTime"; SummarizeBy = "None" },
            @{ Name = "run_finished_at"; Source = "run_finished_at"; DataType = "DateTime"; SummarizeBy = "None" },
            @{ Name = "phase_name"; Source = "phase_name"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "contains_simulated_data"; Source = "contains_simulated_data"; DataType = "Boolean"; SummarizeBy = "None" },
            @{ Name = "source_warehouse_path"; Source = "source_warehouse_path"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "project_git_commit"; Source = "project_git_commit"; DataType = "String"; SummarizeBy = "None" },
            @{ Name = "notes"; Source = "notes"; DataType = "String"; SummarizeBy = "None" }
        )}
    )

    foreach ($tableDefinition in $schemaDefinitions) {
        foreach ($columnDefinition in $tableDefinition.Columns) {
            Ensure-DataColumn -Context $context -TableName $tableDefinition.Table -ColumnName $columnDefinition.Name -SourceColumn $columnDefinition.Source -DataType $columnDefinition.DataType -SummarizeBy $columnDefinition.SummarizeBy
        }
    }
    Save-Model -Context $context

    foreach ($tableName in @(
        "fact_snapshot_network_growth",
        "fact_snapshot_applications",
        "fact_snapshot_presence",
        "fact_snapshot_career_education",
        "fact_snapshot_data_quality",
        "dim_snapshot_method",
        "dim_snapshot_run"
    )) {
        Refresh-TableFull -Context $context -TableName $tableName
    }

    $columnMetadata = @(
        @{ Table = "dim_snapshot_method"; Column = "snapshot_method"; Description = "Método de geração do snapshot histórico."; Hidden = $false },
        @{ Table = "dim_snapshot_method"; Column = "method_group"; Description = "Grupo de governança associado ao método de snapshot."; Hidden = $false },
        @{ Table = "dim_snapshot_method"; Column = "method_description"; Description = "Descrição do método usado na geração do snapshot."; Hidden = $false },
        @{ Table = "dim_snapshot_method"; Column = "is_simulated"; Description = "Indicador se o método produz snapshots simulados."; Hidden = $false },
        @{ Table = "dim_snapshot_run"; Column = "snapshot_run_id"; Description = "Identificador único da execução de geração de snapshots."; Hidden = $false },
        @{ Table = "dim_snapshot_run"; Column = "run_started_at"; Description = "Timestamp UTC de início da execução de geração de snapshots."; Hidden = $false },
        @{ Table = "dim_snapshot_run"; Column = "run_finished_at"; Description = "Timestamp UTC de término da execução de geração de snapshots."; Hidden = $false },
        @{ Table = "dim_snapshot_run"; Column = "phase_name"; Description = "Fase metodológica executada na geração de snapshots."; Hidden = $false },
        @{ Table = "dim_snapshot_run"; Column = "contains_simulated_data"; Description = "Indicador se a execução contém dados simulados."; Hidden = $false },
        @{ Table = "dim_snapshot_run"; Column = "source_warehouse_path"; Description = "Caminho do warehouse DuckDB usado na execução."; Hidden = $true },
        @{ Table = "dim_snapshot_run"; Column = "project_git_commit"; Description = "Hash Git do projeto no momento da geração do snapshot."; Hidden = $true },
        @{ Table = "dim_snapshot_run"; Column = "notes"; Description = "Notas metodológicas registradas na execução do snapshot."; Hidden = $false }
    )

    foreach ($factTable in @("fact_snapshot_network_growth","fact_snapshot_applications","fact_snapshot_presence","fact_snapshot_career_education","fact_snapshot_data_quality")) {
        $columnMetadata += @(
            @{ Table = $factTable; Column = "snapshot_run_id"; Description = "Chave da execução de geração do snapshot histórico."; Hidden = $true },
            @{ Table = $factTable; Column = "snapshot_date"; Description = "Data do snapshot histórico usada no relacionamento com o calendário."; Hidden = $true },
            @{ Table = $factTable; Column = "snapshot_year_month"; Description = "Rótulo ano-mês do snapshot histórico."; Hidden = $true },
            @{ Table = $factTable; Column = "is_simulated_snapshot"; Description = "Indicador se a linha do snapshot histórico foi simulada."; Hidden = $true },
            @{ Table = $factTable; Column = "snapshot_method"; Description = "Método de geração da linha do snapshot histórico."; Hidden = $true },
            @{ Table = $factTable; Column = "snapshot_source"; Description = "Origem física usada para construir a linha do snapshot histórico."; Hidden = $true },
            @{ Table = $factTable; Column = "snapshot_created_at"; Description = "Timestamp UTC em que a linha do snapshot histórico foi criada."; Hidden = $true }
        )
    }
    $columnMetadata += @(
        @{ Table = "fact_snapshot_data_quality"; Column = "table_key"; Description = "Tabela monitorada na linha histórica de data quality."; Hidden = $false },
        @{ Table = "fact_snapshot_data_quality"; Column = "export_type"; Description = "Tipo de exportação monitorado na linha histórica de data quality."; Hidden = $false },
        @{ Table = "fact_snapshot_data_quality"; Column = "health_status"; Description = "Status semântico de saúde da carga monitorada."; Hidden = $false }
    )

    foreach ($meta in $columnMetadata) {
        Ensure-ColumnMetadata -Context $context -TableName $meta.Table -ColumnName $meta.Column -Description $meta.Description -Hidden $meta.Hidden
    }
    Save-Model -Context $context

    foreach ($factTable in @("fact_snapshot_network_growth","fact_snapshot_applications","fact_snapshot_presence","fact_snapshot_career_education","fact_snapshot_data_quality")) {
        Ensure-Relationship -Context $context -FromTable $factTable -FromColumn "snapshot_date" -ToTable "dCalendario" -ToColumn "date"
        Ensure-Relationship -Context $context -FromTable $factTable -FromColumn "snapshot_method" -ToTable "dim_snapshot_method" -ToColumn "snapshot_method"
        Ensure-Relationship -Context $context -FromTable $factTable -FromColumn "snapshot_run_id" -ToTable "dim_snapshot_run" -ToColumn "snapshot_run_id"
    }
    Save-Model -Context $context

    foreach ($measureDefinition in $measureDefinitions) {
        Ensure-Measure -Context $context -Name $measureDefinition.Name -Expression $measureDefinition.Expression -Description $measureDefinition.Description -FormatString $measureDefinition.FormatString -DisplayFolder $measureDefinition.DisplayFolder
    }
    Save-Model -Context $context
    Refresh-ModelCalculate -Context $context

    $measureValidation = @()
    foreach ($measureDefinition in $measureDefinitions) {
        $measure = $context.Model.Tables.Find("_Measures").Measures.Find($measureDefinition.Name)
        $measureValidation += [pscustomobject]@{
            Name = $measure.Name
            State = [string]$measure.State
            ErrorMessage = $measure.ErrorMessage
        }
    }

    $rowCounts = [ordered]@{}
    foreach ($tableName in @(
        "fact_snapshot_network_growth",
        "fact_snapshot_applications",
        "fact_snapshot_presence",
        "fact_snapshot_career_education",
        "fact_snapshot_data_quality",
        "dim_snapshot_method",
        "dim_snapshot_run"
    )) {
        $rowCounts[$tableName] = Invoke-DaxRowCount -ServerName $DataSource -DatabaseName $Catalog -TableName $tableName
    }

    $relationships = foreach ($relationship in $context.Model.Relationships) {
        if ($relationship -is [Microsoft.AnalysisServices.Tabular.SingleColumnRelationship]) {
            $fromTable = $relationship.FromColumn.Table.Name
            if ($fromTable -like "fact_snapshot_*") {
                [pscustomobject]@{
                    From = "$($relationship.FromColumn.Table.Name)[$($relationship.FromColumn.Name)]"
                    To = "$($relationship.ToColumn.Table.Name)[$($relationship.ToColumn.Name)]"
                    CrossFilter = [string]$relationship.CrossFilteringBehavior
                    Active = $relationship.IsActive
                }
            }
        }
    }

    $summary = [ordered]@{
        DataSource = $DataSource
        Catalog = $Catalog
        Tables = $rowCounts
        Relationships = $relationships
        Measures = $measureValidation
        Changes = $script:Changes
    }

    $summary | ConvertTo-Json -Depth 6
}
finally {
    $context.Server.Disconnect()
}
