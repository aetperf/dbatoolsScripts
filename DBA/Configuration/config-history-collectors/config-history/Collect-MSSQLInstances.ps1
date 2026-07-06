param(
    [Parameter(Mandatory)] [string] $RepositoryConnectionString,
    [Parameter(Mandatory)] [string[]] $SqlInstance,
    [string] $SqlAuthenticationConnectionStringTemplate
)

. "$PSScriptRoot\ConfigHistory.Common.ps1"

$collectedAt = Get-ConfigHistoryUtcNow
$machine = $env:COMPUTERNAME

$configColumns = @('collected_at_utc','machine_name','sql_instance_name','sql_server_name','sql_machine_name','product_version','product_level','edition','engine_edition','configuration_id','name','value','value_in_use','minimum','maximum','is_dynamic','is_advanced','description','collector_version')
$rgColumns = @('collected_at_utc','machine_name','sql_instance_name','resource_pool_name','workload_group_name','pool_max_cpu_percent','pool_cap_cpu_percent','pool_min_cpu_percent','pool_max_memory_percent','group_importance','group_request_max_memory_grant_percent','group_request_max_cpu_time_sec','group_max_dop','group_group_max_requests','is_enabled','collector_version')
$tempdbColumns = @('collected_at_utc','machine_name','sql_instance_name','file_id','type_desc','logical_name','physical_name','size_mb','max_size_mb','growth_desc','is_percent_growth','state_desc','collector_version')

$configDt = New-ConfigHistoryDataTable -TableName 'MssqlInstanceConfiguration' -Columns $configColumns
$rgDt = New-ConfigHistoryDataTable -TableName 'MssqlResourceGovernor' -Columns $rgColumns
$tempdbDt = New-ConfigHistoryDataTable -TableName 'MssqlTempdbFile' -Columns $tempdbColumns

function Get-InstanceConnectionString {
    param([string] $InstanceName)
    if (-not [string]::IsNullOrWhiteSpace($SqlAuthenticationConnectionStringTemplate)) {
        return ($SqlAuthenticationConnectionStringTemplate -replace '\{instance\}', $InstanceName)
    }
    return "Server=$InstanceName;Database=master;Integrated Security=True;TrustServerCertificate=True;Application Name=ConfigHistoryCollector;"
}

$configQuery = @"
SELECT
    CAST(SERVERPROPERTY('ServerName') AS nvarchar(256)) AS sql_server_name,
    CAST(SERVERPROPERTY('MachineName') AS nvarchar(256)) AS sql_machine_name,
    CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)) AS product_version,
    CAST(SERVERPROPERTY('ProductLevel') AS nvarchar(128)) AS product_level,
    CAST(SERVERPROPERTY('Edition') AS nvarchar(256)) AS edition,
    CAST(SERVERPROPERTY('EngineEdition') AS int) AS engine_edition,
    configuration_id,
    name,
    CONVERT(nvarchar(256), value) AS value,
    CONVERT(nvarchar(256), value_in_use) AS value_in_use,
    CONVERT(nvarchar(256), minimum) AS minimum,
    CONVERT(nvarchar(256), maximum) AS maximum,
    is_dynamic,
    is_advanced,
    description
FROM sys.configurations;
"@

$rgQuery = @"
SELECT
    rp.name AS resource_pool_name,
    wg.name AS workload_group_name,
    rp.max_cpu_percent AS pool_max_cpu_percent,
    rp.cap_cpu_percent AS pool_cap_cpu_percent,
    rp.min_cpu_percent AS pool_min_cpu_percent,
    rp.max_memory_percent AS pool_max_memory_percent,
    wg.importance AS group_importance,
    CAST(wg.request_max_memory_grant_percent AS decimal(9,4)) AS group_request_max_memory_grant_percent,
    wg.request_max_cpu_time_sec AS group_request_max_cpu_time_sec,
    wg.max_dop AS group_max_dop,
    wg.group_max_requests AS group_group_max_requests,
    CAST((SELECT CASE WHEN is_enabled = 1 THEN 1 ELSE 0 END FROM sys.resource_governor_configuration) AS bit) AS is_enabled
FROM sys.resource_governor_resource_pools AS rp
LEFT JOIN sys.resource_governor_workload_groups AS wg
    ON wg.pool_id = rp.pool_id;
"@

$tempdbQuery = @"
SELECT
    file_id,
    type_desc,
    name AS logical_name,
    physical_name,
    CAST(size / 128.0 AS decimal(19,2)) AS size_mb,
    CAST(CASE WHEN max_size = -1 THEN -1 ELSE max_size / 128.0 END AS decimal(19,2)) AS max_size_mb,
    CASE WHEN is_percent_growth = 1 THEN CONCAT(growth, '%') ELSE CONCAT(CAST(growth / 128.0 AS decimal(19,2)), ' MB') END AS growth_desc,
    is_percent_growth,
    state_desc
FROM tempdb.sys.database_files;
"@

foreach ($instance in $SqlInstance) {
    $cs = Get-InstanceConnectionString -InstanceName $instance

    $config = Invoke-ConfigHistorySqlQuery -ConnectionString $cs -Query $configQuery
    foreach ($r in $config.Rows) {
        Add-ConfigHistoryRow -Table $configDt -Values @{
            collected_at_utc = $collectedAt
            machine_name = $machine
            sql_instance_name = $instance
            sql_server_name = $r.sql_server_name
            sql_machine_name = $r.sql_machine_name
            product_version = $r.product_version
            product_level = $r.product_level
            edition = $r.edition
            engine_edition = $r.engine_edition
            configuration_id = $r.configuration_id
            name = $r.name
            value = $r.value
            value_in_use = $r.value_in_use
            minimum = $r.minimum
            maximum = $r.maximum
            is_dynamic = $r.is_dynamic
            is_advanced = $r.is_advanced
            description = $r.description
            collector_version = '1.0'
        }
    }

    $rg = Invoke-ConfigHistorySqlQuery -ConnectionString $cs -Query $rgQuery
    foreach ($r in $rg.Rows) {
        Add-ConfigHistoryRow -Table $rgDt -Values @{
            collected_at_utc = $collectedAt
            machine_name = $machine
            sql_instance_name = $instance
            resource_pool_name = $r.resource_pool_name
            workload_group_name = $r.workload_group_name
            pool_max_cpu_percent = $r.pool_max_cpu_percent
            pool_cap_cpu_percent = $r.pool_cap_cpu_percent
            pool_min_cpu_percent = $r.pool_min_cpu_percent
            pool_max_memory_percent = $r.pool_max_memory_percent
            group_importance = $r.group_importance
            group_request_max_memory_grant_percent = $r.group_request_max_memory_grant_percent
            group_request_max_cpu_time_sec = $r.group_request_max_cpu_time_sec
            group_max_dop = $r.group_max_dop
            group_group_max_requests = $r.group_group_max_requests
            is_enabled = $r.is_enabled
            collector_version = '1.0'
        }
    }

    $tempdb = Invoke-ConfigHistorySqlQuery -ConnectionString $cs -Query $tempdbQuery
    foreach ($r in $tempdb.Rows) {
        Add-ConfigHistoryRow -Table $tempdbDt -Values @{
            collected_at_utc = $collectedAt
            machine_name = $machine
            sql_instance_name = $instance
            file_id = $r.file_id
            type_desc = $r.type_desc
            logical_name = $r.logical_name
            physical_name = $r.physical_name
            size_mb = $r.size_mb
            max_size_mb = $r.max_size_mb
            growth_desc = $r.growth_desc
            is_percent_growth = $r.is_percent_growth
            state_desc = $r.state_desc
            collector_version = '1.0'
        }
    }
}

Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $configDt -DestinationTable 'confighistory.MssqlInstanceConfiguration'
Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $rgDt -DestinationTable 'confighistory.MssqlResourceGovernor'
Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $tempdbDt -DestinationTable 'confighistory.MssqlTempdbFile'
