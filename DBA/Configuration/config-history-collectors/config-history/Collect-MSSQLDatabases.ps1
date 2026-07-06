param(
    [Parameter(Mandatory)] [string] $RepositoryConnectionString,
    [Parameter(Mandatory)] [string[]] $SqlInstance,
    [string] $SqlAuthenticationConnectionStringTemplate
)

. "$PSScriptRoot\ConfigHistory.Common.ps1"

$collectedAt = Get-ConfigHistoryUtcNow
$machine = $env:COMPUTERNAME

$dbColumns = @('collected_at_utc','machine_name','sql_instance_name','database_id','database_name','create_date','compatibility_level','collation_name','user_access_desc','state_desc','recovery_model_desc','page_verify_option_desc','is_read_committed_snapshot_on','snapshot_isolation_state_desc','is_auto_create_stats_on','is_auto_update_stats_on','is_auto_update_stats_async_on','is_query_store_on','target_recovery_time_in_seconds','delayed_durability_desc','is_encrypted','owner_sid','collector_version')
$fileColumns = @('collected_at_utc','machine_name','sql_instance_name','database_id','database_name','file_id','type_desc','logical_name','physical_name','state_desc','size_mb','max_size_mb','growth_desc','is_percent_growth','collector_version')

$dbDt = New-ConfigHistoryDataTable -TableName 'MssqlDatabase' -Columns $dbColumns
$fileDt = New-ConfigHistoryDataTable -TableName 'MssqlDatabaseFile' -Columns $fileColumns

function Get-InstanceConnectionString {
    param([string] $InstanceName)
    if (-not [string]::IsNullOrWhiteSpace($SqlAuthenticationConnectionStringTemplate)) {
        return ($SqlAuthenticationConnectionStringTemplate -replace '\{instance\}', $InstanceName)
    }
    return "Server=$InstanceName;Database=master;Integrated Security=True;TrustServerCertificate=True;Application Name=ConfigHistoryCollector;"
}

$dbQuery = @"
SELECT
    database_id,
    name AS database_name,
    create_date,
    compatibility_level,
    collation_name,
    user_access_desc,
    state_desc,
    recovery_model_desc,
    page_verify_option_desc,
    is_read_committed_snapshot_on,
    snapshot_isolation_state_desc,
    is_auto_create_stats_on,
    is_auto_update_stats_on,
    is_auto_update_stats_async_on,
    is_query_store_on,
    target_recovery_time_in_seconds,
    delayed_durability_desc,
    is_encrypted,
    owner_sid
FROM sys.databases;
"@

$fileQuery = @"
SELECT
    mf.database_id,
    DB_NAME(mf.database_id) AS database_name,
    mf.file_id,
    mf.type_desc,
    mf.name AS logical_name,
    mf.physical_name,
    mf.state_desc,
    CAST(mf.size / 128.0 AS decimal(19,2)) AS size_mb,
    CAST(CASE WHEN mf.max_size = -1 THEN -1 ELSE mf.max_size / 128.0 END AS decimal(19,2)) AS max_size_mb,
    CASE WHEN mf.is_percent_growth = 1 THEN CONCAT(mf.growth, '%') ELSE CONCAT(CAST(mf.growth / 128.0 AS decimal(19,2)), ' MB') END AS growth_desc,
    mf.is_percent_growth
FROM sys.master_files AS mf;
"@

foreach ($instance in $SqlInstance) {
    $cs = Get-InstanceConnectionString -InstanceName $instance

    $dbs = Invoke-ConfigHistorySqlQuery -ConnectionString $cs -Query $dbQuery
    foreach ($r in $dbs.Rows) {
        Add-ConfigHistoryRow -Table $dbDt -Values @{
            collected_at_utc = $collectedAt
            machine_name = $machine
            sql_instance_name = $instance
            database_id = $r.database_id
            database_name = $r.database_name
            create_date = $r.create_date
            compatibility_level = $r.compatibility_level
            collation_name = $r.collation_name
            user_access_desc = $r.user_access_desc
            state_desc = $r.state_desc
            recovery_model_desc = $r.recovery_model_desc
            page_verify_option_desc = $r.page_verify_option_desc
            is_read_committed_snapshot_on = $r.is_read_committed_snapshot_on
            snapshot_isolation_state_desc = $r.snapshot_isolation_state_desc
            is_auto_create_stats_on = $r.is_auto_create_stats_on
            is_auto_update_stats_on = $r.is_auto_update_stats_on
            is_auto_update_stats_async_on = $r.is_auto_update_stats_async_on
            is_query_store_on = $r.is_query_store_on
            target_recovery_time_in_seconds = $r.target_recovery_time_in_seconds
            delayed_durability_desc = $r.delayed_durability_desc
            is_encrypted = $r.is_encrypted
            owner_sid = $r.owner_sid
            collector_version = '1.0'
        }
    }

    $files = Invoke-ConfigHistorySqlQuery -ConnectionString $cs -Query $fileQuery
    foreach ($r in $files.Rows) {
        Add-ConfigHistoryRow -Table $fileDt -Values @{
            collected_at_utc = $collectedAt
            machine_name = $machine
            sql_instance_name = $instance
            database_id = $r.database_id
            database_name = $r.database_name
            file_id = $r.file_id
            type_desc = $r.type_desc
            logical_name = $r.logical_name
            physical_name = $r.physical_name
            state_desc = $r.state_desc
            size_mb = $r.size_mb
            max_size_mb = $r.max_size_mb
            growth_desc = $r.growth_desc
            is_percent_growth = $r.is_percent_growth
            collector_version = '1.0'
        }
    }
}

Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $dbDt -DestinationTable 'confighistory.MssqlDatabase'
Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $fileDt -DestinationTable 'confighistory.MssqlDatabaseFile'
