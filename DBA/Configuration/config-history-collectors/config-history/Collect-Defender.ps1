param(
    [Parameter(Mandatory)] [string] $RepositoryConnectionString
)

. "$PSScriptRoot\ConfigHistory.Common.ps1"

$collectedAt = Get-ConfigHistoryUtcNow
$machine = $env:COMPUTERNAME

$statusColumns = @('collected_at_utc','machine_name','am_service_enabled','antivirus_enabled','antispyware_enabled','behavior_monitor_enabled','ioav_protection_enabled','nise_enabled','on_access_protection_enabled','real_time_protection_enabled','tamper_protection_enabled','antivirus_signature_version','antispyware_signature_version','nise_signature_version','quick_scan_age','full_scan_age','collector_version')
$exclColumns = @('collected_at_utc','machine_name','exclusion_type','exclusion_value','collector_version')

$statusDt = New-ConfigHistoryDataTable -TableName 'WindowsDefenderStatus' -Columns $statusColumns
$exclDt = New-ConfigHistoryDataTable -TableName 'WindowsDefenderExclusion' -Columns $exclColumns

$status = $null
$pref = $null
try {
    $status = Get-MpComputerStatus -ErrorAction Stop
    $pref = Get-MpPreference -ErrorAction Stop
}
catch {
    throw "DEFENDER_NOT_AVAILABLE: $_"
}

if ($null -eq $status) { throw 'DEFENDER_NOT_AVAILABLE: Windows Defender status not available on this machine.' }
if ($null -eq $pref) { throw 'DEFENDER_NOT_AVAILABLE: Windows Defender preferences not available on this machine.' }

Add-ConfigHistoryRow -Table $statusDt -Values @{
    collected_at_utc = $collectedAt
    machine_name = $machine
    am_service_enabled = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'AMServiceEnabled')
    antivirus_enabled = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'AntivirusEnabled')
    antispyware_enabled = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'AntispywareEnabled')
    behavior_monitor_enabled = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'BehaviorMonitorEnabled')
    ioav_protection_enabled = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'IoavProtectionEnabled')
    nise_enabled = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'NISEnabled')
    on_access_protection_enabled = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'OnAccessProtectionEnabled')
    real_time_protection_enabled = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'RealTimeProtectionEnabled')
    tamper_protection_enabled = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'IsTamperProtected')
    antivirus_signature_version = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'AntivirusSignatureVersion')
    antispyware_signature_version = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'AntispywareSignatureVersion')
    nise_signature_version = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'NISSignatureVersion')
    quick_scan_age = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'QuickScanAge')
    full_scan_age = (Get-ConfigHistoryPropertyValue -InputObject $status -PropertyName 'FullScanAge')
    collector_version = '1.0'
}

function Add-Exclusions {
    param([string] $Type, $Values)
    foreach ($v in @($Values)) {
        if ([string]::IsNullOrWhiteSpace([string]$v)) { continue }
        Add-ConfigHistoryRow -Table $exclDt -Values @{
            collected_at_utc = $collectedAt
            machine_name = $machine
            exclusion_type = $Type
            exclusion_value = [string]$v
            collector_version = '1.0'
        }
    }
}

Add-Exclusions -Type 'Path' -Values (Get-ConfigHistoryPropertyValue -InputObject $pref -PropertyName 'ExclusionPath')
Add-Exclusions -Type 'Process' -Values (Get-ConfigHistoryPropertyValue -InputObject $pref -PropertyName 'ExclusionProcess')
Add-Exclusions -Type 'Extension' -Values (Get-ConfigHistoryPropertyValue -InputObject $pref -PropertyName 'ExclusionExtension')
Add-Exclusions -Type 'IpAddress' -Values (Get-ConfigHistoryPropertyValue -InputObject $pref -PropertyName 'ExclusionIpAddress')

Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $statusDt -DestinationTable 'confighistory.WindowsDefenderStatus'
Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $exclDt -DestinationTable 'confighistory.WindowsDefenderExclusion'
