param(
    [Parameter(Mandatory)] [string] $RepositoryConnectionString
)

. "$PSScriptRoot\ConfigHistory.Common.ps1"

$collectedAt = Get-ConfigHistoryUtcNow
$machine = $env:COMPUTERNAME

$osColumns = @('collected_at_utc','machine_name','caption','version','build_number','ubr','installation_type','edition_id','product_name','display_version','release_id','os_architecture','install_date','last_boot_up_time','power_plan_name','power_plan_guid','time_zone','collector_version')
$privColumns = @('collected_at_utc','machine_name','privilege_name','privilege_label','assigned_to_raw','collector_version')

$osDt = New-ConfigHistoryDataTable -TableName 'OperatingSystemInfo' -Columns $osColumns
$privDt = New-ConfigHistoryDataTable -TableName 'OperatingSystemPrivilege' -Columns $privColumns

$os = Get-CimInstance Win32_OperatingSystem
$cv = Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion'
$power = (powercfg /GETACTIVESCHEME) -join ' '
$powerGuid = $null
$powerName = $null
if ($power -match '([a-f0-9\-]{36})\s+\((.+)\)') {
    $powerGuid = $matches[1]
    $powerName = $matches[2]
}

Add-ConfigHistoryRow -Table $osDt -Values @{
    collected_at_utc = $collectedAt
    machine_name = $machine
    caption = $os.Caption
    version = $os.Version
    build_number = $os.BuildNumber
    ubr = $cv.UBR
    installation_type = $cv.InstallationType
    edition_id = $cv.EditionID
    product_name = $cv.ProductName
    display_version = $cv.DisplayVersion
    release_id = $cv.ReleaseId
    os_architecture = $os.OSArchitecture
    install_date = $os.InstallDate
    last_boot_up_time = $os.LastBootUpTime
    power_plan_name = $powerName
    power_plan_guid = $powerGuid
    time_zone = (Get-TimeZone).Id
    collector_version = '1.0'
}

# Local security policy export. Requires local admin for full reliability.
$tempInf = Join-Path $env:TEMP "secpol_$([guid]::NewGuid().ToString('N')).inf"
try {
    secedit.exe /export /cfg $tempInf /quiet | Out-Null
    $content = Get-Content -Path $tempInf -ErrorAction Stop
    $wanted = @{
        'SeLockMemoryPrivilege' = 'Lock pages in memory'
        'SeManageVolumePrivilege' = 'Perform volume maintenance tasks / Instant File Initialization'
    }

    foreach ($privilege in $wanted.Keys) {
        $line = $content | Where-Object { $_ -like "$privilege*" } | Select-Object -First 1
        $assigned = $null
        if ($line -match '^([^=]+)=(.*)$') { $assigned = $matches[2].Trim() }
        Add-ConfigHistoryRow -Table $privDt -Values @{
            collected_at_utc = $collectedAt
            machine_name = $machine
            privilege_name = $privilege
            privilege_label = $wanted[$privilege]
            assigned_to_raw = $assigned
            collector_version = '1.0'
        }
    }
}
finally {
    Remove-Item -Path $tempInf -ErrorAction SilentlyContinue
}

Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $osDt -DestinationTable 'confighistory.OperatingSystemInfo'
Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $privDt -DestinationTable 'confighistory.OperatingSystemPrivilege'
