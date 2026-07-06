param(
    [Parameter(Mandatory)] [string] $RepositoryConnectionString,
    [Parameter(Mandatory)] [string[]] $SqlInstance,
    [string] $SqlAuthenticationConnectionStringTemplate,
    [switch] $SkipHardware,
    [switch] $SkipOperatingSystem,
    [switch] $SkipDefender,
    [switch] $SkipSqlInstance,
    [switch] $SkipSqlDatabase
)

$ErrorActionPreference = 'Stop'

function Write-StepLog {
    param(
        [string] $StepName,
        [string] $Status,
        [int] $Rows = 0
    )
    $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $color = switch ($Status) {
        'OK'      { 'Green' }
        'KO'      { 'Red' }
        'SKIPPED' { 'Yellow' }
        default   { 'White' }
    }
    Write-Host "$ts - " -NoNewline
    Write-Host "[$StepName]" -NoNewline
    Write-Host " - " -NoNewline
    Write-Host "$Status" -ForegroundColor $color -NoNewline
    Write-Host " - $Rows rows"
}

function Write-StepError {
    param(
        [string] $StepName,
        [string] $ErrorMessage
    )
    $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    Write-Host "$ts - " -NoNewline
    Write-Host "[$StepName]" -NoNewline
    Write-Host " - " -NoNewline
    Write-Host "KO" -ForegroundColor Red -NoNewline
    Write-Host " - $ErrorMessage"
}

$localSteps = @(
    @{ Name = 'Hardware';        Skip = $SkipHardware;        Script = "$PSScriptRoot\Collect-Hardware.ps1";        Args = @{ RepositoryConnectionString = $RepositoryConnectionString } }
    @{ Name = 'OperatingSystem'; Skip = $SkipOperatingSystem; Script = "$PSScriptRoot\Collect-OperatingSystem.ps1"; Args = @{ RepositoryConnectionString = $RepositoryConnectionString } }
    @{ Name = 'Defender';        Skip = $SkipDefender;        Script = "$PSScriptRoot\Collect-Defender.ps1";        Args = @{ RepositoryConnectionString = $RepositoryConnectionString } }
)

$instanceSteps = @(
    @{ Name = 'SqlInstance'; Skip = $SkipSqlInstance; Script = "$PSScriptRoot\Collect-MSSQLInstances.ps1" }
    @{ Name = 'SqlDatabase'; Skip = $SkipSqlDatabase; Script = "$PSScriptRoot\Collect-MSSQLDatabases.ps1" }
)

$hasError = $false

# Local (machine-level) collectors
foreach ($step in $localSteps) {
    if ($step.Skip) {
        Write-StepLog -StepName $step.Name -Status 'SKIPPED'
        continue
    }
    try {
        $splat = $step.Args
        $output = @(& $step.Script @splat)
        $rows = ($output | Where-Object { $_ -is [int] } | Measure-Object -Sum).Sum
        Write-StepLog -StepName $step.Name -Status 'OK' -Rows ([int]$rows)
    }
    catch {
        if ($_.Exception.Message -like 'DEFENDER_NOT_AVAILABLE*') {
            Write-StepLog -StepName $step.Name -Status 'SKIPPED'
        }
        else {
            $hasError = $true
            Write-StepError -StepName $step.Name -ErrorMessage $_.Exception.Message
        }
    }
}

# Per-instance SQL collectors
foreach ($step in $instanceSteps) {
    if ($step.Skip) {
        Write-StepLog -StepName $step.Name -Status 'SKIPPED'
        continue
    }
    foreach ($inst in $SqlInstance) {
        $label = "$($step.Name) - $inst"
        try {
            $splat = @{
                RepositoryConnectionString = $RepositoryConnectionString
                SqlInstance = @($inst)
                SqlAuthenticationConnectionStringTemplate = $SqlAuthenticationConnectionStringTemplate
            }
            $output = @(& $step.Script @splat)
            $rows = ($output | Where-Object { $_ -is [int] } | Measure-Object -Sum).Sum
            Write-StepLog -StepName $label -Status 'OK' -Rows ([int]$rows)
        }
        catch {
            $hasError = $true
            Write-StepError -StepName $label -ErrorMessage $_.Exception.Message
        }
    }
}

if ($hasError) {
    Write-Host "`nOne or more steps failed." -ForegroundColor Red
    exit 1
}
else {
    Write-Host "`nAll steps completed successfully." -ForegroundColor Green
}
