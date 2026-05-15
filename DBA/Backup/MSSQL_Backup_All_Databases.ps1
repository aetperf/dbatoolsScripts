<#
    .SYNOPSIS
        Simple Backup of all databases of a SQL Server instance to target Directory
        
    .DESCRIPTION
        Objective : Backup all databases (except tempdb and model) to a target Backup directory
        Uses dbatools Backup-DbaDatabase directly without loops or parallelism
        Creates multiple backup files per database for improved performance
        
        The script will create .bak or .trn files depending on backup type (Full, Diff, Log)
        Path structure: "${BackupDirectory}\servername\instancename\dbname\backuptype\servername_dbname_backuptype_timestamp.${BackupExtension}"
        
        Features:
        - Inclusion/exclusion of databases by exact name or SQL LIKE patterns (e.g. PROD%, %_CRITICAL)
        - Multi-file backups for improved performance
        - Automatic backup verification (can be disabled)
        - Compression and checksum enabled by default
        - Optional encryption support
        - Automatic cleanup of old backup files
        - WhatIf mode for preview
       
    .PARAMETER SqlInstance
        The SQL Server instance hosting the databases to be backed up.

    .PARAMETER BackupType
        The SQL Server backup type (Full, Diff, Log).

    .PARAMETER BackupDirectory
        Target root directory for backups

    .PARAMETER IncludeDatabases
        Array of database names or SQL LIKE patterns to include.
        If specified, ONLY databases matching these patterns will be backed up.
        Example: @("PROD%", "MyDB", "%_CRITICAL")

    .PARAMETER ExcludeDatabases
        Array of database names or SQL LIKE patterns to exclude.
        Example: @("MyDB", "%_NOBACKUP", "TEST%")

    .PARAMETER FileCount
        Number of files to split the backup (improves backup and restore performance)
        Default: 4

    .PARAMETER Timeout
        Timeout for the backup operation in seconds
        Default: 7200 seconds (2 hours)

    .PARAMETER NoVerify
        Skip backup integrity verification. By default, verification is always performed.

    .PARAMETER CleanupTime
        Retention period in hours. Backup files older than this will be deleted.
        0 means no cleanup (default).

    .PARAMETER CleanupWhen
        When to run cleanup: Before, After, or Both.
        Default: After

    .PARAMETER EncryptionAlgorithm
        Encryption algorithm for backup encryption: AES128, AES192, AES256, or TRIPLEDES.
        Requires EncryptionCertificate to be specified.

    .PARAMETER EncryptionCertificate
        Name of the certificate in the master database used for backup encryption.
        The certificate must already exist on the SQL Server instance.
        
    .PARAMETER LogDirectory
        Directory where a log file will be stored. 
        Log file is named with the instance name and timestamp.

    .NOTES
        Tags: DisasterRecovery, Backup, Restore
        Author: Pierre-Antoine Collet (based on Romain Ferraton's parallel script)
        Website: 
        Copyright: (c) 2026 licensed under MIT
        License: MIT https://opensource.org/licenses/MIT
        
        Dependencies : 
            Install-Module Logging
            Install-Module dbatools

        Compatibility : PowerShell 5.1+

    .LINK
        
    .EXAMPLE
        PS C:\> .\MSSQL_Backup_All_Databases.ps1 -SqlInstance MySQLServer -BackupType Full -BackupDirectory "S:\BACKUPS" -FileCount 4
        Performs full backups of all databases with 4 files per backup

    .EXAMPLE
        PS C:\> .\MSSQL_Backup_All_Databases.ps1 -SqlInstance MySQLServer -BackupType Diff -BackupDirectory "S:\BACKUPS" -ExcludeDatabases @("%_NOBACKUP","TEST%") -CleanupTime 72
        Performs differential backups excluding pattern-matched databases, then cleans up files older than 72 hours
    
    .EXAMPLE
        PS C:\> .\MSSQL_Backup_All_Databases.ps1 -SqlInstance MySQLServer -BackupType Log -BackupDirectory "S:\BACKUPS" -IncludeDatabases @("PROD%") -WhatIf
        Shows what would be backed up without executing
#>

param 
(
    [Parameter(Mandatory)] [string] $SqlInstance,
    [Parameter(Mandatory)] [ValidateSet('Full','Diff','Log')] [string] $BackupType,
    [Parameter(Mandatory)] [string] $BackupDirectory,
    [Parameter()] [string[]] $IncludeDatabases = @(),
    [Parameter()] [string[]] $ExcludeDatabases = @(),
    [Parameter()] [Int16] $FileCount = 4,
    [Parameter()] [Int32] $Timeout = 7200,
    [Parameter()] [switch] $NoVerify,
    [Parameter()] [Int32] $CleanupTime = 0,
    [Parameter()] [ValidateSet('Before','After','Both')] [string] $CleanupWhen = 'After',
    [Parameter()] [switch] $WhatIf,
    [Parameter()] [ValidateSet('AES128','AES192','AES256','TRIPLEDES')] [string] $EncryptionAlgorithm,
    [Parameter()] [string] $EncryptionCertificate,
    [Parameter()] [string] $LogLevel = "INFO",
    [Parameter()] [string] $LogDirectory
)

#region Functions

function Convert-SqlLikeToRegex {
    <#
    .SYNOPSIS
        Converts a SQL LIKE pattern (with % and _) to a PowerShell regex pattern
    #>
    param([string]$Pattern)
    $escaped = [regex]::Escape($Pattern)
    $escaped = $escaped -replace '%', '.*'
    $escaped = $escaped -replace '_', '.'
    return "^${escaped}$"
}

function Test-DatabaseMatchesPattern {
    <#
    .SYNOPSIS
        Tests if a database name matches any of the given patterns (exact name or SQL LIKE pattern)
    #>
    param(
        [string]$DatabaseName,
        [string[]]$Patterns
    )
    foreach ($pattern in $Patterns) {
        if ($pattern -match '[%_]') {
            $regex = Convert-SqlLikeToRegex -Pattern $pattern
            if ($DatabaseName -match $regex) { return $true }
        }
        else {
            if ($DatabaseName -ieq $pattern) { return $true }
        }
    }
    return $false
}

function Invoke-BackupCleanup {
    <#
    .SYNOPSIS
        Removes old backup files from the backup directory based on retention period.
    #>
    param(
        [string]$BackupDirectory,
        [string]$ComputerName,
        [string]$InstanceName,
        [int]$CleanupTime,
        [string[]]$Databases = @()
    )

    if ($CleanupTime -le 0) { return }
    if ($Databases.Count -eq 0) {
        Write-Log -Level INFO -Message "Cleanup: no databases specified, skipping"
        return
    }

    $instanceSubPath = Join-Path -Path $BackupDirectory -ChildPath (Join-Path -Path $ComputerName -ChildPath $InstanceName)
    $RetentionDate = (Get-Date).AddHours(-$CleanupTime)
    Write-Log -Level INFO -Message "Cleanup: removing backup files older than ${RetentionDate} from ${instanceSubPath} for $($Databases.Count) database(s)"

    if (-not (Test-Path $instanceSubPath)) {
        Write-Log -Level INFO -Message "Cleanup: instance directory ${instanceSubPath} does not exist, skipping"
        return
    }

    $deletedCount = 0
    foreach ($db in $Databases) {
        $dbPath = Join-Path -Path $instanceSubPath -ChildPath $db
        if (-not (Test-Path $dbPath)) { continue }
        $oldFiles = Get-ChildItem -Path $dbPath -Recurse -File -Include "*.bak","*.trn" |
            Where-Object { $_.LastWriteTime -lt $RetentionDate }
        foreach ($file in $oldFiles) {
            Write-Log -Level INFO -Message "Cleanup: deleting $($file.FullName)"
            Remove-Item -Path $file.FullName -Force
            $deletedCount++
        }
    }
    Write-Log -Level INFO -Message "Cleanup complete: ${deletedCount} file(s) deleted"
}

#endregion Functions

#region Initialization

# Setup logging
Set-LoggingDefaultLevel -Level $LogLevel
Add-LoggingTarget -Name Console -Configuration @{
    ColorMapping = @{
        DEBUG = 'Gray'
        INFO  = 'White'
        ERROR  = 'DarkRed'
    }
}

if ($PSBoundParameters.ContainsKey('LogDirectory'))
{   
    $TimestampLog = Get-Date -UFormat "%Y-%m-%dT%H%M%S"
    $InstanceNameSafe = $SqlInstance -replace '\\', '_'
    $null = New-Item -Path $LogDirectory -ItemType Directory -Force
    $LogfileName = "dbatools_backup_${InstanceNameSafe}_${BackupType}_${TimestampLog}.log"
    $LogFile = Join-Path -Path $LogDirectory -ChildPath $LogfileName
    Add-LoggingTarget -Name File -Configuration @{Level = 'INFO'; Path = $LogFile}
    Write-Log -Level INFO -Message "Log File : $LogFile"
}

# Log parameters
Write-Log -Level INFO -Message "Parameter SQLInstance : ${SqlInstance}"
Write-Log -Level INFO -Message "Parameter BackupType : ${BackupType}"
Write-Log -Level INFO -Message "Parameter BackupDirectory : ${BackupDirectory}"
Write-Log -Level INFO -Message "Parameter IncludeDatabases : $($IncludeDatabases -join ', ')"
Write-Log -Level INFO -Message "Parameter ExcludeDatabases : $($ExcludeDatabases -join ', ')"
Write-Log -Level INFO -Message "Parameter FileCount : ${FileCount}"
Write-Log -Level INFO -Message "Parameter Timeout : ${Timeout}"
Write-Log -Level INFO -Message "Parameter NoVerify : ${NoVerify}"
Write-Log -Level INFO -Message "Parameter CleanupTime : ${CleanupTime} hours"
Write-Log -Level INFO -Message "Parameter CleanupWhen : ${CleanupWhen}"
if ($EncryptionAlgorithm) {
    Write-Log -Level INFO -Message "Parameter EncryptionAlgorithm : ${EncryptionAlgorithm}"
    Write-Log -Level INFO -Message "Parameter EncryptionCertificate : ${EncryptionCertificate}"
}

# Validate encryption parameters
if ($EncryptionAlgorithm -and -not $EncryptionCertificate) {
    Write-Log -Level ERROR -Message "EncryptionAlgorithm requires EncryptionCertificate to be specified"
    exit 1
}
if ($EncryptionCertificate -and -not $EncryptionAlgorithm) {
    Write-Log -Level ERROR -Message "EncryptionCertificate requires EncryptionAlgorithm to be specified"
    exit 1
}

$null = Set-DbatoolsInsecureConnection -SessionOnly

#endregion Initialization

#region Database Selection

# System databases to exclude
$SystemExclusions = @("tempdb", "model")
if ($BackupType -eq "Diff") { $SystemExclusions += "master" }

# Separate exact names from LIKE patterns
$ExactExclusions = $SystemExclusions + ($ExcludeDatabases | Where-Object { $_ -notmatch '[%_]' })
$LikePatterns = @($ExcludeDatabases | Where-Object { $_ -match '[%_]' })
$IncludeExact = @($IncludeDatabases | Where-Object { $_ -notmatch '[%_]' })
$IncludeLikePatterns = @($IncludeDatabases | Where-Object { $_ -match '[%_]' })

# Get databases based on backup type
switch ($BackupType) {
    "Full" {
        $BackupExtension = "bak"  
        $Databases = Get-DbaDatabase -SqlInstance $SqlInstance -ExcludeDatabase $ExactExclusions | 
            Where-Object {($_.IsUpdateable) -and ($_.Status -ilike "Normal*")}
    }
    "Diff" {
        $BackupExtension = "bak" 
        $Databases = Get-DbaDatabase -SqlInstance $SqlInstance -ExcludeDatabase $ExactExclusions | 
            Where-Object {($_.IsUpdateable) -and ($_.Status -ilike "Normal*")}
    }
    "Log" {
        $BackupExtension = "trn"  
        $Databases = Get-DbaDatabase -SqlInstance $SqlInstance -ExcludeDatabase $ExactExclusions | 
            Where-Object { ($_.IsUpdateable) -and ($_.Status -ilike "Normal*") -and ($_.RecoveryModel -ne "Simple")}
    }
}

# Apply IncludeDatabases filter
if ($IncludeDatabases.Count -gt 0) {
    $beforeCount = $Databases.Count
    $Databases = $Databases | Where-Object {
        $dbName = $_.Name
        ($IncludeExact -icontains $dbName) -or
        ($IncludeLikePatterns.Count -gt 0 -and (Test-DatabaseMatchesPattern -DatabaseName $dbName -Patterns $IncludeLikePatterns))
    }
    $includedByFilter = $Databases.Count
    Write-Log -Level INFO -Message "Included ${includedByFilter} database(s) from ${beforeCount} by IncludeDatabases filter: $($IncludeDatabases -join ', ')"
}

# Apply SQL LIKE pattern exclusions
if ($LikePatterns.Count -gt 0) {
    $beforeCount = $Databases.Count
    $Databases = $Databases | Where-Object { -not (Test-DatabaseMatchesPattern -DatabaseName $_.Name -Patterns $LikePatterns) }
    $excludedByPattern = $beforeCount - $Databases.Count
    if ($excludedByPattern -gt 0) {
        Write-Log -Level INFO -Message "Excluded ${excludedByPattern} database(s) by LIKE pattern(s): $($LikePatterns -join ', ')"
    }
}

Write-Log -Level INFO -Message "BackupExtension : ${BackupExtension}"
Write-Log -Level INFO -Message "Databases to backup : $($Databases.Count) - $($Databases.Name -join ', ')"

#endregion Database Selection

#region WhatIf Mode

if ($WhatIf) {
    Write-Host ""
    Write-Host "=== WhatIf Mode - No action will be performed ===" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Instance        : $SqlInstance" -ForegroundColor White
    Write-Host "Backup Type     : $BackupType" -ForegroundColor White
    Write-Host "Backup Directory: $BackupDirectory" -ForegroundColor White
    Write-Host "File Extension  : .$BackupExtension" -ForegroundColor White
    Write-Host "FileCount       : $FileCount" -ForegroundColor White
    Write-Host "Verify          : $(-not $NoVerify)" -ForegroundColor White
    Write-Host "Timeout         : $Timeout seconds" -ForegroundColor White
    if ($EncryptionAlgorithm) {
        Write-Host "Encryption      : $EncryptionAlgorithm with certificate '$EncryptionCertificate'" -ForegroundColor White
    }
    Write-Host ""
    if ($IncludeDatabases.Count -gt 0) {
        Write-Host "Included patterns: $($IncludeDatabases -join ', ')" -ForegroundColor Green
    }
    if ($ExcludeDatabases.Count -gt 0) {
        Write-Host "Excluded patterns: $($ExcludeDatabases -join ', ')" -ForegroundColor Yellow
    }
    Write-Host ""
    Write-Host "Databases that would be backed up ($($Databases.Count)):" -ForegroundColor Green
    foreach ($db in ($Databases | Sort-Object -Property Size -Descending)) {
        $sizeGB = [math]::Round($db.Size / 1024, 2)
        Write-Host "  - $($db.Name) (Size: ${sizeGB} GB)" -ForegroundColor White
    }
    Write-Host ""
    if ($CleanupTime -gt 0) {
        $RetentionDate = (Get-Date).AddHours(-$CleanupTime)
        Write-Host "Cleanup         : *.bak and *.trn files older than ${RetentionDate} (${CleanupTime}h retention)" -ForegroundColor Yellow
        Write-Host "Cleanup When    : $CleanupWhen" -ForegroundColor Yellow
    } else {
        Write-Host "Cleanup         : Disabled" -ForegroundColor DarkGray
    }
    Write-Host ""
    Write-Host "=== End WhatIf ===" -ForegroundColor Cyan
    return
}

#endregion WhatIf Mode

#region Backup Execution

# Get actual server info for cleanup
$actualComputerName = ($Databases | Select-Object -First 1).ComputerName
$actualInstanceName = ($Databases | Select-Object -First 1).InstanceName

# Run pre-backup cleanup if configured
if ($CleanupWhen -in @('Before','Both') -and $CleanupTime -gt 0) {
    Write-Log -Level INFO -Message "Running pre-backup cleanup..."
    $DbNamesToClean = @($Databases | ForEach-Object { $_.Name })
    Invoke-BackupCleanup -BackupDirectory $BackupDirectory -ComputerName $actualComputerName `
        -InstanceName $actualInstanceName -CleanupTime $CleanupTime -Databases $DbNamesToClean
}

# Build backup parameters
$BackupParams = @{
    SqlInstance     = $SqlInstance
    Database        = @($Databases.Name)
    Type            = $BackupType
    CompressBackup  = $true
    Checksum        = $true
    FileCount       = $FileCount
    Path            = "${BackupDirectory}\servername\instancename\dbname\backuptype\"
    FilePath        = "servername_dbname_backuptype_timestamp.${BackupExtension}"
    TimeStampFormat = "yyyyMMdd_HHmm"
    ReplaceInName   = $true
    CreateFolder    = $true
    EnableException = $true
    NoAppendDbNameInPath = $true
}

if (-not $NoVerify) { 
    $BackupParams['Verify'] = $true 
}

if ($EncryptionAlgorithm) {
    $BackupParams['EncryptionAlgorithm'] = $EncryptionAlgorithm
    $BackupParams['EncryptionCertificate'] = $EncryptionCertificate
}

# Execute backup
Write-Log -Level INFO -Message "Starting backup of $($Databases.Count) database(s)..."
$duration = Measure-Command {
    try {
        $BackupResults = Backup-DbaDatabase @BackupParams -WarningVariable BackupWarnings
    }
    catch {
        Write-Log -Level ERROR -Message "Backup failed: $($_.Exception.Message)"
        if ($BackupWarnings) {
            foreach ($warning in $BackupWarnings) {
                Write-Log -Level ERROR -Message "Warning: $($warning.Message)"
            }
        }
        exit 2
    }
}

# Log results
$SuccessCount = 0
$FailureCount = 0
$FailedDatabases = @()

foreach ($result in $BackupResults) {
    if ($result.BackupComplete) {
        $SuccessCount++
        $Message = "ComputerName: $($result.ComputerName) | InstanceName: $($result.InstanceName) | Database: $($result.Database) | Type: $($result.Type) | Start: $($result.Start) | End: $($result.End) | Duration: $($result.Duration) | TotalSize: $($result.TotalSize) | CompressedBackupSize: $($result.CompressedBackupSize) | Verified: $($result.Verified) | BackupFilesCount: $($result.BackupFilesCount) | BackupPath: $($result.BackupPath)"
        Write-Log -Level INFO -Message $Message
    }
    else {
        $FailureCount++
        $FailedDatabases += $result.Database
        $Message = "Database: $($result.Database) | Error: Backup not completed"
        Write-Log -Level ERROR -Message $Message
    }
}

# Run post-backup cleanup (skip failed databases)
if ($CleanupWhen -in @('After','Both') -and $CleanupTime -gt 0) {
    Write-Log -Level INFO -Message "Running post-backup cleanup..."
    $DbNamesToClean = @($Databases | ForEach-Object { $_.Name } | Where-Object { $_ -notin $FailedDatabases })
    Invoke-BackupCleanup -BackupDirectory $BackupDirectory -ComputerName $actualComputerName `
        -InstanceName $actualInstanceName -CleanupTime $CleanupTime -Databases $DbNamesToClean
}

# Final status
if ($FailureCount -gt 0) {
    Write-Log -Level INFO -Message "${SuccessCount} ${BackupType} backups succeeded, ${FailureCount} failed in $([math]::Round($duration.TotalSeconds, 2)) seconds"
    Write-Log -Level ERROR -Message "${FailureCount} Database Backup(s) had a problem"
    exit 2
}

Write-Log -Level INFO -Message "${SuccessCount} ${BackupType} Backups SUCCESSFULLY COMPLETED in $([math]::Round($duration.TotalSeconds, 2)) seconds"

#endregion Backup Execution
