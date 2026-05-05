    <#
    .SYNOPSIS
        Purge old backup files for all databases of a SQL Server instance
    .DESCRIPTION
        Removes backup files older than a specified retention period for all databases of an instance.
        Only deletes files matching the specified backup type extension (.bak for Full/Diff, .trn for Log).
        Supports inclusion and exclusion of databases by exact name or SQL LIKE patterns (e.g. %_NOBACKUP, TEST%).
       
    .PARAMETER SqlInstance
        The SQL Server instance whose backup files should be purged.
        Used to identify the backup folder structure (servername\instancename).

    .PARAMETER BackupType
        The backup type to purge: Full, Diff, Log or All.
        - Full: purges *.bak files in Full folders
        - Diff: purges *.bak files in Diff folders
        - Log: purges *.trn files in Log folders
        - All: purges all backup types

    .PARAMETER BackupDirectory
        Root backup directory to scan for old files.

    .PARAMETER RetentionHours
        Retention period in hours. Files older than this will be deleted.

    .PARAMETER IncludeDatabases
        Array of database names or SQL LIKE patterns to include in purge.
        If specified, ONLY databases matching these patterns will be purged.
        Exact names (e.g. "MyDB") and patterns using % and _ wildcards are supported.
        Example: @("PROD%", "MyDB", "%_CRITICAL")

    .PARAMETER ExcludeDatabases
        Array of database names or SQL LIKE patterns to exclude from purge.
        Exact names (e.g. "MyDB") and patterns using % and _ wildcards are supported.
        Example: @("MyDB", "%_NOBACKUP", "TEST%")

    .PARAMETER WhatIf
        If specified, only displays what would be deleted without performing any action.

    .PARAMETER LogDirectory
        Directory where a log file will be stored.
        Log file is named with the instance name and timestamp.

    .NOTES
        Tags: DisasterRecovery, Backup, Maintenance
        Author: Romain Ferraton
        Website: 
        Copyright: (c) 2022 by Romain Ferraton, licensed under MIT
        License: MIT https://opensource.org/licenses/MIT
        
        Dependencies : 
            Install-Module Logging

        Compatibility : Powershell 7.3+

    .EXAMPLE
        PS C:\> .\MSSQL_Backup_Purge.ps1 -SqlInstance "MyServer\INST01" -BackupType Full -BackupDirectory "S:\BACKUPS" -RetentionHours 168
        Purges Full backup files older than 7 days (168 hours)

    .EXAMPLE
        PS C:\> .\MSSQL_Backup_Purge.ps1 -SqlInstance "MyServer" -BackupType All -BackupDirectory "S:\BACKUPS" -RetentionHours 72 -ExcludeDatabases @("%_CRITICAL", "ProductionDB") -WhatIf
        Shows what would be purged for all backup types, excluding databases matching patterns

    .EXAMPLE
        PS C:\> .\MSSQL_Backup_Purge.ps1 -SqlInstance "MyServer" -BackupType Log -BackupDirectory "S:\BACKUPS" -RetentionHours 24 -LogDirectory "D:\logs"
        Purges transaction log backups older than 24 hours and logs actions to file
    #>

    param 
    (
        [Parameter(Mandatory)] [string] $SqlInstance,
        [Parameter(Mandatory)] [ValidateSet('Full','Diff','Log','All')] [string] $BackupType,
        [Parameter(Mandatory)] [string] $BackupDirectory,
        [Parameter(Mandatory)] [Int32] $RetentionHours,
        [Parameter()] [string[]] $IncludeDatabases = @(),
        [Parameter()] [string[]] $ExcludeDatabases = @(),
        [Parameter()] [switch] $WhatIf,
        [Parameter()] [string] $LogLevel = "INFO",
        [Parameter()] [string] $LogDirectory
    )
    

    #region Functions

    function Convert-SqlLikeToRegex {
        param([string]$Pattern)
        $escaped = [regex]::Escape($Pattern)
        $escaped = $escaped -replace '%', '.*'
        $escaped = $escaped -replace '_', '.'
        return "^${escaped}$"
    }

    function Test-DatabaseMatchesPattern {
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

    #endregion Functions


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
        mkdir $LogDirectory -Force | Out-Null
        $LogfileName = "dbatools_purge_${InstanceNameSafe}_${BackupType}_${TimestampLog}.log"
        $LogFile = Join-Path -Path $LogDirectory -ChildPath $LogfileName
        Add-LoggingTarget -Name File -Configuration @{Level = 'INFO'; Path = $LogFile}
        Write-Log -Level INFO -Message "Log File : $LogFile"
    }

    Write-Log -Level INFO -Message "Parameter SQLInstance : ${SqlInstance}"
    Write-Log -Level INFO -Message "Parameter BackupType : ${BackupType}"
    Write-Log -Level INFO -Message "Parameter BackupDirectory : ${BackupDirectory}"
    Write-Log -Level INFO -Message "Parameter RetentionHours : ${RetentionHours}"
    Write-Log -Level INFO -Message "Parameter IncludeDatabases : $($IncludeDatabases -join ', ')"
    Write-Log -Level INFO -Message "Parameter ExcludeDatabases : $($ExcludeDatabases -join ', ')"

    # Determine which extensions and subfolder patterns to target
    $PurgeTargets = @()
    switch ($BackupType) {
        "Full" { $PurgeTargets += @{ Extension = "bak"; Subfolder = "Full" } }
        "Diff" { $PurgeTargets += @{ Extension = "bak"; Subfolder = "Diff" } }
        "Log"  { $PurgeTargets += @{ Extension = "trn"; Subfolder = "Log" } }
        "All"  { 
            $PurgeTargets += @{ Extension = "bak"; Subfolder = "Full" }
            $PurgeTargets += @{ Extension = "bak"; Subfolder = "Diff" }
            $PurgeTargets += @{ Extension = "trn"; Subfolder = "Log" }
        }
    }

    $RetentionDate = (Get-Date).AddHours(-$RetentionHours)
    Write-Log -Level INFO -Message "Retention date : ${RetentionDate} (files older than this will be purged)"

    if (-not (Test-Path $BackupDirectory)) {
        Write-Log -Level ERROR -Message "Backup directory ${BackupDirectory} does not exist"
        exit 1
    }

    $totalDeletedCount = 0
    $totalSkippedCount = 0
    $totalDeletedSizeMB = 0
    $hasError = $false

    foreach ($target in $PurgeTargets) {
        $ext = $target.Extension
        $subfolder = $target.Subfolder

        Write-Log -Level INFO -Message "--- Processing ${subfolder} backups (*.${ext}) ---"

        # Find files matching the extension
        $allFiles = Get-ChildItem -Path $BackupDirectory -Recurse -File -Filter "*.${ext}" |
            Where-Object { $_.LastWriteTime -lt $RetentionDate -and $_.FullName -ilike "*\${subfolder}\*" }

        if ($allFiles.Count -eq 0) {
            Write-Log -Level INFO -Message "No *.${ext} files older than ${RetentionDate} found in ${subfolder} folders"
            continue
        }

        # Group files by database (extract db name from path structure: ...\dbname\backuptype\...)
        $groupedFiles = $allFiles | Group-Object { 
            # Path structure: BackupDirectory\ComputerName\InstanceName\DatabaseName\BackupType\...
            $parts = $_.FullName.Replace($BackupDirectory, '').TrimStart('\').Split('\')
            if ($parts.Count -ge 3) { $parts[2] } else { "UNKNOWN" }
        }

        foreach ($group in $groupedFiles) {
            $dbName = $group.Name

            # Check inclusion filter
            if ($IncludeDatabases.Count -gt 0 -and -not (Test-DatabaseMatchesPattern -DatabaseName $dbName -Patterns $IncludeDatabases)) {
                $skippedSizeMB = [math]::Round(($group.Group | Measure-Object -Property Length -Sum).Sum / 1MB, 2)
                Write-Log -Level INFO -Message "SKIPPED (not included): ${dbName} - $($group.Count) file(s), ${skippedSizeMB} MB"
                $totalSkippedCount += $group.Count
                continue
            }

            # Check exclusion
            if ($ExcludeDatabases.Count -gt 0 -and (Test-DatabaseMatchesPattern -DatabaseName $dbName -Patterns $ExcludeDatabases)) {
                $skippedSizeMB = [math]::Round(($group.Group | Measure-Object -Property Length -Sum).Sum / 1MB, 2)
                Write-Log -Level INFO -Message "SKIPPED (excluded): ${dbName} - $($group.Count) file(s), ${skippedSizeMB} MB"
                $totalSkippedCount += $group.Count
                continue
            }

            $dbSizeMB = [math]::Round(($group.Group | Measure-Object -Property Length -Sum).Sum / 1MB, 2)

            if ($WhatIf) {
                Write-Host "  [WhatIf] Would delete: ${dbName} - $($group.Count) file(s), ${dbSizeMB} MB" -ForegroundColor Yellow
            }
            else {
                foreach ($file in $group.Group) {
                    try {
                        Remove-Item -Path $file.FullName -Force
                        $totalDeletedCount++
                        $totalDeletedSizeMB += $file.Length / 1MB
                    }
                    catch {
                        Write-Log -Level ERROR -Message "Failed to delete $($file.FullName): $($_.Exception.Message)"
                        $hasError = $true
                    }
                }
                Write-Log -Level INFO -Message "PURGED: ${dbName} - $($group.Count) file(s), ${dbSizeMB} MB"
            }
        }
    }

    # Summary
    $totalDeletedSizeMB = [math]::Round($totalDeletedSizeMB, 2)

    if ($WhatIf) {
        $wouldDeleteCount = ($allFiles | Where-Object { 
            $parts = $_.FullName.Replace($BackupDirectory, '').TrimStart('\').Split('\')
            $dbName = if ($parts.Count -ge 3) { $parts[2] } else { "UNKNOWN" }
            $included = ($IncludeDatabases.Count -eq 0) -or (Test-DatabaseMatchesPattern -DatabaseName $dbName -Patterns $IncludeDatabases)
            $excluded = ($ExcludeDatabases.Count -gt 0) -and (Test-DatabaseMatchesPattern -DatabaseName $dbName -Patterns $ExcludeDatabases)
            $included -and -not $excluded
        }).Count
        $wouldDeleteSizeMB = [math]::Round(($allFiles | Where-Object { 
            $parts = $_.FullName.Replace($BackupDirectory, '').TrimStart('\').Split('\')
            $dbName = if ($parts.Count -ge 3) { $parts[2] } else { "UNKNOWN" }
            $included = ($IncludeDatabases.Count -eq 0) -or (Test-DatabaseMatchesPattern -DatabaseName $dbName -Patterns $IncludeDatabases)
            $excluded = ($ExcludeDatabases.Count -gt 0) -and (Test-DatabaseMatchesPattern -DatabaseName $dbName -Patterns $ExcludeDatabases)
            $included -and -not $excluded
        } | Measure-Object -Property Length -Sum).Sum / 1MB, 2)
        Write-Host ""
        Write-Host "=== WhatIf Summary ===" -ForegroundColor Cyan
        Write-Host "Would delete : ${wouldDeleteCount} file(s), ${wouldDeleteSizeMB} MB" -ForegroundColor Yellow
        Write-Host "Would skip   : ${totalSkippedCount} file(s) (filtered by include/exclude database rules)" -ForegroundColor DarkGray
        Write-Host "=== No action performed ===" -ForegroundColor Cyan
    }
    else {
        Write-Log -Level INFO -Message "=== Purge Summary ==="
        Write-Log -Level INFO -Message "Deleted : ${totalDeletedCount} file(s), ${totalDeletedSizeMB} MB"
        Write-Log -Level INFO -Message "Skipped : ${totalSkippedCount} file(s) (filtered by include/exclude database rules)"

        if ($hasError) {
            Write-Log -Level ERROR -Message "Some files could not be deleted, check log for details"
            exit 2
        }
    }

    Write-Log -Level INFO -Message "Purge completed successfully"
    exit 0
