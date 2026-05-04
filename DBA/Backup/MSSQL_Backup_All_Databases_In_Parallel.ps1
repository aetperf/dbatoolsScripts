    <#
    .SYNOPSIS
        Parallel Backup of all databases of a single SQL Server SqlInstance to target Directory
    .DESCRIPTION
        Objective : Backup all databases (except tempdb and model) to a target Backup directory in parallel
        the script will create a .bak or .trn file depending of the backup type (full, diff, log)
        "${BackupDirectory}\servername\instancename\dbname\backuptype\servername_dbname_backuptype_timestamp.${BackupExtension}"
        
        Supports:
        - Inclusion of databases by exact name or SQL LIKE patterns (e.g. PROD%, %_CRITICAL)
        - Exclusion of databases by exact name or SQL LIKE patterns (e.g. %_NOBACKUP, TEST%)
        - Automatic cleanup of old backup files (only files matching the backup extension)
        - Cleanup can run Before, After or Both, and skips databases whose backup failed
        - Verify is enabled by default but can be disabled
       
    .PARAMETER SqlInstance
        The SQL Server instance hosting the databases to be backed up.

    .PARAMETER BackupType
        The SQL Server backup type (Full, Diff, Log).

    .PARAMETER BackupDirectory
        Target root directory

    .PARAMETER IncludeDatabases
        Array of database names or SQL LIKE patterns to include.
        If specified, ONLY databases matching these patterns will be backed up.
        Exact names (e.g. "MyDB") and patterns using % and _ wildcards are supported.
        Example: @("PROD%", "MyDB", "%_CRITICAL")

    .PARAMETER ExcludeDatabases
        Array of database names or SQL LIKE patterns to exclude.
        Exact names (e.g. "MyDB") and patterns using % and _ wildcards are supported.
        Example: @("MyDB", "%_NOBACKUP", "TEST%")

    .PARAMETER Degree
        Degree of parallelism (number of parallel backups)
        Default 4

    .PARAMETER FileCount
        Number of files to split the backup (improve performance of backup and restore)
        Default 4
     
    .PARAMETER Timeout
        Timeout of one backup (not the whole timeout)
        Default 3600 seconds   

    .PARAMETER Verify
        Verify backup integrity after completion. Enabled by default.

    .PARAMETER CleanupTime
        Retention period in hours. Backup files older than this will be deleted.
        0 means no cleanup (default).

    .PARAMETER CleanupWhen
        When to run cleanup: Before, After, or Both.
        Default: After
        
    .PARAMETER LogDirectory
        Directory where a log file will be stored. 
        Log file is named with the instance name and timestamp.

    .NOTES
        Tags: DisasterRecovery, Backup, Restore
        Author: Romain Ferraton
        Website: 
        Copyright: (c) 2022 by Romain Ferraton, licensed under MIT
        License: MIT https://opensource.org/licenses/MIT
        
        Dependencies : 
            Install-Module Logging
            Install-Module dbatools

        Compatibility : Powershell 7.3+

    .LINK
        
    .EXAMPLE
        PS C:\> .\MSSQL_Backup_All_Databases_In_Parallel.ps1 -SqlInstance MySQLServerInstance -BackupType Diff -BackupDirectory "S:\BACKUPS" -Degree 4 -FileCount 4 -LogDirectory D:\scripts\logs
        This will perform a differential parallel backups of all databases of the MySQLServerInstance Instance in the S:\BACKUPS Directory with backup files slitted into 4 parts and log will be console displayed as well as writen in a timestamped file in the D:\scripts\logs directory
    
    .EXAMPLE
        PS C:\> .\MSSQL_Backup_All_Databases_In_Parallel.ps1 -SqlInstance MySQLServerInstance -BackupType Full -BackupDirectory "S:\BACKUPS" -Degree 4 -FileCount 4 -ExcludeDatabases @("%_NOBACKUP","TEST%","TempWorkDB") -CleanupTime 72 -CleanupWhen After
        This will perform Full parallel backups excluding databases matching patterns, then cleanup backup files older than 72 hours
    #>

    param 
    (
        [Parameter(Mandatory)] [string] $SqlInstance,
        [Parameter(Mandatory)] [ValidateSet('Full','Diff','Log')] [string] $BackupType,
        [Parameter(Mandatory)] [string] $BackupDirectory,
        [Parameter()] [string[]] $IncludeDatabases = @(),
        [Parameter()] [string[]] $ExcludeDatabases = @(),
        [Parameter()] [Int16] $Degree = 4,
        [Parameter()] [Int16] $FileCount = 4,
        [Parameter()] [Int16] $Timeout = 3600,
        [Parameter()] [switch] $Verify = $true,
        [Parameter()] [Int32] $CleanupTime = 0,
        [Parameter()] [ValidateSet('Before','After','Both')] [string] $CleanupWhen = 'After',
        [Parameter()] [switch] $WhatIf,
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
        # Escape regex special chars, then convert SQL LIKE wildcards
        $escaped = [regex]::Escape($Pattern)
        $escaped = $escaped -replace '%', '.*'
        $escaped = $escaped -replace '_', '.'
        return "^${escaped}$"
    }

    function Test-DatabaseExcluded {
        <#
        .SYNOPSIS
            Tests if a database name matches any of the exclusion patterns (exact name or SQL LIKE pattern)
        #>
        param(
            [string]$DatabaseName,
            [string[]]$ExcludePatterns
        )
        foreach ($pattern in $ExcludePatterns) {
            if ($pattern -match '[%_]') {
                # SQL LIKE pattern: convert to regex
                $regex = Convert-SqlLikeToRegex -Pattern $pattern
                if ($DatabaseName -match $regex) { return $true }
            }
            else {
                # Exact name match (case-insensitive)
                if ($DatabaseName -ieq $pattern) { return $true }
            }
        }
        return $false
    }

    function Invoke-BackupCleanup {
        <#
        .SYNOPSIS
            Removes old backup files from the backup directory based on retention period.
            Only deletes files matching the backup extension for the current backup type.
            Skips databases listed in SkipDatabases (e.g. failed backups).
        #>
        param(
            [string]$BackupDirectory,
            [string]$BackupExtension,
            [string]$BackupType,
            [int]$CleanupTime,
            [string[]]$SkipDatabases = @()
        )

        if ($CleanupTime -le 0) { return }

        $RetentionDate = (Get-Date).AddHours(-$CleanupTime)
        Write-Log -Level INFO -Message "Cleanup: removing *.${BackupExtension} files older than ${RetentionDate} from ${BackupDirectory}"

        if (-not (Test-Path $BackupDirectory)) {
            Write-Log -Level INFO -Message "Cleanup: backup directory ${BackupDirectory} does not exist, skipping"
            return
        }

        # Find all backup files matching the extension recursively
        $oldFiles = Get-ChildItem -Path $BackupDirectory -Recurse -File -Filter "*.${BackupExtension}" |
            Where-Object { $_.LastWriteTime -lt $RetentionDate }

        $deletedCount = 0
        $skippedCount = 0
        foreach ($file in $oldFiles) {
            # Check if this file belongs to a skipped database (by checking if any skip db name is in the path)
            $shouldSkip = $false
            foreach ($skipDb in $SkipDatabases) {
                if ($file.FullName -ilike "*\${skipDb}\*") {
                    $shouldSkip = $true
                    break
                }
            }
            if ($shouldSkip) {
                Write-Log -Level INFO -Message "Cleanup: SKIPPING (failed backup) $($file.FullName)"
                $skippedCount++
                continue
            }
            Write-Log -Level INFO -Message "Cleanup: deleting $($file.FullName)"
            Remove-Item -Path $file.FullName -Force
            $deletedCount++
        }
        Write-Log -Level INFO -Message "Cleanup complete: ${deletedCount} file(s) deleted, ${skippedCount} file(s) skipped"
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
        $TimestampLog=Get-Date -UFormat "%Y-%m-%dT%H%M%S"
        $InstanceNameSafe = $SqlInstance -replace '\\', '_'
        mkdir $LogDirectory -Force
        $LogfileName="dbatools_backup_${InstanceNameSafe}_${BackupType}_${TimestampLog}.log"
        $LogFile= Join-DbaPath -Path $LogDirectory -Child $LogfileName
        Add-LoggingTarget -Name File -Configuration @{Level = 'INFO'; Path = $LogFile}
        Write-Log -Level INFO -Message "Log File : $LogFile"
    }
    
    Write-Log -Level INFO -Message "Parameter SQLInstance : ${SqlInstance}"
    Write-Log -Level INFO -Message "Parameter BackupType : ${BackupType}"
    Write-Log -Level INFO -Message "Parameter BackupDirectory : ${BackupDirectory}"
    Write-Log -Level INFO -Message "Parameter IncludeDatabases : $($IncludeDatabases -join ', ')"
    Write-Log -Level INFO -Message "Parameter ExcludeDatabases : $($ExcludeDatabases -join ', ')"
    Write-Log -Level INFO -Message "Parameter Degree : ${Degree}"
    Write-Log -Level INFO -Message "Parameter FileCount : ${FileCount}"
    Write-Log -Level INFO -Message "Parameter Timeout : ${Timeout}"
    Write-Log -Level INFO -Message "Parameter Verify : ${Verify}"
    Write-Log -Level INFO -Message "Parameter CleanupTime : ${CleanupTime} hours"
    Write-Log -Level INFO -Message "Parameter CleanupWhen : ${CleanupWhen}"
    

    $silentInsecureConnectionLog=Set-DbatoolsInsecureConnection -SessionOnly

    # Select Normal and Open Writable databases excluding system databases except msdb
    # Then apply user-defined exclusions (exact names + SQL LIKE patterns)

    $SystemExclusions = @("tempdb", "model")
    if ($BackupType -eq "Diff") { $SystemExclusions += "master" }

    # Separate exact names from LIKE patterns for Get-DbaDatabase -ExcludeDatabase (exact only)
    $ExactExclusions = $SystemExclusions + ($ExcludeDatabases | Where-Object { $_ -notmatch '[%_]' })
    $LikePatterns = @($ExcludeDatabases | Where-Object { $_ -match '[%_]' })

    # Separate IncludeDatabases into exact names and LIKE patterns
    $IncludeExact = @($IncludeDatabases | Where-Object { $_ -notmatch '[%_]' })
    $IncludeLikePatterns = @($IncludeDatabases | Where-Object { $_ -match '[%_]' })

    switch ( $BackupType ) 
        {
            "Full" 
                {
                    $BackupExtension="bak"  
                    $Databases = Get-DbaDatabase -SqlInstance $SqlInstance -ExcludeDatabase $ExactExclusions | Where-Object {($_.IsUpdateable) -and ($_.Status -ilike "Normal*")}
                }
            "Diff" 
                {
                    $BackupExtension="bak" 
                    $Databases = Get-DbaDatabase -SqlInstance $SqlInstance -ExcludeDatabase $ExactExclusions | Where-Object {($_.IsUpdateable) -and ($_.Status -ilike "Normal*")}
                }
            "Log"  
                {
                    $BackupExtension="trn"  
                    $Databases = Get-DbaDatabase -SqlInstance $SqlInstance -ExcludeDatabase $ExactExclusions | Where-Object { ($_.IsUpdateable) -and ($_.Status -ilike "Normal*") -and ($_.RecoveryModel -ne "Simple")}
                }
    
        }

    # Apply IncludeDatabases filter (exact names + LIKE patterns)
    if ($IncludeDatabases.Count -gt 0) {
        $beforeCount = $Databases.Count
        $Databases = $Databases | Where-Object {
            $dbName = $_.Name
            # Match if exact name is in the include list OR matches any include LIKE pattern
            ($IncludeExact -icontains $dbName) -or
            ($IncludeLikePatterns.Count -gt 0 -and (Test-DatabaseExcluded -DatabaseName $dbName -ExcludePatterns $IncludeLikePatterns))
        }
        $includedByFilter = $Databases.Count
        Write-Log -Level INFO -Message "Included ${includedByFilter} database(s) from ${beforeCount} by IncludeDatabases filter: $($IncludeDatabases -join ', ')"
    }

    # Apply SQL LIKE pattern exclusions
    if ($LikePatterns.Count -gt 0) {
        $beforeCount = $Databases.Count
        $Databases = $Databases | Where-Object { -not (Test-DatabaseExcluded -DatabaseName $_.Name -ExcludePatterns $LikePatterns) }
        $excludedByPattern = $beforeCount - $Databases.Count
        if ($excludedByPattern -gt 0) {
            Write-Log -Level INFO -Message "Excluded ${excludedByPattern} database(s) by LIKE pattern(s): $($LikePatterns -join ', ')"
        }
    }

    Write-Log -Level INFO -Message "BackupExtension : ${BackupExtension}"
    Write-Log -Level INFO -Message "Databases to backup : $($Databases.Count) - $($Databases.Name -join ', ')"

    # WhatIf mode: print what would be done and exit
    if ($WhatIf) {
        Write-Host ""
        Write-Host "=== WhatIf Mode - No action will be performed ===" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Instance        : $SqlInstance" -ForegroundColor White
        Write-Host "Backup Type     : $BackupType" -ForegroundColor White
        Write-Host "Backup Directory: $BackupDirectory" -ForegroundColor White
        Write-Host "File Extension  : .$BackupExtension" -ForegroundColor White
        Write-Host "FileCount       : $FileCount" -ForegroundColor White
        Write-Host "Degree          : $Degree" -ForegroundColor White
        Write-Host "Verify          : $Verify" -ForegroundColor White
        Write-Host "Timeout         : $Timeout seconds" -ForegroundColor White
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
            Write-Host "Cleanup         : *.${BackupExtension} files older than ${RetentionDate} (${CleanupTime}h retention)" -ForegroundColor Yellow
            Write-Host "Cleanup When    : $CleanupWhen" -ForegroundColor Yellow
            Write-Host ""
            if (Test-Path $BackupDirectory) {
                $filesToDelete = Get-ChildItem -Path $BackupDirectory -Recurse -File -Filter "*.${BackupExtension}" |
                    Where-Object { $_.LastWriteTime -lt $RetentionDate }
                if ($filesToDelete.Count -gt 0) {
                    $totalSizeMB = [math]::Round(($filesToDelete | Measure-Object -Property Length -Sum).Sum / 1MB, 2)
                    Write-Host "Files that would be deleted ($($filesToDelete.Count) files, ${totalSizeMB} MB):" -ForegroundColor Red
                    $filesToDelete | Group-Object { $_.Directory.Parent.Parent.Name } | Sort-Object Name | ForEach-Object {
                        $dbSizeMB = [math]::Round(($_.Group | Measure-Object -Property Length -Sum).Sum / 1MB, 2)
                        Write-Host "  - $($_.Name) : $($_.Count) file(s), ${dbSizeMB} MB" -ForegroundColor DarkRed
                    }
                } else {
                    Write-Host "No files would be deleted (no *.${BackupExtension} files older than ${RetentionDate})" -ForegroundColor DarkGray
                }
            } else {
                Write-Host "Backup directory does not exist yet, no files to clean up" -ForegroundColor DarkGray
            }
        } else {
            Write-Host "Cleanup         : Disabled" -ForegroundColor DarkGray
        }
        Write-Host ""
        Write-Host "=== End WhatIf ==="  -ForegroundColor Cyan
        return
    }
        
    
    # Run pre-backup cleanup if configured
    if ($CleanupWhen -in @('Before','Both') -and $CleanupTime -gt 0) {
        Write-Log -Level INFO -Message "Running pre-backup cleanup..."
        Invoke-BackupCleanup -BackupDirectory $BackupDirectory -BackupExtension $BackupExtension `
            -BackupType $BackupType -CleanupTime $CleanupTime
    }

    # A concurrent dictionnary to manage log and returns from parallel backups
    $ResultsObjects = [System.Collections.Concurrent.ConcurrentDictionary[string,System.Object]]::new()
    
    $duration=(Measure-Command{
        
        # Get Databases list and loop on each database to backup in parallel (largest first)
        $SilentdbatoolsResults = $Databases | Sort-Object -Property Size -Descending | ForEach-Object -Parallel {
    
        $ResultsDict = $using:ResultsObjects #mapped to the $ResultsObjects ConcurrentDictionnary (Thread Safe)
        $Database=$_.Name
        $SqlInstance=$_.SqlInstance
        $BackupDirectory=$using:BackupDirectory
        $FileCount=$using:FileCount
        $BackupType=$using:BackupType
        $BackupExtension=$using:BackupExtension
        $VerifyFlag=$using:Verify
    
        try {   
                # Build backup parameters via splatting
                $BackupParams = @{
                    SqlInstance     = $SqlInstance
                    Database        = $Database
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
                if ($VerifyFlag) { $BackupParams['Verify'] = $true }

                $resbackup = Backup-DbaDatabase @BackupParams -WarningVariable WarningVariable 
                
                # Add result to ConcurrentDictionary for logging and return
                $silentres = $ResultsDict.TryAdd($Database,$resbackup)
            }
            catch  {
                if ($WarningVariable)
                 {  
                    $ErrorMessage = ""
                    foreach ($warning in $WarningVariable) {
                        $ErrorMessage += $WarningVariable.Message
                        $ErrorMessage += "`n"  
                    }
                    Write-Output "Warnings occurred: $ErrorMessage"
                 }            
                $ExitError=[PSCustomObject]@{Database = $Database; ErrorMessage = $ErrorMessage}                
                $ResultsDict.TryAdd($Database,$ExitError)
            } 
        } -ThrottleLimit $Degree -TimeoutSeconds $Timeout 
    } ).TotalSeconds
         
        # Count Results
        $ResultsCount = $ResultsObjects.Count        
       
        # Get Results from $ResultsObjects ConcurrentDictionary and find issues
        $resultinfos = $ResultsObjects.Values | Select-Object ComputerName, InstanceName, Database,Type,Software , Start, End, Duration,TotalSize, CompressedBackupSize, Verified, Backupcomplete, Backupfilescount, BackupPath,Script, ErrorMessage  | Sort-Object -Property Database

        # Find databases with backup problems (no BackupComplete or BackupComplete = False, or has ErrorMessage)
        $DatabasesBackupProblems = @()
        foreach ($key in $ResultsObjects.Keys) {
            $val = $ResultsObjects[$key]
            $info = $resultinfos | Where-Object { $_.Database -eq $key }
            if ($null -eq $info) {
                # Result exists but had no standard backup output (error object)
                $DatabasesBackupProblems += [PSCustomObject]@{ Database = $key; ErrorMessage = $val.ErrorMessage }
            }
            elseif ($null -eq $info.BackupComplete -or $info.BackupComplete -eq $false) {
                $DatabasesBackupProblems += [PSCustomObject]@{ Database = $key; ErrorMessage = $info.ErrorMessage }
            }
        }
        
        # Count Databases with backup problems
        $DatabasesBackupProblemsCount = $DatabasesBackupProblems.Count
    
        # Log Results for successful backups
        foreach ($resultinfo in $resultinfos) {
            $Message= "ComputerName : " + $resultinfo.ComputerName + " | InstanceName : " + $resultinfo.InstanceName + " | Database : " + $resultinfo.Database + " | Type : " + $resultinfo.Type + " | Start : " + $resultinfo.Start + " | End : " + $resultinfo.End + " | Duration : " + $resultinfo.Duration  + " | TotalSize : " + $resultinfo.TotalSize + " | CompressedBackupSize : " + $resultinfo.CompressedBackupSize + " | Verified : " + $resultinfo.Verified + " | Backupfilescount : " + $resultinfo.Backupfilescount + " | BackupPath : " + $resultinfo.BackupPath+ " | BackupScript : " + $resultinfo.Script
            if ($resultinfo.Backupcomplete)
            {
                Write-Log -Level INFO -Message $Message
            }
    
        }
        # Log Results for Databases with backup problems
        foreach ($resultinfo in $DatabasesBackupProblems)
        {
            $Message= "InstanceName : " + $SqlInstance + " | Database : " + $resultinfo.Database + " | Type : " + $BackupType + " | Error Message : " + $resultinfo.ErrorMessage
            Write-Log -Level ERROR -Message $Message
        }
    
        # Run post-backup cleanup if configured (skip failed databases to preserve their last good backup)
        if ($CleanupWhen -in @('After','Both') -and $CleanupTime -gt 0) {
            $FailedDbNames = @($DatabasesBackupProblems | ForEach-Object { $_.Database })
            Write-Log -Level INFO -Message "Running post-backup cleanup..."
            Invoke-BackupCleanup -BackupDirectory $BackupDirectory -BackupExtension $BackupExtension `
                -BackupType $BackupType -CleanupTime $CleanupTime -SkipDatabases $FailedDbNames
        }

        # change Return Code if at least one backup had a problem
        if ($DatabasesBackupProblems.Count -gt 0)
        {
            Start-Sleep -Seconds 1       
            Write-Log -Level INFO -Message "${ResultsCount} ${BackupType} Backups was tried with ${DatabasesBackupProblemsCount} failure(s) in ${duration} seconds `n"
            Write-Log -Level ERROR -Message "${DatabasesBackupProblemsCount} Database Backup(s) had a problem"
            exit 2
        }
    
        Write-Log -Level INFO -Message "${ResultsCount} ${BackupType} Backups SUCCESSFULLY COMPLETED in ${duration} seconds"
    
        #return $ResultsObjects
    