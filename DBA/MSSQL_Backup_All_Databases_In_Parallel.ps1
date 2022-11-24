    <#
    .SYNOPSIS
        Parallel Backup of all databases of a single SQL Server SqlInstance to target Directory
    .DESCRIPTION
        Objective : Backup all databases (except tempdb and model) to a target Backup directory in parallel
        the script will create a .bak, .diff or .trn file depending of the backup type (full, diff, log)
        "${BackupDirectory}\servername\instancename\dbname\backuptype\servername_dbname_backuptype_timestamp.${BackupExtension}"
       
    .PARAMETER SqlInstance
        The SQL Server instance hosting the databases to be backed up.

    .PARAMETER BackupType
        The SQL Server backup type (Full, Diff, Log).

    .PARAMETER BackupDirectory
        Target root directory

    .PARAMETER Degree
        Degree of parallelism (number of parallel backups)
        Default 4

    .PARAMETER FileCount
        Number of files to split the backup (improve performance of backup and restore)
        Default 4
     
    .PARAMETER Timeout
        Timeout of one backup (not the whole timeout)
        Default 3600 seconds   
        
    .PARAMETER LogDirectory
    Directory where a log file can be stored

    .NOTES
        Tags: DisasterRecovery, Backup, Restore
        Author: Romain Ferraton
        Website: 
        Copyright: (c) 2022 by Romain Ferraton, licensed under MIT
        License: MIT https://opensource.org/licenses/MIT
        
        Dependencies : 
            Install-Module Logging
            Install-Module dbatools
            Install-Module JoinModule

        Compatibility : Powershell 7.3+

    .LINK
        
    .EXAMPLE
        PS C:\> .\MSSQL_Backup_All_Databases_In_Parallel.ps1 -SqlInstance MySQLServerInstance -BackupType Diff -BackupDirectory "S:\BACKUPS" -Degree 4 -FileCount 4 -LogDirectory D:\scripts\logs
        This will perform a differential parallel backups of all databases of the MySQLServerInstance Instance in the S:\BACKUPS Directory with backup files slitted into 4 parts and log will be console displayed as well as writen in a timestamped file in the D:\scripts\logs directory
    
        PS C:\> .\MSSQL_Backup_All_Databases_In_Parallel.ps1 -SqlInstance MySQLServerInstance -BackupType Full -BackupDirectory "S:\BACKUPS" -Degree 4 -FileCount 4
        This will perform a Full parallel backups of all databases of the MySQLServerInstance Instance in the S:\BACKUPS Directory with backup files slitted into 4 parts and log will be console displayed
    #>

    param 
    (
        [Parameter(Mandatory)] [string] $SqlInstance,
        [Parameter(Mandatory)] [ValidateSet('Full','Diff','Log')] [string] $BackupType,
        [Parameter(Mandatory)] [string] $BackupDirectory,
        [Parameter()] [Int16] $Degree = 4,
        [Parameter()] [Int16] $FileCount = 4,
        [Parameter()] [Int16] $Timeout = 3600,
        [Parameter()] [string] $LogLevel = "INFO",
        [Parameter()] [string] $LogDirectory
    )
    
    
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
        mkdir $LogDirectory -Force
        $LogfileName="dbatools_backup_database_" + $BackupType + "_"+ $TimestampLog + ".log"
        $LogFile= Join-DbaPath -Path $LogDirectory -Child $LogfileName
        Add-LoggingTarget -Name File -Configuration @{Level = 'INFO'; Path = $LogFile}
        Write-Log -Level INFO -Message "Log File : $LogFile"
    }
    
    Write-Log -Level INFO -Message "Parameter SQLInstance : ${SqlInstance}"
    Write-Log -Level INFO -Message "Parameter BackupType : ${BackupType}"
    Write-Log -Level INFO -Message "Parameter BackupDirectory : ${BackupDirectory}"
    Write-Log -Level INFO -Message "Parameter Degree : ${Degree}"
    Write-Log -Level INFO -Message "Parameter FileCount : ${FileCount}"
    Write-Log -Level INFO -Message "Parameter Timeout : ${Timeout}"
    
    # Select Normal and Open Writable databases exluding system databases except msdb
    
    
    #Write-Log -Level DEBUG -Message "Databases found by Get-DbaDatabase detail in body" -Body $Databases
    
    switch ($BackupType) 
        {
            "Full" 
                {
                    $BackupExtension="bak"  
                    $Databases = Get-DbaDatabase -SqlInstance $SqlInstance -ExcludeDatabase "tempdb","model" | Where-Object {($_.IsUpdateable) -and ($_.Status -ilike "Normal*")}
                }
            "Diff" 
                {
                    $BackupExtension="diff" 
                    $Databases = Get-DbaDatabase -SqlInstance $SqlInstance -ExcludeDatabase "tempdb","model","master" | Where-Object {($_.IsUpdateable) -and ($_.Status -ilike "Normal*")}
                }
            "Log"  
                {
                    $BackupExtension="trn"  
                    $Databases = Get-DbaDatabase -SqlInstance $SqlInstance -ExcludeDatabase "tempdb","model" | Where-Object { ($_.IsUpdateable) -and ($_.Status -ilike "Normal*") -and ($_.RecoveryModel -ne "Simple")}
                }
    
        }
        
    
    
    # A concurrent dictionnary to manage log and returns from parallel backups
    $ResultsObjects = [System.Collections.Concurrent.ConcurrentDictionary[string,System.Object]]::new()
    
    $duration=(Measure-Command{
    
        $SilentdbatoolsResults = $Databases | Sort-Object -Property Size -Descending | ForEach-Object -Parallel {
    
        $ResultsDict = $using:ResultsObjects #mapped to the $ResultsObjects ConcurrentDictionnary (Thread Safe)
        $Database=$_.Name
        $SqlInstance=$_.SqlInstance
        $BackupDirectory=$using:BackupDirectory
        $FileCount=$using:FileCount
        $BackupType=$using:BackupType
    
        try {            
                $resbackup = Backup-DbaDatabase -SqlInstance $SqlInstance -Database $Database -Type $BackupType -CompressBackup -Checksum -Verify -FileCount $FileCount -FilePath "${BackupDirectory}\servername\instancename\dbname\backuptype\servername_dbname_backuptype_timestamp.${BackupExtension}" -TimeStampFormat "yyyyMMdd_HHmm" -ReplaceInName -CreateFolder #-ErrorAction Stop -EnableException
                $silentres = $ResultsDict.TryAdd($Database,$resbackup)
            }
            catch  {
                Write-Host "[ERROR] - Backup $BackupType of $Database Failed"
                $silentres=$ResultsDict.TryAdd($Database,$resbackup)
            } 
        } -ThrottleLimit $Degree -TimeoutSeconds $Timeout 
    } ).TotalSeconds
         
        # Get Results from $ResultsObjects ConcurrentDictionary and find issues
        $ResultsCount = $ResultsObjects.Count
        
       
        $resultinfos = $ResultsObjects.Values | Select-Object ComputerName, InstanceName, Database,Type,Software , Start, End, Duration,TotalSize, CompressedBackupSize, Verified, Backupcomplete, Backupfilescount, BackupPath,Script  | Sort-Object -Property Database
        
        $ResultAllDb = $ResultsObjects.Keys | Foreach-Object { [PSCustomObject]@{'Database' = $_ }}
        $DatabasesBackupProblems = $ResultAllDb | LeftJoin $resultinfos -On Database | where-object{$null -eq $_.BackupComplete -Or $_BackupComplete -eq $False}
        
        $DatabasesBackupProblemsCount = $DatabasesBackupProblems.Count
    
        # Log Results
        foreach ($resultinfo in $resultinfos) {
            $Message= "ComputerName : " + $resultinfo.ComputerName + " | InstanceName : " + $resultinfo.InstanceName + " | Database : " + $resultinfo.Database + " | Type : " + $resultinfo.Type + " | Start : " + $resultinfo.Start + " | End : " + $resultinfo.End + " | Duration : " + $resultinfo.Duration  + " | TotalSize : " + $resultinfo.TotalSize + " | CompressedBackupSize : " + $resultinfo.CompressedBackupSize + " | Verified : " + $resultinfo.Verified + " | Backupfilescount : " + $resultinfo.Backupfilescount + " | BackupPath : " + $resultinfo.BackupPath+ " | BackupScript : " + $resultinfo.Script
            if ($resultinfo.Backupcomplete)
            {
                Write-Log -Level INFO -Message $Message
            }
    
        }
        
        foreach ($resultinfo in $DatabasesBackupProblems)
        {
            $Message= "InstanceName : " + $SqlInstance + " | Database : " + $resultinfo.Database + " | Type : " + $BackupType
            Write-Log -Level ERROR -Message $Message
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
    
    