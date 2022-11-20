    <#
    .SYNOPSIS
        Parallel Differential Backup of all databases of a single SQL Server SqlInstance to target Directory
    .DESCRIPTION
        Objective : Backup Differential all databases (except master, tempdb and model) to a target Backup directory in parallel
        the script will create a .diff file
        "${BackupDirectory}\servername\instancename\dbname\backuptype\servername_dbname_backuptype_timestamp.diff"
       
    .PARAMETER SqlInstance
        The SQL Server instance hosting the databases to be backed up.

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

    .NOTES
        Tags: DisasterRecovery, Backup, Restore
        Author: Romain Ferraton
        Website: 
        Copyright: (c) 2022 by Romain Ferraton, licensed under MIT
        License: MIT https://opensource.org/licenses/MIT
        
        Dependencies : 
            Install-Module Logging
            Install-Module dbatools

    .LINK
        
    .EXAMPLE
        PS C:\> .\MSSQL_BackupDiff_All_Databases.ps1 -SqlInstance MySQLServerInstance -BackupDirectory "S:\BACKUPS" -Degree 4 -FileCount 4
        This will perform a differential parallel backups of all databases of the MySQLServerInstance Instance in the S:\BACKUPS Directory with backup files slitted into 4 parts
    
    #>

param 
(
    [Parameter(Mandatory)] [string] $SqlInstance = ".",
    [Parameter(Mandatory)] [string] $BackupDirectory = "S:\BACKUPS\",
    [Parameter()] [Int16] $Degree = 4,
    [Parameter()] [Int16] $FileCount = 4,
    [Parameter()] [Int16] $Timeout = 3600,
    [Parameter()] [string] $LogLevel = "INFO"
)

Set-LoggingDefaultLevel -Level $LogLevel
Add-LoggingTarget -Name Console -Configuration @{
    ColorMapping = @{
        DEBUG = 'Gray'
        INFO  = 'White'
    }
}

Write-Log -Level INFO -Message "Parameter SQL Instance = ${SqlInstance}"
Write-Log -Level INFO -Message "Parameter BackupDirectory = ${BackupDirectory}"
Write-Log -Level INFO -Message "Parameter Degree = ${Degree}"
Write-Log -Level INFO -Message "Parameter FileCount = ${FileCount}"
Write-Log -Level INFO -Message "Parameter Timeout = ${Timeout}"

# Select Normal and Open Writable databases exluding system databases except msdb
$Databases = Get-DbaDatabase -SqlInstance $SqlInstance -ExcludeDatabase "tempdb","model","master" | Where-Object {($_.IsUpdateable) -and ($_.Status -ilike "Normal*")}
Write-Log -Level DEBUG -Message "Databases detail in body" -Body $Databases

$Results = [System.Collections.Concurrent.ConcurrentDictionary[string,boolean]]::new()
$ResultsMessages = [System.Collections.Concurrent.ConcurrentDictionary[string,string]]::new()

$duration=(Measure-Command{

    $SilentdbatoolsResults = $Databases | Sort-Object -Property Size -Descending | ForEach-Object -Parallel {
    $TimestampLogFormat=(Get-LoggingDefaultFormat).Split("]")[0].Split("+")[1].Replace("}","")
    $LogLevel=$using:LogLevel
    Set-LoggingDefaultLevel -Level $LogLevel
    $Resdict = $using:Results    # $Resdict is mapped to the $Result Dictionnary
    $Messagesdict = $using:ResultsMessages
    $Database=$_.Name
    $SqlInstance=$_.SqlInstance
    $ServerName=$_.ComputerName
    $InstanceName=$_.InstanceName
    $v_BackupDirectory=$using:BackupDirectory
    $v_filecount=$using:FileCount
    try {
            
            $resbackup = Backup-DbaDatabase -SqlInstance $SqlInstance -Database $Database -Type Diff -CompressBackup -Checksum -Verify -FileCount $v_filecount -FilePath "${v_BackupDirectory}\servername\instancename\dbname\backuptype\servername_dbname_backuptype_timestamp.diff" -TimeStampFormat "yyyyMMdd_HHmm" -ReplaceInName -CreateFolder -ErrorAction Stop -EnableException
            $rc=$?
            $BackupFolder=$resbackup.BackupFolder
            $CompressedBackupSize=$resbackup.CompressedBackupSize
            $BackupDuration=$resbackup.Duration
            $BackupTotalSize=$resbackup.TotalSize
            $BackupVerified=$resbackup.Verified
            $BackupScript=$resbackup.Script
            $TimestampLog=Get-Date -UFormat $TimestampLogFormat
            $LogMessage="[${TimestampLog}] [INFO   ] SUCCESS - Backups Differential of ${Database} on ${SqlInstance} to ${BackupFolder} with Compressed Backup Size of ${CompressedBackupSize} in ${BackupDuration} - Total Size : ${BackupTotalSize} - Verified : ${BackupVerified} `r`n"
            $BodyMessage="[${TimestampLog}]  [DEBUG   ] Script : ${BackupScript} `r`n"
            Write-Log -Level INFO -Message $LogMessage -Body $BodyMessage
            $silentres=$Resdict.TryAdd($Database, $rc)            
            $silentres=$Messagesdict.TryAdd($Database,$LogMessage)

        }
        catch {
            $TimestampLog=Get-Date -UFormat $TimestampLogFormat
            $LogMessage="[${TimestampLog}] [ERROR   ] FAILED - Backup Differential of ${Database} on ${SqlInstance} to ${v_BackupDirectory} `r`n"
            $BodyMessage="[${TimestampLog}] [DEBUG   ] Script : ${BackupScript} `r`n"
            Write-Log -Level INFO -Message $LogMessage -Body $BodyMessage            
            $rc=$False
            $silentres=$Resdict.TryAdd($Database, $rc)
            $silentres=$Messagesdict.TryAdd($Database,$LogMessage)
        } 
    } -ThrottleLimit $Degree -TimeoutSeconds $Timeout 
} ).TotalSeconds
    
    Set-LoggingDefaultLevel -Level $LogLevel

    $DatabasesBackupProblems = $Results.ToArray() | Where-Object{$_.Value -eq $False}
    $ResultsCount=$Results.Count
    $DatabasesBackupProblemsCount=$DatabasesBackupProblems.Count
   
    if ($DatabasesBackupProblems.Count -gt 0)
    {
        Start-Sleep -Seconds 1       
        $ResultsMessages.ToArray() | Sort-Object | ForEach-Object {Write-Host($_.Value)}
        Write-Log -Level INFO -Message "${ResultsCount} Differential Backups was tried with ${DatabasesBackupProblemsCount} failure(s) in ${duration} seconds"
        Write-Log -Level ERROR -Message "${DatabasesBackupProblemsCount} Database Backup(s) had a problem : `r`n"
        exit 2
    }

    $ResultsMessages.ToArray() |  Sort-Object | ForEach-Object {Write-Host($_.Value)}  

    Write-Log -Level INFO -Message "${ResultsCount} Differential Backups SUCCESSFULLY COMPLETED in ${duration} seconds"

