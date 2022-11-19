# Objective : Backup FULL all databases (except tempdb and model) to a target Backup directory
# the script use dbatools.io that must be installed
# the script will create a .bak file
# "${BackupDirectory}\servername\instancename\dbname\backuptype\servername_dbname_backuptype_timestamp.bak"

# Usage sample : .\MSSQL_BackupFull_All_Databases.ps1 -SqlInstance MySQLServerInstance -BackupDirectory "S:\BACKUPS" -Degree 4 -FileCount 4

param 
(
    [Parameter(Mandatory)] [string] $SqlInstance = ".",
    [Parameter(Mandatory)] [string] $BackupDirectory = "S:\BACKUPS\",
    [Parameter()] [Int16] $Degree = 4,
    [Parameter()] [Int16] $FileCount = 4
)

# Select Normal and Open Writable databases
$Databases = Get-DbaDatabase -SqlInstance $SqlInstance -ExcludeDatabase "tempdb","model" | Where-Object {($_.IsUpdateable) -and ($_.Status -ilike "Normal*")}
$Databases | Format-Table

$Results = [System.Collections.Concurrent.ConcurrentDictionary[string,boolean]]::new()

$Databases  | Sort-Object -Property Size -Descending | ForEach-Object -Parallel {
    $Resdict = $using:Results    # $Resdict is mapped to the $Result Dictionnary
    try {
            $Database=$_.Name
            $SqlInstance=$_.SqlInstance
            $ServerName=$_.ComputerName
            $InstanceName=$_.InstanceName
            $v_BackupDirectory=$using:BackupDirectory
            $v_filecount=$using:FileCount
            Backup-DbaDatabase -SqlInstance $SqlInstance -Database $Database -Type Full -CompressBackup -Checksum -Verify -FileCount $v_filecount -FilePath "${v_BackupDirectory}\servername\instancename\dbname\backuptype\servername_dbname_backuptype_timestamp.bak" -TimeStampFormat "yyyyMMdd_HHmm" -ReplaceInName -CreateFolder -ErrorAction Stop -EnableException
            $rc=$?
            Write-Host ("Backups Full of ${Database} on ${SqlInstance} to ${v_BackupDirectory}\${ServerName}\${InstanceName}\${Database}\Full\ SUCCESSFULLY`r`n")  
            $silentres=$Resdict.TryAdd($_.Name, $rc)
        }
        catch {
            Write-Host ("Backups Full of ${Database} on ${SqlInstance} to ${v_BackupDirectory}\${Servername}\${InstanceName}\${Database}\Full\ FAILED`r`n")
            $rc=$false
            $silentres=$Resdict.TryAdd($_.Name, $rc)
        } 


    } -ThrottleLimit $Degree

    $Results | Format-Table

    if ($Results.Values.Contains($false))
    {
        Write-Host ("At east one database Backups had a problem")
        exit 2
    }


