#################################################################################################
#  
#   
#   MSSQL_BackupHistory_Load.ps1 will collect Backup History of all databases of all instances
#   referenced in a Central Management Server (CMS)
#
#   The Script use dbatools.io functions for that.
#
#   Data Collected are writen in a central Backup History Table named MSSQLBackupHistory.
#
#   MSSQLBackupHistory Table must be present in to dba database (dbatools for exemple)
#   You will find the DDL of the table in the git repo.
#
#
#   Each run will retrieve the last backup End Captured in the MSSQLBackupHistory table 
#   the last backup date retrieved of each database is used to filter the backup history
#   If no Last Backup Date is found, the full history is retrieved
#
#
#################################################################################################

#################################################################################################
##
##  Author(s) : Romain Ferraton
##
#################################################################################################

#################################################################################################
##
## Usage : MSSQL_BackupHistory_Load.ps1 -CMS CMSSERVER\DBA01 -DbaDatabase DBATOOLS -CMSGroup ALL
##
#################################################################################################

param 
(
    [Parameter(Mandatory)] [string] $CMS = ".", #Central Management Server
    [Parameter()] [string] $CMSGroup = "", #Central Management Server
    [Parameter(Mandatory)] [string] $DbaDatabase = "DBATOOLS" #Central DBA Database where the central MSSQLBackupHistory Table reside		
)

$DBAOS=$PSVersionTable.OS
if($null -eq $DBAOS)
{
    $DBAOS=[System.Environment]::OSVersion.Platform
}
Write-Output("$('[{0:yyyy-MM-dd} {0:HH:mm:ss.fff}]' -f (Get-Date)) - INFO - OS:"+$DBAOS)
Write-Output("$('[{0:yyyy-MM-dd} {0:HH:mm:ss.fff}]' -f (Get-Date)) - INFO - CMS:"+$CMS)
Write-Output("$('[{0:yyyy-MM-dd} {0:HH:mm:ss.fff}]' -f (Get-Date)) - INFO - CMSroup:"+$CMSGroup)
Write-Output("$('[{0:yyyy-MM-dd} {0:HH:mm:ss.fff}]' -f (Get-Date)) - INFO - DbaDatabase:"+$DbaDatabase)


# Get last backups of all databases in the MSSQLBackupHistory Central Table
$vquery = "SELECT [ComputerName],[InstanceName],[SqlInstance],[AvailabilityGroupName],[Database],MAX([End]) Max_End FROM [dbo].[MSSQLBackupHistory] GROUP BY [ComputerName], [InstanceName],  [SqlInstance],  [AvailabilityGroupName],  [Database]"
$DBLastBackups=Invoke-DbaQuery -SqlInstance $CMS -Database $DbaDatabase -Query $vquery -As DataTable

#Get All MSSQL Instances referenced in the Central Management Server (CMS)
$MSSQLInstances=Get-DbaRegServer -SqlInstance $CMS -Group $CMSGroup | Select-Object ServerName
Write-Output("$('[{0:yyyy-MM-dd} {0:HH:mm:ss.fff}]' -f (Get-Date)) - INFO - Found "+ $MSSQLInstances.Count + " MSSQLInstances" )

ForEach($MSSQLInstance in $MSSQLInstances)
{
    #$Databases=@()
    Write-Output("$('[{0:yyyy-MM-dd} {0:HH:mm:ss.fff}]' -f (Get-Date)) - INFO - Search Databases Starting for Instance "+ $MSSQLInstance.ServerName )
    $Databases = Invoke-DbaQuery -SqlInstance $MSSQLInstance.ServerName -Query 'SELECT name FROM sys.databases' -QueryTimeout 10 -As DataTable
    Write-Output("$('[{0:yyyy-MM-dd} {0:HH:mm:ss.fff}]' -f (Get-Date)) - INFO - Found " + $Databases.Count + " Databases for Server "+ $MSSQLInstance.ServerName )

    ForEach($Database in $Databases)
    {  
        $DBLastBackupEndDate=$null
        $DBLastBackupEndDate=$DBLastBackups.Select("Database = '"+$Database.Name+"' AND SqlInstance='"+$MSSQLInstance.ServerName+"'")| Select-Object Max_End
        If($null -ne $DBLastBackupEndDate)
        {
            Write-Output("$('[{0:yyyy-MM-dd} {0:HH:mm:ss.fff}]' -f (Get-Date)) - INFO - Search Database LastBackup for "+$Database.Name+"@"+$MSSQLInstance.ServerName+" found for "+$DBLastBackupEndDate.Max_End)
            #Get Backup history since the last backup found in the MSSQLBackupHistory Central Table +1 second 
            $DatabasesBackupHistory = Get-DbaDbBackupHistory -SqlInstance $MSSQLInstance.ServerName -Database $Database.Name -Since $DBLastBackupEndDate.Max_End.AddSeconds(1)
            $DatabasesBackupHistory | Write-DbaDbTableData -SqlInstance $CMS -Database $DbaDatabase -Table MSSQLBackupHistory
        }
        else {
            Write-Output("$('[{0:yyyy-MM-dd} {0:HH:mm:ss.fff}]' -f (Get-Date)) - INFO - Search Database LastBackup for "+$Database.Name+"@"+$MSSQLInstance.ServerName+" not found")
            $DatabasesBackupHistory = Get-DbaDbBackupHistory -SqlInstance $MSSQLInstance.ServerName -Database $Database.Name  
            $DatabasesBackupHistory | Write-DbaDbTableData -SqlInstance $CMS -Database $DbaDatabase -Table MSSQLBackupHistory -AutoCreateTable
        }    
        Write-Output("$('[{0:yyyy-MM-dd} {0:HH:mm:ss.fff}]' -f (Get-Date)) - INFO - Search Database "+$Database.Name+" Backup History Completed")
    }
    Write-Output("$('[{0:yyyy-MM-dd} {0:HH:mm:ss.fff}]' -f (Get-Date)) - INFO - Search Databases Completed for "+ $MSSQLInstance.ServerName )   
}