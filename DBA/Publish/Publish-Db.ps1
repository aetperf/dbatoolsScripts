<#
.SYNOPSIS
 Publishes (restores) a database to a target SQL instance from the backups files in a root folder.
.DESCRIPTION
 This script uses dbatools to restore a database from backup files located in a specified folder.

.PARAMETER ServerName
 The target SQL Server instance name.
.PARAMETER DatabaseName
The name of the database to be published (restored).
.PARAMETER BackupRoot
 The root folder (from the mssql server perspective) containing one or more backup files for the database
.PARAMETER Steps
    A comma-separated list of steps to perform. Valid steps are:
    - Restore: Restores the database from backup files to a temporary database named '<DatabaseName>_Restore'.
    - Swap: Swaps the restored database with the original database by renaming them.
    Default is "Restore,Swap".
.PARAMETER BackupFilter
 A filter pattern to identify backup files (e.g., '*.bak' or "*$DatabaseName*.bak"). Default is '*.bak'.
.PARAMETER LogDir
 The directory where log files will be stored. Default is ".\Logs".
.PARAMETER WhatIf
 A switch to indicate whether to perform a WhatIf operation (simulates the actions without making changes)

.NOTES
- Requires dbatools on the Agent host.
- Exit code 0 = success; non‑zero = failure (Agent will treat as error).

.EXAMPLE
.\Publish-Db.ps1 -ServerName "localhost" -DatabaseName "tpch_test" -BackupRoot "D:\MSSQL\BACKUPS\MSI\tpch_test" -Steps "Restore,Swap" -WhatIf
.EXAMPLE
.\Publish-Db.ps1 -ServerName "localhost" -DatabaseName "tpch_test" -BackupRoot "D:\MSSQL\BACKUPS\MSI\tpch_test" -Steps "Restore"
.EXAMPLE
.\Publish-Db.ps1 -ServerName "localhost" -DatabaseName "tpch_test" -BackupRoot "D:\MSSQL\BACKUPS\MSI\tpch_test" -Steps "Swap"
#>

[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
    [string]$ServerName,

    [Parameter(Mandatory = $true)]
    [string]$DatabaseName,

    [Parameter(Mandatory = $true)]
    [string]$BackupRoot,              # Folder containing one or more backup files

    [Parameter(Mandatory = $false)]
    [string]$Steps = "Restore,Swap",  # Comma-separated list of steps to perform: Restore, Swap

    [Parameter(Mandatory = $false)]
    [string]$BackupFilter = '*.bak',  # e.g. '*.bak' or "*$DatabaseName*.bak"

    [Parameter(Mandatory = $false)]
    [string]$LogDir = ".\Logs"
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$LogFile = Join-Path -Path $LogDir -ChildPath ("PublishDb_{0:yyyyMMdd_HHmmss}.log" -f (Get-Date))   

function Write-Log {
    param([string]$Message)
    $stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    Write-Host "$stamp $Message"
}

$transcriptStarted = $false
try {
    Start-Transcript -Path $LogFile -Append -ErrorAction Stop
    $transcriptStarted = $true
    Write-Log "Publish-Db started."
    Write-Log "Parameters:"
    Write-Log "  ServerName: $ServerName"
    Write-Log "  DatabaseName: $DatabaseName"
    Write-Log "  BackupRoot: $BackupRoot"
    Write-Log "  BackupFilter: $BackupFilter"
    Write-Log "  LogDir: $LogDir"
    Write-Log "  Steps to perform: $Steps"

    Write-Log "Importing dbatools..."
    Import-Module dbatools -ErrorAction Stop

    # validate $Steps parameter
    $validSteps = @("Restore", "Swap")
    $stepsToPerform = $Steps.Split(",") | ForEach-Object { $_.Trim() }
    foreach ($step in $stepsToPerform) {
        if ($validSteps -notcontains $step) {
            Write-Log "Invalid step ${step} in Steps parameter."
            Write-Log "Valid steps are: $($validSteps -join ', ')"
            throw "Invalid step ${step} in Steps parameter. Valid steps are: $($validSteps -join ', ')"
        }
    } 

    # Try to connect to the target SQL instance
    Write-Log "Connecting to instance ${ServerName}..."
    $instance = Connect-DbaInstance -SqlInstance $ServerName -ErrorAction Stop
    Write-Log "Connected to $($instance.DomainInstanceName)"

    # Check if Restore step is to be performed
    if ($stepsToPerform -contains "Restore") 
    {

        ${suffixdatetime} = Get-Date -Format "yyyyMMdd_HHmmss"

        # Validate $Database backup parts (in BackupRoot) using a VerifyOnly and check for warnings
        Write-Log "Validating backup files for database ${DatabaseName} in ${BackupRoot}..."
        Restore-DbaDatabase `
                -SqlInstance $ServerName `
                -Path $BackupRoot `
                -DatabaseName ${DatabaseName}_Restore `
                -DestinationFileSuffix "_${suffixdatetime}" `
                -WithReplace `
                -EnableException `
                -ReplaceDbNameInFile `
                -ErrorAction Stop `
                -VerifyOnly `
                -ErrorVariable errors `

        $errorsCount = $errors.Count
        if ($errorsCount -gt 0) {   
            Write-Log "Backup validation failed with error(s):"
            
           $errors | ForEach-Object {
                Write-Log "  $_"
            }
            
                       
            throw "Backup validation failed with error(s). See log for details."
        }
        else {
            Write-Log "Backup validation succeeded with no errors."
        }

        # Check if there are any warnings in the verification results
        
        Write-Log "Backup validation completed."


       
        Write-Log "Restore step selected."
        $suffixdatetime = Get-Date -Format "yyyyMMdd_HHmmss"
        
        if ($PSCmdlet.ShouldProcess("$ServerName/$DatabaseName", "Restore from $BackupRoot using suffix ${DatabaseName}_Restore_${suffixdatetime}")) {
            Write-Log "Starting restore ${DatabaseName} into ${DatabaseName}_Restore..."
            Restore-DbaDatabase `
                -SqlInstance $ServerName `
                -Path $BackupRoot `
                -DatabaseName ${DatabaseName}_Restore `
                -DestinationFileSuffix "_${suffixdatetime}" `
                -WithReplace `
                -EnableException `
                -ReplaceDbNameInFile `
                -ErrorAction Stop `
                -ErrorVariable errors
                
            if( $errors.Count -gt 0 ) {
                Write-Log "Restore encountered error(s):"
                $errors | ForEach-Object {
                    Write-Log "  $_"
                }
                throw "Restore encountered error(s). See log for details."
            }
            else {
                Write-Log "Restore completed."
            }            
                
            
        }
        Write-Log "Restore ${DatabaseName}_Restore Done."
    }


    # Check if Swap step is to be performed
    if ($stepsToPerform -contains "Swap") 
    {
        Write-Log "Swap step selected."

        ## Now rename databases with -Force
        # 1 ) Rename the original database as ${DatabaseName}_temp
        # 2 ) Rename rename the restored database ${DatabaseName}_restore to ${DatabaseName}
        # 3 ) Rename the temp database back to ${DatabaseName}_restore  
        Write-Log "Renaming databases..."
        
        if ($PSCmdlet.ShouldProcess("Renaming", "Renamed original database to ${DatabaseName}_temp")) {
        Rename-DbaDatabase `
            -SqlInstance $ServerName `
            -Database ${DatabaseName} `
            -DatabaseName ${DatabaseName}_temp `
            -Move `
            -Force `
            -ErrorAction Stop
        }
        Write-Log "Renamed original database to ${DatabaseName}_temp."
        
        if ($PSCmdlet.ShouldProcess("Renaming", "Renamed ${DatabaseName}_Restore to ${DatabaseName}.")) {
        Rename-DbaDatabase `
            -SqlInstance $ServerName `
            -Database ${DatabaseName}_Restore `
            -DatabaseName ${DatabaseName} `
            -Force `
            -Move `
            -ErrorAction Stop
        }
        Write-Log "Renamed ${DatabaseName}_Restore to ${DatabaseName}."

        if ($PSCmdlet.ShouldProcess("Renaming", "Renamed ${DatabaseName}_temp back to ${DatabaseName}_Restore.")) {
        Rename-DbaDatabase `
            -SqlInstance $ServerName `
            -Database ${DatabaseName}_temp `
            -DatabaseName ${DatabaseName}_Restore `
            -Force `
            -Move `
            -ErrorAction Stop
        }
        Write-Log "Renamed ${DatabaseName}_temp back to ${DatabaseName}_Restore."

        Write-Log "Renaming completed."  
    }

    Write-Log "Publish-Db completed for Step(s): $Steps"
    exit 0

}
catch {
    Write-Error $_
    Write-Log "Failed: $($_.Exception.Message)"
    exit 1
}
finally {
    if ($transcriptStarted) { Stop-Transcript | Out-Null }
}
