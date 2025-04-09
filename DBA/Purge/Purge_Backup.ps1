<#
    .SYNOPSIS
        Recursively delete files in a specified directory older than a given number of hours based on file extension.
    .DESCRIPTION
        Objective: This script allows you to remove files from a specified directory (including subdirectories) that are older than a certain number of hours. 
        You can specify the file extension for filtering the files to be deleted. The script also supports a dry-run mode with the -WhatIf switch 
        to preview the files that would be deleted without actually removing them.

        The script will:
        - Traverse the directory and its subdirectories recursively.
        - Filter files by the specified extension.
        - Delete files older than the specified number of hours (unless the -WhatIf flag is set).
        
        The output will be a list of deleted files or a preview of which files would be deleted.

    .PARAMETER directory
        The directory containing the files to be purged. This directory will be searched recursively.

    .PARAMETER fileExtension
        The file extension to filter the files to be deleted (e.g., ".log", ".txt").

    .PARAMETER purgeHours
        The number of hours old a file must be to be eligible for deletion.

    .PARAMETER whatIf
        If specified, the script will not delete any files but will print the paths of the files that would be deleted.

    .NOTES
        Tags: File Cleanup, Purge, File Management
        Author: Pierre-Antoine Collet
        Website: 
        Copyright: (c) 2025 by Pierre-Antoine Collet, licensed under MIT
        License: MIT https://opensource.org/licenses/MIT
        
    .LINK
        https://www.architecture-performance.fr/

    .EXAMPLE
        .\Purge_Backup.ps1 -directory "C:\Logs" -fileExtension ".log" -purgeHours 48
        This will delete all .log files older than 48 hours in the C:\Logs directory and its subdirectories.

    .EXAMPLE
        .\Purge_Backup.ps1 -directory "C:\Logs" -fileExtension ".log" -purgeHours 48 -WhatIf
        This will display the files that would be deleted (but will not delete them), filtering by .log extension and older than 48 hours.

    .EXAMPLE
        .\Purge_Backup.ps1 -directory "C:\Logs" -fileExtension ".txt" -purgeHours 72
        This will delete all .txt files older than 72 hours in the C:\Logs directory and its subdirectories.

    .EXAMPLE
        .\Purge_Backup.ps1 -directory "C:\Logs" -fileExtension ".log" -purgeHours 24 -WhatIf
        This will preview (but not delete) all .log files that are older than 24 hours in the C:\Logs directory and its subdirectories.
#>


param(
    [Parameter(Mandatory=$true)][string]$directory,
    [Parameter(Mandatory=$true)][string]$fileExtension,
    [Parameter(Mandatory=$true)][int]$purgeHours,
    [switch]$whatIf
)

# Check if the specified directory exists
if (-Not (Test-Path -Path $directory -PathType Container)) {
    Write-Host "The specified directory does not exist."
    exit
}

# Calculate the cutoff date by subtracting the specified number of hours from the current time
$purgeDate = (Get-Date).AddHours(-$purgeHours)

# Get all the files in the directory and subdirectories
Get-ChildItem -Path $directory -Recurse -File -Filter "*$fileExtension" | ForEach-Object {
    # Check if the file's last modified time is older than the purge date
    if ($_.LastWriteTime -lt $purgeDate) {
        if ($whatIf) {
            # If -WhatIf is specified, just print the files that would be deleted
            Write-Host "Would delete: $($_.FullName)"
        }
        else {
            try {
                # If -WhatIf is not specified, delete the file
                Remove-Item -Path $_.FullName -Force
                Write-Host "Deleted: $($_.FullName)"
            }
            catch {
                Write-Host "Error while deleting $($_.FullName): $_"
            }
        }
    }
    
}
