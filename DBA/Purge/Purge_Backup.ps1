<#
    .SYNOPSIS
        Recursively delete files in a specified directory based on their age in hours and file pattern.
    .DESCRIPTION
        Objective: This script allows you to remove files from a specified directory (including subdirectories) based on their last modified time. 
        You can specify the file pattern and the time range for the files to be deleted. It supports deleting files older than a certain number 
        of hours (using `purgeHoursLater`) or files that were modified between a specified time range (using `purgeHoursLater` and `purgeHoursEarlier` together).
        The script also supports a dry-run mode with the `-WhatIf` switch to preview the files that would be deleted without actually removing them.

        The script will:
        - Traverse the directory and its subdirectories recursively.
        - Filter files by the specified pattern.
        - Delete files that meet the conditions based on the specified `purgeHoursLater` or `purgeHoursEarlier`.
        
        The output will be a list of deleted files or a preview of which files would be deleted.

    .PARAMETER directory
        The directory containing the files to be purged. This directory will be searched recursively.

    .PARAMETER filePattern
        The file pattern to filter the files to be deleted (e.g., "*.log", "backup_*", "*.txt").

    .PARAMETER purgeHoursLater
        The number of hours old a file must be to be eligible for deletion. Files older than this will be deleted.

    .PARAMETER purgeHoursEarlier
        The number of hours ago up to the current time to filter files to be deleted. Files modified between these hours will be deleted.

    .PARAMETER whatIf
        If specified, the script will not delete any files but will print the paths of the files that would be deleted.

    .NOTES
        Tags: File Cleanup, Purge, File Management
        Author: Pierre-Antoine Collet
        Website: https://www.architecture-performance.fr/
        Copyright: (c) 2025 by Your Name, licensed under MIT
        License: MIT https://opensource.org/licenses/MIT
        
    .LINK
        https://www.architecture-performance.fr/

    .EXAMPLE
        .\Purge_Backup.ps1 -directory "C:\Logs" -filePattern "*.log" -purgeHoursLater 48
        This will delete all .log files older than 48 hours in the C:\Logs directory and its subdirectories.

    .EXAMPLE
        .\Purge_Backup.ps1 -directory "C:\Logs" -filePattern "*.log" -purgeHoursLater 48 -WhatIf
        This will display the files that would be deleted (but will not delete them), filtering by .log extension and older than 48 hours.

    .EXAMPLE
        .\Purge_Backup.ps1 -directory "C:\Logs" -filePattern "*.txt" -purgeHoursEarlier 72 -purgeHoursLater 24
        This will delete all .txt files that were modified between 24 and 72 hours ago in the C:\Logs directory and its subdirectories.
#>

param(
    [Parameter(Mandatory=$true)][string]$directory,
    [Parameter(Mandatory=$true)][string]$filePattern,
    [int]$purgeHoursLater,
    [int]$purgeHoursEarlier,
    [switch]$whatIf
)

# Check if the specified directory exists
if (-Not (Test-Path -Path $directory -PathType Container)) {
    Write-Host "The specified directory does not exist."
    exit
}

# Get the current time
$currentTime = Get-Date

# If purgeHoursLater is specified, calculate the cutoff for older files
if ($purgeHoursLater) {
    $purgeDateLater = $currentTime.AddHours(-$purgeHoursLater)
}

# If purgeHoursEarlier is specified, calculate the cutoff for files within the time window
if ($purgeHoursEarlier) {
    $purgeDateEarlier = $currentTime.AddHours(-$purgeHoursEarlier)
}

# Get all the files in the directory and subdirectories
Get-ChildItem -Path $directory -Recurse -File -Filter $filePattern | ForEach-Object {
    # Logic for files between purgeHoursLater and purgeHoursEarlier
    if($purgeHoursLater -and $purgeHoursEarlier){
        if($_.LastWriteTime -lt $purgeDateLater -and $_.LastWriteTime -gt $purgeDateEarlier){
            if ($whatIf) {
                Write-Host "Would delete (between $purgeHoursEarlier and $purgeHoursLater hours): $($_.FullName)"
            }
            else {
                try {
                    Remove-Item -Path $_.FullName -Force
                    Write-Host "Deleted: $($_.FullName)"
                }
                catch {
                    Write-Host "Error while deleting $($_.FullName): $_"
                }
            }
        }
    }
    # Logic for files older than purgeHoursLater
    elseif ($purgeHoursLater -and -not $purgeHoursEarlier -and $_.LastWriteTime -lt $purgeDateLater) {
        if ($whatIf) {
            Write-Host "Would delete (older than $purgeHoursLater hours): $($_.FullName)"
        }
        else {
            try {
                Remove-Item -Path $_.FullName -Force
                Write-Host "Deleted: $($_.FullName)"
            }
            catch {
                Write-Host "Error while deleting $($_.FullName): $_"
            }
        }
    }

    # Logic for files younger than purgeHoursEarlier (if only purgeHoursEarlier is specified)
    elseif ($purgeHoursEarlier -and -not $purgeHoursLater -and $_.LastWriteTime -gt $purgeDateEarlier) {
        if ($whatIf) {
            Write-Host "Would delete (younger than $purgeHoursEarlier hours): $($_.FullName)"
        }
        else {
            try {
                Remove-Item -Path $_.FullName -Force
                Write-Host "Deleted: $($_.FullName)"
            }
            catch {
                Write-Host "Error while deleting $($_.FullName): $_"
            }
        }
    }
    
} 
