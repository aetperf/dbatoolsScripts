param
 (
     [Parameter(Position=0, Mandatory=$true)] [string]$SrcFolder,
     [Parameter(Position=1, Mandatory=$true)] [string]$TgtFolder
 )
 
# Robocopy command to copy files from source to target with options 
# /S - copy subdirectories, but not empty ones
# /XO - exclude older files
# /XC - exclude changed files
# /XN - exclude newer files
# /NP - no progress
# /R:10 - retry 10 times
# /W:15 - wait 15 seconds between retries
robocopy $SrcFolder $TgtFolder /S /XO /XC /XN /NP /R:10 /W:15
 
##set exit code using robocopy exit code and bit mask
$exit = $lastexitcode -band 24
 
exit $exit