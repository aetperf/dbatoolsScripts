
# Read the CSV file containing the list of the servers to compare
$IndusCheckList = Import-Csv -Path .\IndusCheckList.csv -Delimiter ","

$IndusCheckList | ft -AutoSize

#Number of servers to compare
$DatabaseCount= $IndusCheckList.Count

$DatabaseIndex = 0

# Loop on the $IndusCheckList
foreach ($IndusCheck in $IndusCheckList) {

    $SqlInstanceSource=$IndusCheck.SqlInstanceSource
    $SqlInstanceTarget=$IndusCheck.SqlInstanceTarget
    $PublicationName=$IndusCheck.PublicationName
    $Database=$IndusCheck.Database

    Write-Host $SqlInstanceSource
    Write-Host $SqlInstanceTarget
    Write-Host $PublicationName
    Write-Host $Database
    

    # Write global progress
    Write-Progress -Activity "Compare" -Status "Progress -> " -PercentComplete (($DatabaseIndex / $DatabaseCount) * 100) -CurrentOperation "Database ${Database} processed" -Id 0

    # Try to connect to the target instance and check if the database exists
    try {
        $db = Get-SqlDatabase -ServerInstance $SqlInstanceTarget -Database $Database -ErrorAction Stop
        $result=.\ParallelDataComparator.ps1 -SqlInstanceSource $SqlInstanceSource -SqlInstanceTarget $SqlInstanceTarget -PublicationName $PublicationName -DatabaseSource $Database -DatabaseTarget $Database -TempPath "D:\temp\" -DiffCount $true
        $result | ft -AutoSize
    }
    catch {
        Write-Error "The database ${Database} does not exist on the target instance ${SqlInstanceTarget} or the target instance is not reachable."
        Write-Error $_.Exception.Message
        continue
    }
    $DatabaseIndex++    
}


