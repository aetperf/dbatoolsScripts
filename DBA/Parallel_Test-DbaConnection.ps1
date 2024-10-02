# Read the CSV file containing the list of the servers to compare
$IndusCheckList = Import-Csv -Path .\IndusCheckList.csv -Delimiter ","
$SqlInstanceLog = "FRQUIVCM35\MSDBA01"
$ReplicationCheckerDB = "ReplicationCheckerDB"

$IndusCheckList | ft -AutoSize

# Create a list to store the instances to check
$SqlInstancesToChecks = @()

# Get the list of the servers to compare from the SqlInstanceSource + SqlInstanceTarget columns
foreach ($item in $IndusCheckList) {
    $SqlInstancesToChecks += [PSCustomObject]@{SqlInstance = $item.SqlInstanceSource}
    $SqlInstancesToChecks += [PSCustomObject]@{SqlInstance = $item.SqlInstanceTarget}
}

Write-Host "List of the servers to compare :"
$SqlInstancesToChecks | ft -AutoSize

# Use a parallel loop to check the connection to the servers, limit to 8 threads
$SqlInstancesToChecks | ForEach-Object -ThrottleLimit 8 -Parallel {
    $SqlInstanceToCheck = $_.SqlInstance
    $WarningVar = @()

    # Try to connect to the target instance and check if the database exists
    try {
        $result = Test-DbaConnection -SqlInstance $SqlInstanceToCheck -SkipPSRemoting -ErrorAction Continue -WarningVariable "WarningVar" -WarningAction SilentlyContinue
        $IpAddress = $result.IPAddress
        $TcpPort = $result.TcpPort.ToString()

        ## get the last row of warning variable
        $WarningVar = $WarningVar[-1]
        
        if (!$result.ConnectSuccess) {
            Write-Host -ForegroundColor Red "$SqlInstanceToCheck : KO - IP=$IpAddress | Message : $WarningVar"
        }
        else {
            Write-Host -ForegroundColor Green "$SqlInstanceToCheck : OK - IP=$IpAddress | TCPPort=$TcpPort"
        }
        
    }
    catch {
        $WarningVar = $WarningVar[-1]
        $ErrorMessage = $_.Exception.Message
        Write-Host -ForegroundColor Red "$SqlInstanceToCheck : KO - $WarningVar - $ErrorMessage"
        continue
    }
}
