# use the .\Profiling_Column_V2.sql sql query to get all orders for profiling and use a powershell parallel foreach loop to run the query for each column in the database
# set the target database and the profile database and the number of threads in parallel
# the output will be store in  profile table 

# CREATE TABLE profiles(
#	database_name nvarchar(256) NOT NULL,
#	table_full_name nvarchar(256) NOT NULL,
#	colname nvarchar(256) NOT NULL,
#	profile varchar(50) NOT NULL,
#	val nvarchar(4000) NULL
# )
param (
    [string]$SQLProfileInstance = "localhost",
    [string]$ProfileDatabase = "ProfileDB",
    [string]$SQLTargetInstance = "localhost",
    [string]$TargetDatabase,
    [int]$Threads = 1
)

$mainsw = [System.Diagnostics.Stopwatch]::StartNew()
$mainsw.Start()
# Load the SQL Server module
Import-Module SQLServer

# Set the SQL query
$SQLQuery = Get-Content ".\Profiling_Column_V2.sql"

# Set the connection string
$TargetConnectionString = "Server=$SQLTargetInstance;Database=$TargetDatabase;Integrated Security=True;"
$ProfileConnectionString = "Server=$SQLProfileInstance;Database=$ProfileDatabase;Integrated Security=True;"
# Set the connection
$TargetConnection = New-Object System.Data.SqlClient.SqlConnection
$TargetConnection.ConnectionString = $TargetConnectionString

# stop watch to measure the time


try {
    
    $TargetConnection.Open()

    $TargetCommand = New-Object System.Data.SqlClient.SqlCommand
    $TargetCommand.Connection = $TargetConnection
    $TargetCommand.CommandText = $SQLQuery
    #retrieve all sql orders from the SQL query in a datatable
    $SQLProfileOrders = New-Object System.Data.DataTable
    $SQLProfileOrders.Load($TargetCommand.ExecuteReader())

    # Delete the profile table for the where [database_name]=$TargetDatabase
    $ProfileConnection = New-Object System.Data.SqlClient.SqlConnection
    $ProfileConnection.ConnectionString = $ProfileConnectionString
    $ProfileConnection.Open()
    $ProfileCommand = New-Object System.Data.SqlClient.SqlCommand   
    $ProfileCommand.Connection = $ProfileConnection
    $ProfileCommand.CommandText = "DELETE FROM dbo.profiles WHERE [database_name]='$TargetDatabase'"
    $DeletedRows=$ProfileCommand.ExecuteNonQuery()
    write-host "Deleted $DeletedRows rows from dbo.profiles where [database_name]=$TargetDatabase"
    $ProfileConnection.Close()



    # use a parallel foreach loop to run the query for each column in the database
    $SQLProfileOrders | ForEach-Object -Parallel {
        #write-host $_["profile_command"]
        $TargetConnection = New-Object System.Data.SqlClient.SqlConnection
        $TargetConnectionString = "Server=$using:SQLTargetInstance;Database=$using:TargetDatabase;Integrated Security=True;"
        $ProfileConnectionString = "Server=$using:SQLProfileInstance;Database=$using:ProfileDatabase;Integrated Security=True;"
        $TargetConnection = New-Object System.Data.SqlClient.SqlConnection
        $TargetConnection.ConnectionString = $TargetConnectionString
        $ProfileConnection = New-Object System.Data.SqlClient.SqlConnection
        $ProfileConnection.ConnectionString = $ProfileConnectionString
        
        $ProfileConnection.Open()
        $TargetConnection.Open()
        $profileselect = $_["profile_command"]
        $TargetCommand = New-Object System.Data.SqlClient.SqlCommand 
        $TargetCommand.Connection = $TargetConnection
        $TargetCommand.CommandText = $profileselect 
        $TargetFullTableName = $_["database_name"]+"."+$_["schema_name"]+"."+$_["table_name"]
        $ProfileType = $_["profile"]
         

       #SQL Adapter - get the results using the SQL Command
        $sqlAdapter = new-object System.Data.SqlClient.SqlDataAdapter $TargetCommand
        $dataresulttable = new-object System.Data.DataTable        
        #fill the datatable with the results and measure elapsed time using the stopwatch
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $sw.Start()  
        $sqlAdapter.SelectCommand.CommandTimeout = 3600 
        
        try {
            $silentres=$sqlAdapter.Fill($dataresulttable) 
        }
        catch {
            write-host "Error for ${TargetFullTableName} for the profile ${ProfileType}"
            #Write-Host $_.Exception.Message
            Throw $_.Exception.Message
            
        }
        

        $sw.Stop()
        write-host "${TargetFullTableName} Profiled for the profile ${ProfileType} in " $sw.Elapsed.TotalSeconds"s"

        
        #write the resultset into tjhe profile table
        $ProfileCommand = New-Object System.Data.SqlClient.SqlCommand
        $ProfileCommand.Connection = $ProfileConnection
        #insert the results into the profile table
        $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $ProfileConnection
        #Define the destination table 
        
        $bulkCopy.DestinationTableName = "dbo.profiles"

        #bulk copy the data
        $bulkCopy.WriteToServer($dataresulttable)

        $sqlAdapter.Dispose() 
        $dataresulttable.Dispose()  
        $ProfileConnection.Close()   
        $TargetConnection.Close()

    } -ThrottleLimit $Threads



}
catch {
        
    write-host $profileselect
    Write-Host $_.Exception.Message

}
finally {
     
    $TargetConnection.Close()
    $mainsw.Stop()
    write-host "Total Elapsed Time: " $mainsw.Elapsed.TotalSeconds"s"
}



# Set the number of threads

