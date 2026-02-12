
<#
    Name:			Anthony E. Nocentino aen@centinosystems.com
    Date:			04/28/2015
    Name:           Romain Ferraton romain.ferraton [at] architecture-performance.fr
    Description:	Script to move SQL Server data between filegroups
    
    Notes:			Does not migrate the following 
        			Partitioned tables
        			LOB objects
    Notes:          Steps :
                    Step 1 : List objects to rebuild
                    Step 2 : disable non-clustered indexes
                    Step 3 : rebuild clustered indexes into the Target Filegroup
                    Step 4 : Rebuild Heaps into the Target Filegroup
                    Step 5 : Rebuild Non-Clustered indexes into the Target Filegroup

    Evolutions:
    2022-09-28 - Romain Ferraton - Add Parallel Capacities (Powershell 7+ mandatory)



    Prerequesites Powershell 7 and sql server module (as of date of writing 2022-09-28) thereis no easy way to install sqlserver module with powershell 7
    WorkAround :

    Open a Powershell 7 terminal as administrator and run the following orders
    mkdir "C:\Program Files\PackageManagement\NuGet\Packages\" -Force
    cd "C:\Program Files\PackageManagement\NuGet\Packages\"
    $rootpath = "C:\Program Files\PackageManagement\NuGet\Packages\"
    $sourceNugetExe = "https://dist.nuget.org/win-x86-commandline/latest/nuget.exe"
    $targetNugetExe = "$rootPath\nuget.exe"
    Invoke-WebRequest $sourceNugetExe -OutFile $targetNugetExe
    ./nuget install Microsoft.SqlServer.SqlManagementObjects     
    ./nuget install Microsoft.Data.SqlClient 
    Install-Module -Name SqlServer -AllowClobber -Force

#>

# .\FileGroupsMove.ps1 -server ".\DBA01" -dbName "RedGateMonitorCEGID" -doWork $TRUE -onlineOpt $FALSE -tablesToMove "*" -schemaToMove "data" -TargetfileGroup "DATA" -SourcefileGroup "*" -Degree 5
param 
(
    [Parameter(Mandatory)] [string] $server = ".",
    [Parameter(Mandatory)] [string] $dbName,
    [Parameter(Mandatory)] [bool] $doWork = $FALSE, #safety net, true actually moves the data, false just outputs what the process will do
    [Parameter(Mandatory)] [bool] $onlineOpt = $FALSE, #request an online index move
    [Parameter(Mandatory)] [string] $tablesToMove = "*", # * is default, enter a matching string for example tableName*
    [Parameter(Mandatory)] [string] $schemaToMove = "*",
    [Parameter(Mandatory)] [string] $SourcefileGroup = "*",
    [Parameter(Mandatory)] [string] $TargetfileGroup = "SECONDARY",
    [Parameter(Mandatory)] [Int16] $Degree = 1
		
)

<#  Testing 

    [string] $server = ".\DBA01"
    [string] $dbName = "RedGateMonitorCEGID"
    [bool] $doWork = $TRUE #safety net, true actually moves the data, false just outputs what the process will do
    [bool] $onlineOpt = $FALSE #request an online index move
    [string] $tablesToMove = "*" # * is default, enter a matching string for example tableName*
    [string] $schemaToMove = "alert"
    [string] $SourcefileGroup = "*"
    [string] $TargetfileGroup = "DATA"
    [Int16] $Degree = 4

#>



Write-host "INFO - Parameters======================================================================="
Write-host "INFO - Server = ${server}"
Write-host "INFO - dbName = ${dbName}"
Write-host "INFO - doWork = ${doWork}"
Write-host "INFO - onlineOpt = ${onlineOpt}"
Write-host "INFO - tablesToMove = ${tablesToMove}"
Write-host "INFO - schemaToMove = ${schemaToMove}"
Write-host "INFO - TargetfileGroup = ${TargetfileGroup}"
Write-host "INFO - Degree = ${Degree}"
Write-host "INFO - Parameters======================================================================="


Import-Module SqlServer

$sqlServer = New-Object ('Microsoft.SqlServer.Management.Smo.Server') -argumentlist $server

#Reduce impact of loading metadata
$sqlServer.SetDefaultInitFields($FALSE)

$db = $sqlServer.Databases | Where-Object { $_.Name -eq $dbName }
$onlineIndex = $FALSE
$tableCount = 0 #simple counter for tables

if ($db.Name -ne $dbName) {
    Write-Output('ERROR - Database not found')
    return
}



$destFileGroup = ($db.FileGroups | Where-Object { $_.Name -eq $TargetfileGroup } )

#check to see if the destination file group exists
if ( $destFileGroup.State -ne "Existing") {
    Write-Output('ERROR - Destination filegroup not found')
    return
}

Write-Output ('INFO - Database Found : ' + $db.Name)

#if edition supports online indexing and the user requested it, turn it on
if ( $sqlServer.Information.EngineEdition -eq 'EnterpriseOrDeveloper' -and $onlineOpt -eq $TRUE ) {
    $onlineIndex = $TRUE
}

#all tables that are not paritioned, that meet our search criteria specified as cmd line parameters
$TablesEnum = $sqlServer.Databases["${dbName}"].Tables.GetEnumerator()
$tables = $TablesEnum | Where-Object { ($_.Name -like $tablesToMove) -and ($_.Schema -like $schemaToMove) -and ($_.IsPartitioned -eq $FALSE) -and ($_.FileGroup -like $SourcefileGroup) } | Select Name, Schema, FileGroup, HasClusteredIndex, Indexes

Write-Output("INFO - Found "+ $tables.count + " Tables")

#$tables = $db.Tables | Where-Object { $_.Name -like $tablesToMove -and $_.Schema -like $schemaToMove -and $_.IsPartitioned -eq $FALSE -and $_.FileGroup -like $SourcefileGroup }

$indexesClusteredToMove = @()
$indexesNonClusteredToMove = @()
$heapsToMove = @()


#build a list of tables to be moved
foreach ( $table in $tables ) {     
    #get a list of all indexes on this table
    $indexes = $table.Indexes 

    #iterate over the set of indexes
    foreach ( $index in $indexes ) {
        #$itype= $index.IndexType 
        #Write-Host "${index} is a ${itype}"
        #if this table is a clustered or Non-Clustered index. Ignore special index types.
        if ( $index.IndexType -ne "HeapIndex") {
            if ( $index.IndexType -eq "ClusteredIndex" -or $index.IndexType -eq "ClusteredColumnStoreIndex" ) {
                if ( $index.FileGroup -ne $TargetfileGroup ) {
                    Write-Output( 'INFO - CLUSTERED INDEX : ' + $table.Schema + '.' + $table.Name + " " + $index.Name)
                    $tableCount++
                    $indexesClusteredToMove += $index
                }
            }
            else { # non clustered indexes
                if ( $index.FileGroup -ne $TargetfileGroup ) {
                    Write-Output( 'INFO - NON CLUSTERED INDEX : ' + $table.Schema + '.' + $table.Name + " " + $index.Name)
                    $tableCount++
                    $indexesNonClusteredToMove += $index
                }
            }
        }
  
        
    }
    if ($table.HasClusteredIndex -eq $FALSE -and $table.FileGroup -ne $TargetfileGroup) {
        Write-Output( 'INFO - HEAP : ' + $table.Schema + '.' + $table.Name)
        $tableCount++
        $heapsToMove += $table
    }
}

Write-Output( "INFO - Found "+ $indexesClusteredToMove.count + " Clustered Indexes To Move")
Write-Output( "INFO - Found "+ $indexesNonClusteredToMove.count+ " Non Clustered Indexes To Move")
Write-Output( "INFO - Found "+ $heapsToMove.count + " Heaps To Move")

#confirmation of the move request
$confirmation = Read-Host "Are you sure you want to move the" $tableCount "objects listed above to the destination filegroup? (y/n)"
if ($confirmation -ne 'y') {
    Write-Output('No tables moved')
    return
}


#Deactivate NonClusteredToMove
foreach ( $index in $indexesNonClusteredToMove ) {
    try {
        Write-Output ('INFO - Deactivate Non clustered index: ' + $index.Parent + '.['+ $index.Name +']')
        
        if ( $doWork -eq $TRUE ) {
            $index.Disable()
        }
    }
    catch {
        Write-Output ('ERROR - Failed Disabling index : ' + $index.Parent + '.['+ $index.Name +'] :' + $_ + $error[0].Exception.InnerException )
        return
    }
}#end for each index


########################################################################################################
##                                                                                                    ##
##                                                                                                    ##
## REBUILD PART : Using Parallel Rebuild will force to open several connections                       ##
##                                                                                                    ##
##                                                                                                    ##
########################################################################################################

[int]$Batch = 0
[int]$BatchSize = [int]$Degree*50

Write-Host("INFO - Starting Clustered Index Rebuild by Batch of " + $BatchSize )
# parallel rebuild clustered index (Batched to avoid memory saturation)


do 
{
     # Create an arrary with limited number of objects in it for memory management
     $ObjectsProcessing = [System.Collections.Generic.List[System.Object]]::new()
     for ($i = $Batch; (($i -lt ($Batch + $BatchSize)) -and ($i -lt $indexesClusteredToMove.count)); $i++) 
     {
         $ObjectsProcessing.add($indexesClusteredToMove[$i])
     }
     $ObjectsProcessing | Format-Table

    $duration=(Measure-Command{$ClusteredIndexlogs = $ObjectsProcessing | ForEach-Object -Parallel{
        $i_tfg=$using:TargetfileGroup
        $i_server=$using:server
        $i_db=$using:dbName
        $i_indexname = $_.Name
        $i_tablename = $_.Parent.Name
        $i_schema = $_.Parent.Schema
        try {
            $new_server=New-Object('Microsoft.SqlServer.Management.Smo.Server') -argumentlist $i_server # Open a new connection
        }
        catch {
            Write-Output ("ERROR - Failed to Connect: " + $_  + $error[0].Exception.InnerException )
            return
        }
       
        try {
            $i_index = $new_server.Databases["${i_db}"].Tables[$i_tablename,$i_schema].Indexes[$i_indexname] 
        }
        catch {
            Write-Output ("ERROR - Cannot Access Index : " + $_  + $error[0].Exception.InnerException )
            return
        }

        if ($i_index.count -eq 1)
        {
            try {
                $i_index.FileGroup = $i_tfg
                Write-Output ("INFO - Moving: ["+ $i_index.Parent.Schema +"].[" + $i_index.Parent.Name + "].["+ $i_index.Name +"] To Filegroup [" + $i_index.FileGroup + "]")
                
                if ( $using:doWork -eq $TRUE ) {
                    if ($_.isOnlineRebuildSupported ) {$_.OnlineIndexOperation = $using:onlineIndex}
                    $i_index.Recreate()
                }
            }
            catch {
                Write-Output ("ERROR - Failed Moving: ["+ $i_index.Parent.Schema +"].[" + $i_index.Parent.Name + "].["+ $i_index.Name +"] To Filegroup [" + $i_index.FileGroup + "]")
                return
            }
        } 
        
        
    } -ThrottleLimit $Degree -UseNewRunspace
    } ).seconds

    $ClusteredIndexlogs 

    [int]$BatchNext=$Batch + $BatchSize

    Write-Host("INFO - Completed Clustered Index Rebuild [Batch "+$Batch + "-"+$BatchNext+"]:"+ ${duration} +"s")

        
    $Batch = $BatchNext
} while ($Batch -lt $indexesClusteredToMove.count)

#if we didn't find a clustered index after looking at all the indexes, it's a heap. Let's move that too
<#
        our algortihm is as follows
        Find the leading column on the table

        Instantiate a new Smo.Index object on the table named "TableName_tempindex"
        Instantiate a new Smo.IndexesColumn object on leading column and add it to our TempIndex
        Set the Index as IsClustered
        Add the column to the index
        set the target filegroup to the tempindex
        Create the tempindex
        Drop the tempindex

    #>

    
$Batch = 0

Write-Host("INFO - Starting Heap Rebuild by Batch of " + $BatchSize )


do {
    # Create an arrary with limited number of objects in it for memory management
    $ObjectsProcessing = [System.Collections.Generic.List[object]]::new()
    for ($i = $Batch; (($i -lt ($Batch + $BatchSize)) -and ($i -lt $heapsToMove.count)); $i++) {
        $ObjectsProcessing.add($heapsToMove[$i])
    }
    $duration=(Measure-Command{$Heaplogs = $ObjectsProcessing | ForEach-Object -Parallel {
        $i_tfg=$using:TargetfileGroup
        $i_server=$using:server
        $i_db=$using:dbName
        $i_tablename = $_.Name
        $i_schema = $_.Schema
        $i_tempindexname = $i_tablename + '_tempindex'
        $new_server=New-Object ('Microsoft.SqlServer.Management.Smo.Server') -argumentlist $i_server 

        $i_table = $new_server.Databases[$i_db].Tables[$i_tablename,$i_schema] 

        $cols = $i_table.Columns[0]
        $leadingCol = $cols[0].Name
        
        $idx = New-Object -TypeName Microsoft.SqlServer.Management.SMO.Index -argumentlist $i_table, $i_tempindexname
        $icol1 = New-Object -TypeName Microsoft.SqlServer.Management.SMO.IndexedColumn -argumentlist $idx, $leadingCol, $true
        $idx.IsClustered = $TRUE
        $idx.IndexedColumns.Add($icol1)
        $idx.FileGroup = $i_tfg

        #Write-Output $idx.Script()

        #check to see if the table is not already in the destination filegroup and the table is indexable 
        if ( $_.FileGroup -ne $i_tfg -and $_.IsIndexable -eq $TRUE) {
            try {
                Write-Output('INFO - Moving Heap : [' + $i_table.Schema +'].['+ $i_tablename+']')
                if ( $using:doWork -eq $TRUE ) {
                    $idx.OnlineIndexOperation = $using:onlineIndex
                    $idx.Create()
                    $idx.OnlineIndexOperation = $using:onlineIndex
                    $idx.Drop()
                }
            }
            catch {
                Write-Output('ERROR - Failed moving heap : [' + $i_table.Schema +'].[' + $i_tablename + '] to [' + $i_tfg + '] : ' +$_ + $error[0].Exception.InnerException )
                Write-Output('ERROR - Remove any Tempory indexes created')
                return
            }
        }
        else {
            Write-Output('INFO - '+ $_.Name + ' is already in destination filegroup')
        }
    } -ThrottleLimit $Degree
    }).Seconds

    $Heaplogs

    Write-Host("INFO - Completed Heap Rebuild : ${duration} s")
    #[gc]::Collect() # garbage collection to recover memory
    $Batch = $Batch + $BatchSize
}while ($Batch -lt $heapsToMove.count)


$Batch=0

Write-Host("INFO - Starting Non Clustered Index Rebuild by Batch of " + $BatchSize )
# parallel rebuild clustered index (Batched to avoid memory saturation)

do {
    # Create an arrary with limited number of objects in it for memory management
    $ObjectsProcessing = [System.Collections.Generic.List[object]]::new()
    for ($i = $Batch; (($i -lt ($Batch + $BatchSize)) -and ($i -lt $indexesNonClusteredToMove.count)); $i++) {
        $ObjectsProcessing.add($indexesNonClusteredToMove[$i])
    }
#Parallel Rebuild NonClusteredToMove into the new target filegroup
    $duration=(Measure-command{$SecondaryIndexLogs = $ObjectsProcessing | ForEach-Object -Parallel {
        $i_tfg=$using:TargetfileGroup
        $i_server=$using:server
        $i_db=$using:dbName
        $i_indexname = $_.Name
        $i_tablename = $_.Parent.Name
        $i_schema = $_.Parent.Schema
        $new_server=New-Object ('Microsoft.SqlServer.Management.Smo.Server') -argumentlist $i_server     
        
        #$i_index = $i_server.Databases[$i_db].Tables.Indexes | Where-Object{($_.Name -eq $i_indexname) -and ($_.Parent -eq $i_tablename) }
        $i_index = $new_server.Databases[$i_db].Tables[$i_tablename,$i_schema].Indexes[$i_indexname] 
        
        if ($i_index.count -eq 1 -And -Not($i_indexname -eq ''))
        {
            try {
                
                $i_index.FileGroup = $i_tfg
                Write-Output ('INFO - Moving: ['+ $i_index.Parent.Schema +'].[' + $i_index.Parent.Name + '].['+ $i_index.Name +'] To Filegroup [' + $i_index.FileGroup + ']')
                

                if ( $using:doWork -eq $TRUE ) {
                    if ($_.isOnlineRebuildSupported ) {$_.OnlineIndexOperation = $using:onlineIndex}
                    $i_index.Recreate()
                }
            }
            catch {
                Write-Output ('ERROR - Failed Moving: ['+ $i_index.Parent.Schema +'].[' + $i_index.Parent.Name + '].['+ $i_index.Name +'] To Filegroup [' + $i_index.FileGroup + ']' + $_  + $error[0].Exception.InnerException )
                return
            }
        }
    } -ThrottleLimit $Degree
    }).Seconds



    $SecondaryIndexLogs

    Write-Host("Completed Secondary Index Rebuild : ${duration} s")
    #[gc]::Collect() # garbage collection to recover memory
    $Batch = $Batch + $BatchSize
}
while ($Batch -lt $indexesNonClusteredToMove.count)

#spit out data about data file size and allocation
$db.Refresh()
$tables.Refresh()

$dbfileGroup = $db.FileGroups 
Write-Output('Filegroup contents')
Write-Output($dbfileGroup.Files | Sort-Object -Property ID |  Format-Table Parent, ID, FileName, Name, Size, UsedSpace)

Write-Output('Tables')
Write-Output($tables | Select-Object Parent, Schema, Name, FileGroup | Format-Table )