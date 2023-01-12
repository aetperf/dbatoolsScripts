
<#
    Name:			FileGroupMove
    Date:			2023-01-12
    Name:           Romain Ferraton romain.ferraton [at] architecture-performance.fr
    Description:	Script to move SQL Server objects between filegroups
    
    Notes:			Does not migrate the following 
        			Partitioned tables
        			LOB objects
                    XML INDEX
                    CLUSTERED INDEX THAT HAVE XML INDEX BASED ON
    Notes:          Steps :
                    For Each Table
						- Disable Secondary Indexes if any heap or clustered should be moved
						- Move Heap or Clustered Index
						- Rebuild Secondary Index

    Evolutions:
    2022-09-28 - Romain Ferraton - Add Parallel Capacities (Powershell 7+ mandatory)



    Prerequesites Powershell 7 and sql server module (as of date of writing 2022-09-28) thereis no easy way to install sqlserver module with powershell 7
    WorkAround :

    mkdir "C:\Program Files\PackageManagement\NuGet\Packages\"
    cd "C:\Program Files\PackageManagement\NuGet\Packages\"
    $rootpath = "C:\Program Files\PackageManagement\NuGet\Packages\"
    $sourceNugetExe = "https://dist.nuget.org/win-x86-commandline/latest/nuget.exe"
    $targetNugetExe = "$rootPath\nuget.exe"
    Invoke-WebRequest $sourceNugetExe -OutFile $targetNugetExe
    ./nuget install Microsoft.SqlServer.SqlManagementObjects # -version 150.18208.0     
    ./nuget install Microsoft.Data.SqlClient # -version 1.1.1
    Install-Module -Name SqlServer -AllowClobber

#>

# .\FileGroupsMoveV2.ps1 -server ".\DBA01" -dbName "AdventureWorks2019" -doWork $FALSE -onlineOpt $FALSE -tablesToMove "*" -schemaToMove "*" -TargetfileGroup "SECONDARY" -SourcefileGroup "*"
# .\FileGroupsMoveV2.ps1 -server "GRASRLYODTW1" -dbName "DWHD" -doWork $FALSE -onlineOpt $FALSE -tablesToMove "*" -schemaToMove "*" -TargetfileGroup "DATA" -SourcefileGroup "*"
param 
(
    [Parameter(Mandatory)] [string] $server = ".",
    [Parameter(Mandatory)] [string] $dbName,
    [Parameter(Mandatory)] [bool] $doWork = $FALSE, #safety net, true actually moves the data, false just outputs what the process will do
    [Parameter(Mandatory)] [bool] $onlineOpt = $FALSE, #request an online index move
    [Parameter(Mandatory)] [string] $tablesToMove = "*", # * is default, enter a matching string for example tableName*
    [Parameter(Mandatory)] [string] $schemaToMove = "*",
    [Parameter(Mandatory)] [string] $SourcefileGroup = "*",
    [Parameter(Mandatory)] [string] $TargetfileGroup = "SECONDARY"
		
)

<#
[string] $server = "GRASRLYODTW1"
[string] $dbName = "DWHGRC311220"
[bool] $doWork = $FALSE #safety net, true actually moves the data, false just outputs what the process will do
[bool] $onlineOpt = $FALSE #request an online index move
[string] $tablesToMove = "*" # * is default, enter a matching string for example tableName*
[string] $schemaToMove = "*"
[string] $SourcefileGroup = "*"
[string] $TargetfileGroup = "DATA"

#>


Write-host "Parameters======================================================================="
Write-host "Server = ${server}"
Write-host "dbName = ${dbName}"
Write-host "doWork = ${doWork}"
Write-host "onlineOpt = ${onlineOpt}"
Write-host "tablesToMove = ${tablesToMove}"
Write-host "schemaToMove = ${schemaToMove}"
Write-host "TargetfileGroup = ${TargetfileGroup}"
Write-host "Parameters======================================================================="


#[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMO")  #| out-null # Only Work with Powershell 5 (Not P7)
#[System.Reflection.Assembly]::LoadFile("C:\windows\Microsoft.Net\assembly\GAC_MSIL\Microsoft.SqlServer.SMO\v4.0_15.0.0.0__89845dcd8080cc91\Microsoft.SqlServer.SMO.dll")


Import-Module dbatools



$sqlServer = New-Object ('Microsoft.SqlServer.Management.Smo.Server') -argumentlist $server 
$db = $sqlServer.Databases | Where-Object { $_.Name -eq $dbName }
$onlineIndex = $FALSE




if ($db.Name -ne $dbName) {
    Write-Host('Database not found')
    return
}



$destFileGroup = ($db.FileGroups | Where-Object { $_.Name -eq $TargetfileGroup } )

#check to see if the destination file group exists
if ( $destFileGroup.State -ne "Existing") {
    Write-Host('Destination filegroup not found')
    return
}

Write-Host ('Database: ' + $db.Name)

#if SQL Server edition does not supports online indexing and the user requested it, turn it off
if ( $sqlServer.Information.EngineEdition -ne 'EnterpriseOrDeveloper' -and $onlineOpt -eq $TRUE ) {
    $onlineIndex = $FALSE
}

$query="WITH TSCRIPT AS
(SELECT I.object_id, I.index_id, 'CREATE ' +
       CASE 
            WHEN I.is_unique = 1 THEN 'UNIQUE '
            ELSE ''
       END +
       I.type_desc COLLATE DATABASE_DEFAULT + ' INDEX ' +
       QUOTENAME(I.name) + ' ON ' +
       QUOTENAME(SCHEMA_NAME(T.schema_id)) + '.' + QUOTENAME(T.name) + ' (' +
       KeyColumns + ') ' +
       ISNULL(' INCLUDE (' + IncludedColumns + ') ', '') +
       ISNULL(' WHERE  ' + I.filter_definition, '') + ' WITH ( ' +
	   'DROP_EXISTING = ON,'+
       CASE 
            WHEN I.is_padded = 1 THEN ' PAD_INDEX = ON '
            ELSE ' PAD_INDEX = OFF '
       END + ',' +
       'FILLFACTOR = ' + CONVERT(
           CHAR(5),
           CASE 
                WHEN I.fill_factor = 0 THEN 100
                ELSE I.fill_factor
           END
       ) + ',' +
       -- default value 
       'SORT_IN_TEMPDB = OFF ' + ',' +
       CASE 
            WHEN I.ignore_dup_key = 1 THEN ' IGNORE_DUP_KEY = ON '
            ELSE ' IGNORE_DUP_KEY = OFF '
       END + ',' +
       CASE 
            WHEN ST.no_recompute = 0 THEN ' STATISTICS_NORECOMPUTE = OFF '
            ELSE ' STATISTICS_NORECOMPUTE = ON '
       END + ',' +
       ' ONLINE = OFF ' + ',' +
       CASE 
            WHEN I.allow_row_locks = 1 THEN ' ALLOW_ROW_LOCKS = ON '
            ELSE ' ALLOW_ROW_LOCKS = OFF '
       END + ',' +
       CASE 
            WHEN I.allow_page_locks = 1 THEN ' ALLOW_PAGE_LOCKS = ON '
            ELSE ' ALLOW_PAGE_LOCKS = OFF '
       END + ' ) ON [__TartgetFileGroup__]' 
       -- +QUOTENAME(DS.name)
	   [CreateIndexScript]
FROM   sys.indexes I
       JOIN sys.tables T
            ON  T.object_id = I.object_id
       JOIN sys.sysindexes SI
            ON  I.object_id = SI.id
            AND I.index_id = SI.indid
       JOIN (
                SELECT *
                FROM   (
                           SELECT IC2.object_id,
                                  IC2.index_id,
                                  STUFF(
                                      (
                                          SELECT ' , ' + QUOTENAME(C.name) + CASE 
                                                                       WHEN MAX(CONVERT(INT, IC1.is_descending_key)) 
                                                                            = 1 THEN 
                                                                            ' DESC'
                                                                       ELSE 
                                                                            ' ASC'
                                                                  END
                                          FROM   sys.index_columns IC1
                                                 JOIN sys.columns C
                                                      ON  C.object_id = IC1.object_id
                                                      AND C.column_id = IC1.column_id
                                                      AND IC1.is_included_column = 
                                                          0
                                          WHERE  IC1.object_id = IC2.object_id
                                                 AND IC1.index_id = IC2.index_id
                                          GROUP BY
                                                 IC1.object_id,
                                                 C.name,
                                                 index_id
                                          ORDER BY
                                                 MAX(IC1.key_ordinal) 
                                                 FOR XML PATH('')
                                      ),
                                      1,
                                      2,
                                      ''
                                  ) KeyColumns
                           FROM   sys.index_columns IC2 
                           GROUP BY
                                  IC2.object_id,
                                  IC2.index_id
                       ) tmp3
            )tmp4
            ON  I.object_id = tmp4.object_id
            AND I.Index_id = tmp4.index_id
       JOIN sys.stats ST
            ON  ST.object_id = I.object_id
            AND ST.stats_id = I.index_id
       JOIN sys.data_spaces DS
            ON  I.data_space_id = DS.data_space_id
       JOIN sys.filegroups FG
            ON  I.data_space_id = FG.data_space_id
       LEFT JOIN (
                SELECT *
                FROM   (
                           SELECT IC2.object_id,
                                  IC2.index_id,
                                  STUFF(
                                      (
                                          SELECT ' , ' + C.name
                                          FROM   sys.index_columns IC1
                                                 JOIN sys.columns C
                                                      ON  C.object_id = IC1.object_id
                                                      AND C.column_id = IC1.column_id
                                                      AND IC1.is_included_column = 
                                                          1
                                          WHERE  IC1.object_id = IC2.object_id
                                                 AND IC1.index_id = IC2.index_id
                                          GROUP BY
                                                 IC1.object_id,
                                                 C.name,
                                                 index_id 
                                                 FOR XML PATH('')
                                      ),
                                      1,
                                      2,
                                      ''
                                  ) IncludedColumns
                           FROM   sys.index_columns IC2 

                           GROUP BY
                                  IC2.object_id,
                                  IC2.index_id
                       ) tmp1
                WHERE  IncludedColumns IS NOT NULL
            ) tmp2
            ON  tmp2.object_id = I.object_id
            AND tmp2.index_id = I.index_id
WHERE  1= 1 )
,
TObjIndex AS
(

SELECT o.object_id,i.index_id, QUOTENAME(SCHEMA_NAME(o.schema_id)) schema_name,QUOTENAME(OBJECT_NAME(o.object_id)) table_name,QUOTENAME(f.name) filegroup_name,i.type_desc, QUOTENAME(i.name) index_name,QUOTENAME(c.name) first_columnname
FROM sys.indexes i
INNER JOIN sys.filegroups f
ON i.data_space_id = f.data_space_id
INNER JOIN sys.all_objects o
ON i.[object_id] = o.[object_id] 
INNER JOIN sys.tables t ON t.object_id=o.object_id
INNER JOIN sys.columns c ON c.object_id=o.object_id AND c.column_id=1
WHERE 
1=1 AND 
o.type = 'U' AND
(SCHEMA_NAME(o.schema_id) LIKE @schema OR (@schema='*')) AND
(OBJECT_NAME(o.object_id) LIKE @table OR (@table='*')) AND
(f.name LIKE @sourcefg OR (@sourcefg='*')) AND
f.name <> @targetfg AND
i.type_desc <>'XML'
)
SELECT toi.*,ts.CreateIndexScript
FROM TObjIndex toi LEFT OUTER JOIN TSCRIPT ts ON ts.object_id=toi.object_id AND ts.index_id=toi.index_id"

$QueryParameters = @{schema = $schemaToMove; table = $tablesToMove; sourcefg = $SourcefileGroup; targetfg=$TargetfileGroup}

$QueryParameters


$ResultTablesIndex=Invoke-DbaQuery -SqlInstance $server -Database $dbName -Query $query -SqlParameter $QueryParameters 

$ResultTablesIndex | Format-Table

$tableindexcount = $ResultTablesIndex.Count
#confirmation of the move request
$confirmation = Read-Host "Are you sure you want to move the" $tableindexcount "objects listed above to the destination filegroup? (y/n)"
if ($confirmation -ne 'y') {
    Write-Host('No table nor index moved')
    break
}

$Tables = $ResultTablesIndex | Select-Object schema_name, table_name -Unique | Sort-Object schema_name, table_name



#Move Table by Table
foreach ( $table in $Tables ) 
{     
    #get a list of all non clustered indexes on this table
    $indexesNonClusteredToMove = $ResultTablesIndex.Where({$_.schema_name -eq $table.schema_name -And $_.table_name -eq $table.table_name -And $_.type_desc -like "NONCLUSTERED*"}) 
    #get other idex (cluster or heap)
    $DataToMove = $ResultTablesIndex.Where({$_.schema_name -eq $table.schema_name -And $_.table_name -eq $table.table_name -And $_.type_desc -notlike "NONCLUSTERED%"})


    #disable of nonclustered secondary indexes
    foreach ( $nonclusterindex in $indexesNonClusteredToMove )
    {
        $disableindexquery="ALTER INDEX "+$nonclusterindex.index_name + " ON " + $nonclusterindex.schema_name + "." + $nonclusterindex.table_name + " DISABLE;"
        Write-Host($disableindexquery)
        if ( $doWork -eq $TRUE ) {
            Invoke-DbaQuery -SqlInstance $server -Database $dbName -Query $disableindexquery
        }
    }

    foreach ( $object in $DataToMove )
    {
        if($object.type_desc -eq "HEAP")
        {
            $createtempindexquery = "CREATE CLUSTERED INDEX TempIndex ON " + $object.schema_name + "." + $object.table_name + "(" + $object.first_columnname +") ON [" + $TargetfileGroup+"]"
            $droptempindexquery = "DROP INDEX TempIndex ON " + $object.schema_name + "." + $object.table_name
            Write-Host($createtempindexquery)
            Write-Host($droptempindexquery)
            if ( $doWork -eq $TRUE ) {
                try {        
                    Invoke-DbaQuery -SqlInstance $server -Database $dbName -Query $createtempindexquery
                    Invoke-DbaQuery -SqlInstance $server -Database $dbName -Query $droptempindexquery
                }
                catch {
                    Write-Host ('Failed moving index ' + $object.schema_name + '.' + $object.table_name + ' to Filegroup ' + $TargetfileGroup + ' ' + $error[0].Exception.InnerException )
                    return
                }
            }
        }
        else {
            $RecreateQuery=$object.CreateIndexScript.Replace('__TartgetFileGroup__',$TargetfileGroup)
			Write-Host($RecreateQuery)
			if ( $doWork -eq $TRUE ) {
                try {        
                    Invoke-DbaQuery -SqlInstance $server -Database $dbName -Query $RecreateQuery
                }
                catch {
                    Write-Host ('Failed moving index ' + $object.schema_name + '.' + $object.table_name + ' to Filegroup ' + $TargetfileGroup + ' ' + $error[0].Exception.InnerException )
                    return
                }
            }
			
        }
    }

    #Rebuild disabled nonclustered secondary indexes
    foreach ( $nonclusterindex in $indexesNonClusteredToMove )
    {
        $RecreateQuery=$nonclusterindex.CreateIndexScript.Replace('__TartgetFileGroup__',$TargetfileGroup)
			Write-Host($RecreateQuery)
			if ( $doWork -eq $TRUE ) {
                try {        
                    Invoke-DbaQuery -SqlInstance $server -Database $dbName -Query $RecreateQuery
                }
                catch {
                    Write-Host ('Failed moving index ' + $nonclusterindex.schema_name + '.' + $nonclusterindex.table_name + ' to Filegroup ' + $TargetfileGroup + ' ' + $error[0].Exception.InnerException )
                    return
                }
            }
    }



}



