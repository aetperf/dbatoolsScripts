# Create the function to compare data between two tables using the dump of the 2 tables, ordered by the primary keys and compare the hash of the files. we will use a sql native binary format for the files.
function Export-SqlTable {
    param(
        [string]$serverInstance,
        [string]$database,
        [string]$tableName,
        [string]$schemaName = "dbo",
        [string]$primaryKeyColumnList,
        [string]$filePath
    )

    $query = "SELECT * FROM [$schemaName].[${tableName}] ORDER BY ${primaryKeyColumnList}"

    # Run the query and dump the data using bcp even if the table is empty
    bcp "$query" queryout $filePath -S $serverInstance -d $database -T -c

}

# Function to get primary key columns for a table
function Get-PrimaryKeyColumns {
        param(
            [string]$serverInstance,
            [string]$database,
            [string]$schemaName = "dbo",
            [string]$tableName
        )

        $query = "SELECT column_name 
                FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY' 
                    AND tc.table_name = '${tableName}'
                    AND tc.table_schema = '${schemaName}'
                    ORDER BY kcu.ordinal_position"

        Write-Debug $query

        #run the querie a dn retrieve the columns using dbatools Invoke-DbaQuery
        $columns = Invoke-DbaQuery -SqlInstance $serverInstance -Database $database -Query $query
        return ($columns.column_name -join ", ")
    }


function Compare-SqlTableDumps {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$SqlInstanceSource,
        [Parameter(Mandatory=$true)]
        [string]$SqlInstanceTarget,
        [Parameter(Mandatory=$true)]
        [string]$DatabaseSource,
        [Parameter(Mandatory=$true)]
        [string]$SchemaSource,
        [Parameter(Mandatory=$true)]
        [string]$DatabaseTarget,
        [Parameter(Mandatory=$true)]
        [string]$SchemaTarget,
        [Parameter(Mandatory=$true)]
        [string]$Table,
        [Parameter(Mandatory=$false)]
        [string]$TempPath = "C:\temp\",
        [Parameter(Mandatory=$false)]
        [string]$DiffCount = $false
    )
    # Create the path for the dump files   
    $fileSource = $TempPath + $DatabaseSource + "_" + $Table + "_Source.bcp"
    $fileTarget = $TempPath + $DatabaseTarget + "_" + $Table + "_Target.bcp"

    # Get the primary key columns for the source table
    $primaryKeyColumnListSource = Get-PrimaryKeyColumns -serverInstance $SqlInstanceSource -database $DatabaseSource -schemaName $SchemaSource -tableName $Table
    # Get the primary key columns for the target table
    $primaryKeyColumnListTarget = Get-PrimaryKeyColumns -serverInstance $SqlInstanceTarget -database $DatabaseTarget -schemaName $SchemaTarget -tableName $Table
   
    # Check if primary key columns are the same
    if ($primaryKeyColumnListSource -ne $primaryKeyColumnListTarget) {
        Write-Error "Primary key columns are different between the source and target tables"
        return 1
    }

    # Dump the data from the source table
    $silentexpsourceresult=Export-SqlTable -serverInstance $SqlInstanceSource -database $DatabaseSource -schemaName $SchemaSource -tableName $Table -primaryKeyColumnList $primaryKeyColumnListSource -filePath $fileSource -ErrorAction Stop
    # Dump the data from the target table
    $silentexptargetresult=Export-SqlTable -serverInstance $SqlInstanceTarget -database $DatabaseTarget -schemaName $SchemaTarget -tableName $Table -primaryKeyColumnList $primaryKeyColumnListTarget -filePath $fileTarget -ErrorAction Stop
    # Compare the hash of the files   
    

    # get the number of lines with differences
    if ($DiffCount -eq $true) {
        # precheck if the files are empty using file number of lines of each file
        $fileSourceLineCount = (Get-Content $fileSource).Count
        $fileTargetLineCount = (Get-Content $fileTarget).Count    
        # if both files are empty, return 0 else return the size of the larger file
        if ($fileSourceLineCount -eq 0 -and $fileTargetLineCount -eq 0) {
            return 0
        }

        if ($fileSourceLineCount -eq 0 -or $fileTargetLineCount -eq 0) {
            #return the size of the larger file
            return [math]::Max($fileSourceLineCount,$fileTargetLineCount)
        }

        # Compare the files and return the number of lines with differences
        $diffcountresult = Compare-Object -ReferenceObject (Get-Content $fileSource) -DifferenceObject (Get-Content $fileTarget) | Measure-Object | Select-Object -ExpandProperty Count
        return $diffcountresult
    }
    else {
        $hashSource = Get-FileHash -Path $fileSource -Algorithm MD5
        $hashTarget = Get-FileHash -Path $fileTarget -Algorithm MD5
        if ($hashSource.Hash -eq $hashTarget.Hash) {
            return 0
        }
        else {
            return 1
        }
    }

    # Remove the dump files
    Remove-Item -Path $fileSource -Force
    Remove-Item -Path $fileTarget -Force
}

# create the function to get the articles get-publication-articles
function Get-PublicationArticles {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$serverInstance,
        [Parameter(Mandatory=$true)]
        [string]$PublicationName
    )
    # Run a query against the distribution database to get the articles og the publication
    $articles = Invoke-DbaQuery -SqlInstance $serverInstance -Database distribution -Query "SELECT a.source_owner , a.source_object FROM dbo.MSarticles a INNER JOIN dbo.MSpublications p ON p.publication_id = a.publication_id WHERE p.publication='$PublicationName'"

    return $articles
}