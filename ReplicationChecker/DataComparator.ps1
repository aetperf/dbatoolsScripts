<#
.SYNOPSIS
Function DataComparator : Compare data between all tables of a publication
.DESCRIPTION
the process would be to dump the 2 tables, ordered by the primary keys and compare the hash of the files generated. we will use a sql native binary format for the files.
.PARAMETER SqlInstanceSource
Source Instance
.PARAMETER SqlInstanceTarget
Target Instance
.PARAMETER DatabaseSource
Source Database
.PARAMETER DatabaseTarget
Target Database
.PARAMETER PublicationName
Publication Name
.PARAMETER TempPath
Path to store the dump files
.PARAMETER DiffCount
Switch to return the number of lines with differences ($true) or just if the tables are identical or not ($false). Default : $false

.EXAMPLE
.\DataComparator.ps1 -SqlInstanceSource "FRQUISV31\SQL_CITECT" -SqlInstanceTarget "FRQUISR31\SQL_CITECT" -PublicationName "CONFIG" -DatabaseSource "QUI31" -DatabaseTarget "QUI31" -TempPath "D:\temp\"

.EXAMPLE
# Compare the data between the tables and return the number lines of differences
.\DataComparator.ps1 -SqlInstanceSource "FRQUISV31\SQL_CITECT" -SqlInstanceTarget "FRQUISR31\SQL_CITECT" -PublicationName "CONFIG" -DatabaseSource "QUI31" -DatabaseTarget "QUI31" -TempPath "D:\tem
p\" -DiffCount $true

#
# NOTES
# Author : Romain Ferraton
# Date : 2023-12-07
# Version : 1.0

#>

#Parameters
param(
    [Parameter(Mandatory=$true)]
    [string]$SqlInstanceSource,
    [Parameter(Mandatory=$true)]
    [string]$SqlInstanceTarget,
    [Parameter(Mandatory=$true)]
    [string]$PublicationName,
    [Parameter(Mandatory=$true)]
    [string]$DatabaseSource,
    [Parameter(Mandatory=$true)]
    [string]$DatabaseTarget,
    [Parameter(Mandatory=$false)]
    [string]$TempPath = "C:\temp\",
    [Parameter(Mandatory=$false)]
    [string]$DiffCount = $false
)

Import-Module -Name './DataComparator.psm1' -Force -NoClobber -WarningVariable WarningDataComparator -ErrorVariable ErrorDataComparator

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Main program
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Get all the article of a publication and check the differences between the source and target tables
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Get the articles of the publication
$articles = Get-PublicationArticles -serverInstance $SqlInstanceSource -PublicationName $PublicationName
$articlescount=$articles.count

# Initialize the variables to count the differences and for progress bar
$diff = 0
$tableindex = 0

#create a custom object to store the results with the schema name, the table name and the number of differences
$diffresult = @()

# Loop on the articles
foreach ($article in $articles) {

    $tableindex += 1
    # Get the table name
    $table = $article.source_object
    # Get the schema name
    $schema = $article.source_owner
    # Compare the data between the source and target tables
    $result = Compare-SqlTableDumps -SqlInstanceSource $SqlInstanceSource -SqlInstanceTarget $SqlInstanceTarget -DatabaseSource $DatabaseSource -SchemaSource $schema -DatabaseTarget $DatabaseTarget -SchemaTarget $schema -Table $table -TempPath $TempPath -DiffCount $DiffCount

    # store the result in the custom object
    $diffresult += [pscustomobject]@{
        SqlInstanceSource = $SqlInstanceSource
        SqlInstanceTarget = $SqlInstanceTarget
        DatabaseSource = $DatabaseSource
        DatabaseTarget = $DatabaseTarget
        PublicationName = $PublicationName
        Schema = $schema
        Table = $table
        DiffCount = $result
    }

    # Check the result
    if ($result -ge 1) {
        # if the result is greater or equal to 1, there are differences
        Write-Debug "${result} Differences found for table $schema.$table"        
        $diff += 1    
    }
    else {
        Write-Debug "No differences found for table $schema.$table"
    }
    # show progress bar
    Write-Progress -Activity "Comparing data between tables" -Status "Progress" -PercentComplete (($tableindex / $articlescount) * 100) 
    
}

Write-Debug "${diff} tables with differences found over ${articlescount} tables for the replication ${PublicationName}"


# Return the result : number of tables with differences found
return $diffresult

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of the program
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


