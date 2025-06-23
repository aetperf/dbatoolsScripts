<#
.SYNOPSIS
    Restore a schema from a source SQL Server database to a target database, including tables, indexes, constraints, and triggers.

.DESCRIPTION
    This script performs a full restore of all objects within a specific schema from a source SQL Server database to a target database. 
    Each step is logged in a dedicated logging database table, with detailed success or error codes.
    Execution can be done in parallel using `-Parallel`, and optionally stopped on first error.

.PARAMETER SqlInstance
    SQL Server instance hosting both source and target databases.

.PARAMETER SourceDB
    Name of the source database containing the schema and data to restore.

.PARAMETER TargetDB
    Name of the target database where the schema and data will be restored.

.PARAMETER SchemaName
    Name of the schema to restore (must exist in both source and target).

.PARAMETER LogInstance
    SQL Server instance hosting the logging database.

.PARAMETER LogDatabase
    Name of the logging database where restore operation logs will be written (tables : RestoreSchemaLog and RestoreSchemaLogDetail).

.PARAMETER Parallel
    Number of parallel threads used for table processing. Default is 1 (sequential).

.PARAMETER WhatIf
    If set, simulates the execution without actually performing changes.

.PARAMETER ContinueOnError
    If set, continues the restore process for all tables even if an error occurs on one.

.PARAMETER LogLevel
    Logging verbosity level. Options: DEBUG, INFO, ERROR. Default is INFO.

.NOTES
    Tags: SchemaRestore, SQLServer, Migration, DevOps
    Author: Pierre-Antoine Collet
    Copyright: (c) 2025, licensed under MIT License
    License: MIT https://opensource.org/licenses/MIT

    Dependencies:
        Install-Module dbatools
        Install-Module Logging

    Compatibility: Windows PowerShell 5.1+ or PowerShell Core 7+

.LINK
    

.EXAMPLE
    .\restoreSchema.ps1 -SqlInstance "MyServer\SQL01" -SourceDB "HR2022" -TargetDB "HR_RESTORE" -SchemaName "HumanResources" -LogInstance "MyServer\SQL01" -LogDatabase "RestoreLogs"

    This restores the entire HumanResources schema from HR2022 to HR_RESTORE using single-threaded execution, and logs to RestoreLogs.

.EXAMPLE
    .\restoreSchema.ps1 -SqlInstance "MyServer\SQL01" -SourceDB "HR2022" -TargetDB "HR_RESTORE" -SchemaName "HumanResources" -LogInstance "MyServer\SQL01" -LogDatabase "RestoreLogs" -Parallel 4 -ContinueOnError

    This restores the HumanResources schema in parallel using 4 threads, and continues processing even if some tables fail.
#>

param 
(
    [Parameter(Mandatory)] [string] $SqlInstance,
    [Parameter(Mandatory)] [string] $SourceDB,
    [Parameter(Mandatory)] [string] $TargetDB,
    [Parameter(Mandatory)] [string] $SchemaName,
    [Parameter(Mandatory)] [string] $LogInstance,
    [Parameter(Mandatory)] [string] $LogDatabase,
    [Parameter()] [int] $Parallel = 1,
    [Parameter()] [switch] $WhatIf,
    [Parameter()] [switch] $ContinueOnError,
    [Parameter()] [ValidateSet("DEBUG", "INFO", "ERROR")] [string] $LogLevel = "INFO"
)

$null = Set-DbatoolsInsecureConnection

$start = Get-Date
$RestoreStartDatetime = $start.ToString("yyyy-MM-dd HH:mm:ss")
$RestoreId = [DateTime]::UtcNow.Ticks
$ErrorCode = 0


# Initialize logging
Set-LoggingDefaultLevel -Level $LogLevel
Add-LoggingTarget -Name Console -Configuration @{
    ColorMapping = @{
        DEBUG = 'Gray'
        INFO  = 'White'
        ERROR = 'DarkRed'
    };
    
}

Write-Log -Level INFO -Message "SQL instance: $SqlInstance"
Write-Log -Level INFO -Message "Source DB: $SourceDB | Target DB: $TargetDB | Schema: $SchemaName"

#############################################################################################
## GATEKEEPER - CHECK PARAMETERS
#############################################################################################
Write-Log -Level INFO -Message "STEP : VALIDATING PARAMETERS (GATEKEEPER CHECKS)"

# Test Sql Instance
try {
    $Server = Connect-DbaInstance -SqlInstance $SqlInstance -TrustServerCertificate -ErrorAction Stop
    Write-Log -Level DEBUG -Message "SQL connection to instance '$SqlInstance' successful."
} catch {
    Write-Log -Level ERROR -Message "SQL connection failed to instance '$SqlInstance'. Error: $_"
    return 
}

# Test Sql Log Instance
try {
    $null = Connect-DbaInstance -SqlInstance $LogInstance -TrustServerCertificate -ErrorAction Stop
    Write-Log -Level DEBUG -Message "SQL connection to log instance '$LogInstance' successful."
} catch {
    Write-Log -Level ERROR -Message "SQL connection failed to log instance '$LogInstance'. Error: $_"
    return 
}

# Test Source Database
$db = Get-DbaDatabase -SqlInstance $Server -Database $SourceDB -ErrorAction Stop
if($db.count -lt 1){
    Write-Log -Level ERROR -Message "Source database '$SourceDB' does not exist or is inaccessible on instance '$SqlInstance'."
    return
}

# Test Target Database
$db = Get-DbaDatabase -SqlInstance $Server -Database $TargetDB -ErrorAction Stop
if($db.count -lt 1){
    Write-Log -Level ERROR -Message "Target database '$SourceDB' does not exist or is inaccessible on instance '$SqlInstance'."
    return
}

$schemaExists = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "SELECT 1 FROM sys.schemas WHERE name = '$schemaName'"
if ($schemaExists.Column1 -ne 1) {
    Write-Log -Level ERROR -Message "Schema '$schemaName' doesn't exist."
    return
}

    

#############################################################################################
## INITIALIZE LOG TABLE IN THE LOGDATABASE
#############################################################################################
Write-Log -Level INFO -Message "STEP : CREATE LOG TABLES"

$createLogTableQuery = @"
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = 'RestoreSchemaLog' AND TABLE_SCHEMA = 'dbo'
)
BEGIN
    CREATE TABLE dbo.RestoreSchemaLog (
        RestoreId BIGINT PRIMARY KEY,
        RestoreStartDatetime DATETIME,
        RestoreEndDatetime DATETIME,
        SourceDB NVARCHAR(255),
        TargetDB NVARCHAR(255),
        SchemaName NVARCHAR(255),
        ErrorCode BIT,
        Message NVARCHAR(MAX)
    );
END
"@

$createDetailTableQuery = @"
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = 'RestoreSchemaLogDetail' AND TABLE_SCHEMA = 'dbo'
)
BEGIN
    CREATE TABLE dbo.RestoreSchemaLogDetail (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        RestoreId BIGINT,
        Step NVARCHAR(MAX),
        ObjectType NVARCHAR(MAX),
        ObjectSchema NVARCHAR(MAX),
        ObjectName NVARCHAR(MAX),
        Action NVARCHAR(MAX),
        Command NVARCHAR(MAX),
        ErrorCode BIT,
        Message NVARCHAR(MAX),
        LogDate DATETIME
    );
END
"@
if(!$WhatIf){
    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $createLogTableQuery 
    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $createDetailTableQuery 
}


#############################################################################################
## CHECK CROSS-SCHEMA OBJECT REFERENCES
#############################################################################################

Write-Log -Level INFO -Message "STEP : CHECKING FOR CROSS-SCHEMA DEPENDENCIES IN SOURCE AND TARGET DATABASES"

# Check for SCHEMABINDING views and functions in source database
$dependentObjectsSource = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query @"
SELECT DISTINCT
    OBJECT_SCHEMA_NAME(d.referencing_id) AS ObjectSchema,
    OBJECT_NAME(d.referencing_id) AS ObjectName,
    o.type_desc AS ObjectType,
    OBJECT_SCHEMA_NAME(d.referenced_id) AS ReferencedSchema,
    OBJECT_NAME(d.referenced_id) AS ReferencedObject
FROM sys.sql_expression_dependencies d
JOIN sys.objects o ON d.referencing_id = o.object_id
WHERE 
    o.type IN ('V', 'FN', 'TF', 'IF') -- Views, Scalar, Table-valued, Inline functions
    AND OBJECTPROPERTY(d.referencing_id, 'IsSchemaBound') = 1
    AND ((OBJECT_SCHEMA_NAME(d.referenced_id) = '$SchemaName'
    AND OBJECT_SCHEMA_NAME(d.referencing_id) <> '$SchemaName')
    OR (OBJECT_SCHEMA_NAME(d.referenced_id) <> '$SchemaName'
    AND OBJECT_SCHEMA_NAME(d.referencing_id) = '$SchemaName'))
"@

if ($dependentObjectsSource.Count -gt 0) {
    Write-Log -Level ERROR -Message "SCHEMABINDING views or functions in the SOURCE database (outside schema '$SchemaName') reference objects inside '$SchemaName'. Cannot safely proceed with schema restore."
    $dependentObjectsSource | Format-Table ObjectSchema, ObjectName, ObjectType, ReferencedSchema, ReferencedObject
    return
} else {
    Write-Log -Level DEBUG -Message "No SCHEMABINDING views or functions in SOURCE reference schema '$SchemaName'."
}

# Check for SCHEMABINDING views and functions in target database
$dependentObjectsTarget = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query @"
SELECT DISTINCT
    OBJECT_SCHEMA_NAME(d.referencing_id) AS ObjectSchema,
    OBJECT_NAME(d.referencing_id) AS ObjectName,
    o.type_desc AS ObjectType,
    OBJECT_SCHEMA_NAME(d.referenced_id) AS ReferencedSchema,
    OBJECT_NAME(d.referenced_id) AS ReferencedObject
FROM sys.sql_expression_dependencies d
JOIN sys.objects o ON d.referencing_id = o.object_id
WHERE 
    o.type IN ('V', 'FN', 'TF', 'IF') -- Views, Scalar, Table-valued, Inline functions
    AND OBJECTPROPERTY(d.referencing_id, 'IsSchemaBound') = 1
    AND ((OBJECT_SCHEMA_NAME(d.referenced_id) = '$SchemaName'
    AND OBJECT_SCHEMA_NAME(d.referencing_id) <> '$SchemaName')
    OR (OBJECT_SCHEMA_NAME(d.referenced_id) <> '$SchemaName'
    AND OBJECT_SCHEMA_NAME(d.referencing_id) = '$SchemaName'))
"@

if ($dependentObjectsTarget.Count -gt 0) {
    Write-Log -Level ERROR -Message "SCHEMABINDING views or functions in the TARGET database (outside schema '$SchemaName') reference objects inside '$SchemaName'. Cannot safely proceed with schema restore."
    $dependentObjectsTarget | Format-Table ObjectSchema, ObjectName, ObjectType, ReferencedSchema, ReferencedObject
    return
} else {
    Write-Log -Level DEBUG -Message "No SCHEMABINDING views or functions in TARGET reference schema '$SchemaName'."
}


#############################################################################################
##
## DROP VIEW BEFORE DROP INSERT
##
#############################################################################################
Write-Log -Level INFO -Message "STEP : DROP VIEW BEFORE DROP INSERT"
$ErrorCodeDropView = 0
$startStep = Get-Date

# --- Step 1: Retrieve views from target schema
$viewsTarget = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query @"
SELECT 
    s.name AS SchemaName,
    v.name AS ViewName,
    OBJECT_SCHEMA_NAME(v.object_id) + '.' + v.name AS FullName,
    OBJECTPROPERTY(v.object_id, 'IsSchemaBound') AS IsSchemaBound,
    v.object_id AS ObjectId
FROM sys.views v
JOIN sys.schemas s ON s.schema_id = v.schema_id
WHERE s.name = '$SchemaName'
"@

Write-Log -Level INFO -Message ("Found {0} views to drop." -f $viewsTarget.Count)

# --- Step 2: Build dictionary (FullName => View object)
$viewDictTarget = @{}
foreach ($view in $viewsTarget) {
    $viewDictTarget[$view.FullName] = $view
}


$dependenciesTarget = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query @"
SELECT 
    OBJECT_SCHEMA_NAME(referencing_id) + '.' + OBJECT_NAME(referencing_id) AS Referencer,
    OBJECT_SCHEMA_NAME(referenced_id) + '.' + OBJECT_NAME(referenced_id) AS Referenced
FROM sys.sql_expression_dependencies
WHERE 
    OBJECTPROPERTY(referencing_id, 'IsView') = 1
    AND OBJECTPROPERTY(referenced_id, 'IsView') = 1
"@

# --- Step 4: Filter internal dependencies (only between views in the schema)
$viewNamesTarget = $viewDictTarget.Keys
$internalDepsTarget = $dependenciesTarget | Where-Object {
    $_.Referencer -in $viewNamesTarget -and $_.Referenced -in $viewNamesTarget
}

# --- Step 5: Build graph and in-degree map for topological sort
$graph = @{}
$inDegree = @{}

foreach ($viewName in $viewNamesTarget) {
    $graph[$viewName] = @()
    $inDegree[$viewName] = 0
}

foreach ($dep in $internalDepsTarget) {
    $graph[$dep.Referenced] += $dep.Referencer
    $inDegree[$dep.Referencer]++
}


$queue = New-Object System.Collections.Generic.Queue[string]
$inDegree.Keys | Where-Object { $inDegree[$_] -eq 0 } | ForEach-Object { $queue.Enqueue($_) }

$sortedTarget = @()

while ($queue.Count -gt 0) {
    $current = $queue.Dequeue()
    $sortedTarget += $current

    foreach ($dependent in $graph[$current]) {
        $inDegree[$dependent]--
        if ($inDegree[$dependent] -eq 0) {
            $queue.Enqueue($dependent)
        }
    }
}

if ($sortedTarget.Count -ne $viewNamesTarget.Count) {
    Write-Log -Level ERROR -Message "Cycle detected in view dependencies. Cannot proceed with drop order."
    return
}


$sortedTarget = [System.Collections.ArrayList]::new($sortedTarget)
$sortedTarget.Reverse()

foreach ($viewName in $sortedTarget) {
    $view = $viewDictTarget[$viewName]

    $dropViewQuery = "DROP VIEW $viewName"
    Write-Log -Level DEBUG -Message "Dropping view: $viewName"

    if(!$WhatIf){
        try {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $dropViewQuery

                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'DROP VIEW BEFORE DROP INSERT', 'VIEW', '$($view.SchemaName)', '$($view.ViewName)', 'DROP', '$dropViewQuery', 0, 'Success', GETDATE())
                "
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
        catch {
            $ErrorCodeDropView = 1
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'DROP VIEW BEFORE DROP INSERT', 'VIEW', '$($view.SchemaName)', '$($view.ViewName)', 'DROP', '$dropViewQuery', 1, '$errorMsg', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            Write-Log -Level ERROR -Message "Failed to drop view $viewName : $errorMsg"
            $ErrorCode = 1
            if(!$ContinueOnError){
                $end = Get-Date
                $RestoreEndDatetime = $end.ToString("yyyy-MM-dd HH:mm:ss")
                $logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId,RestoreStartDatetime,RestoreEndDatetime,SourceDB,TargetDB,SchemaName,ErrorCode,Message)
                VALUES ($RestoreId,'$RestoreStartDatetime','$RestoreEndDatetime','$SourceDB','$TargetDB','$SchemaName',$ErrorCode,'Error on DROP VIEW BEFORE DROP INSERT')"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                return
            }
        }
    }
}

$endStep = Get-Date
$durationInSeconds = ($endStep - $startStep).TotalSeconds
$roundedDuration = [math]::Round($durationInSeconds, 2)

if($ErrorCodeDropView -eq 1){
    Write-Log -Level ERROR -Message "STEP : DROP VIEW BEFORE DROP INSERT in $roundedDuration seconds | FAILED"
}
else{
    Write-Log -Level INFO -Message "STEP : DROP VIEW BEFORE DROP INSERT in $roundedDuration seconds | SUCCEED"
}


#############################################################################################
## DROP FUNCTION 
#############################################################################################
Write-Log -Level INFO -Message "STEP : DROP FUNCTION"
$ErrorCodeDropFunction = 0
$startStep = Get-Date

# --- Step 1: Retrieve UDFs from target schema
$udfsTarget = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query @"
SELECT 
    s.name AS SchemaName,
    o.name AS FunctionName,
    OBJECT_SCHEMA_NAME(o.object_id) + '.' + o.name AS FullName,
    o.object_id AS ObjectId,
    o.type AS ObjectType
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE s.name = '$SchemaName'
  AND o.type IN ('FN', 'TF', 'IF')  -- Scalar, Table-valued, Inline
"@

Write-Log -Level INFO -Message ("Found {0} functions to drop." -f $udfsTarget.Count)

# --- Step 2: Build dictionary (FullName => Function object)
$udfDictTarget = @{}
foreach ($udf in $udfsTarget) {
    $udfDictTarget[$udf.FullName] = $udf
}

# --- Step 3: Load function dependencies
$functionDepsTarget = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query @"
SELECT 
    OBJECT_SCHEMA_NAME(referencing_id) + '.' + OBJECT_NAME(referencing_id) AS Referencer,
    OBJECT_SCHEMA_NAME(referenced_id) + '.' + OBJECT_NAME(referenced_id) AS Referenced
FROM sys.sql_expression_dependencies
WHERE 
    OBJECTPROPERTY(referencing_id, 'IsTableFunction') = 1
    OR OBJECTPROPERTY(referencing_id, 'IsScalarFunction') = 1
    OR OBJECTPROPERTY(referencing_id, 'IsInlineFunction') = 1
"@

# --- Step 4: Filter internal dependencies (only within target schema)
$udfNamesTarget = $udfDictTarget.Keys
$internalUdfDeps = $functionDepsTarget | Where-Object {
    $_.Referencer -in $udfNamesTarget -and $_.Referenced -in $udfNamesTarget
}

# --- Step 5: Build graph and in-degree map for topological sort
$graph = @{}
$inDegree = @{}

foreach ($udfName in $udfNamesTarget) {
    $graph[$udfName] = @()
    $inDegree[$udfName] = 0
}

foreach ($dep in $internalUdfDeps) {
    $graph[$dep.Referenced] += $dep.Referencer
    $inDegree[$dep.Referencer]++
}

# --- Step 6: Topological sort (reverse drop order)
$queue = New-Object System.Collections.Generic.Queue[string]
$inDegree.Keys | Where-Object { $inDegree[$_] -eq 0 } | ForEach-Object { $queue.Enqueue($_) }

$sortedUdf = @()
while ($queue.Count -gt 0) {
    $current = $queue.Dequeue()
    $sortedUdf += $current

    foreach ($dependent in $graph[$current]) {
        $inDegree[$dependent]--
        if ($inDegree[$dependent] -eq 0) {
            $queue.Enqueue($dependent)
        }
    }
}

if ($sortedUdf.Count -ne $udfNamesTarget.Count) {
    Write-Log -Level ERROR -Message "Cycle detected in function dependencies. Cannot proceed with drop order."
    return
}

$sortedUdf = [System.Collections.ArrayList]::new($sortedUdf)
$sortedUdf.Reverse()

# --- Step 7: Drop functions
foreach ($udfName in $sortedUdf) {
    $udf = $udfDictTarget[$udfName]
    $dropQuery = "DROP FUNCTION $udfName"

    Write-Log -Level DEBUG -Message "Dropping function: $udfName"

    if (!$WhatIf) {
        try {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $dropQuery

            $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'DROP FUNCTION', 'FUNCTION', '$($udf.SchemaName)', '$($udf.FunctionName)', 'DROP', '$dropQuery', 0, 'Success', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
        catch {
            $ErrorCodeDropFunction = 1
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'DROP FUNCTION', 'FUNCTION', '$($udf.SchemaName)', '$($udf.FunctionName)', 'DROP', '$dropQuery', 1, '$errorMsg', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            Write-Log -Level ERROR -Message "Failed to drop function $udfName : $errorMsg"
            $ErrorCode = 1

            if (!$ContinueOnError) {
                $end = Get-Date
                $RestoreEndDatetime = $end.ToString("yyyy-MM-dd HH:mm:ss")
                $logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId, RestoreStartDatetime, RestoreEndDatetime, SourceDB, TargetDB, SchemaName, ErrorCode, Message)
                             VALUES ($RestoreId, '$RestoreStartDatetime', '$RestoreEndDatetime', '$SourceDB', '$TargetDB', '$SchemaName', $ErrorCode, 'Error on DROP FUNCTION')"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                return
            }
        }
    }
}

# --- Step 8: Final logging
$endStep = Get-Date
$durationInSeconds = ($endStep - $startStep).TotalSeconds
$roundedDuration = [math]::Round($durationInSeconds, 2)

if ($ErrorCodeDropFunction -eq 1) {
    Write-Log -Level ERROR -Message "STEP : DROP FUNCTION in $roundedDuration seconds | FAILED"
} else {
    Write-Log -Level INFO -Message "STEP : DROP FUNCTION in $roundedDuration seconds | SUCCEED"
}


#############################################################################################
## DISABLE FOREIGN KEYS TO ALLOW TRUNCATE/INSERT ON DEPENDENT TABLES
#############################################################################################
Write-Log -Level INFO -Message "STEP : DISABLING FOREIGN KEYS"
$ErrorCodeDisableFk = 0
$startStep = Get-Date


# Step 1: Retrieve foreign keys referencing tables in the schema
$foreignKey = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query @"
SELECT
    fk.name AS ForeignKeyName,
    sch_parent.name AS ReferencingSchema,
    t_parent.name AS ReferencingTable
FROM sys.foreign_keys fk
    INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    INNER JOIN sys.tables t_parent ON fkc.parent_object_id = t_parent.object_id
    INNER JOIN sys.schemas sch_parent ON t_parent.schema_id = sch_parent.schema_id
    INNER JOIN sys.tables t_ref ON fkc.referenced_object_id = t_ref.object_id
    INNER JOIN sys.schemas sch_ref ON t_ref.schema_id = sch_ref.schema_id
WHERE sch_ref.name = '$SchemaName'
"@

Write-Log -Level INFO -Message ("Found {0} foreign key(s) to disable." -f $foreignKey.Count)

# Step 2: Initialize concurrent result tracking
$fkDropResults = [System.Collections.Concurrent.ConcurrentDictionary[string, int]]::new()

# Step 3: Parallel processing (write logs later)
$foreignKey | ForEach-Object -Parallel {
    $schema = $_.ReferencingSchema
    $table  = $_.ReferencingTable
    $fkName = $_.ForeignKeyName
    $query  = "ALTER TABLE [$schema].[$table] DROP CONSTRAINT [$fkName];"
    $queryLog = $query.Replace("'", "''")
    $ErrorCode = 0

    if (-not $using:WhatIf) {
        try {
            Invoke-DbaQuery -SqlInstance $using:SqlInstance -Database $using:TargetDB -Query $query -EnableException

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail
            (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($using:RestoreId, 'DISABLED FOREIGN KEY TO TRUNCATE INSERT DATA', 'FOREIGN KEY', '$schema', '$fkName', 'DROP', '$queryLog', 0, 'Success', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $using:LogInstance -Database $using:LogDatabase -Query $logQuery
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $ErrorCode = 1

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail
            (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($using:RestoreId, 'DISABLED FOREIGN KEY TO TRUNCATE INSERT DATA', 'FOREIGN KEY', '$schema', '$fkName', 'DROP', '$queryLog', 1, '$errorMsg', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $using:LogInstance -Database $using:LogDatabase -Query $logQuery
        }
    }

    $dict = $using:fkDropResults
    $null = $dict.TryAdd("$schema.$fkName", $ErrorCode)

} -ThrottleLimit $Parallel


$fkDropResults.GetEnumerator() | ForEach-Object {
    $fk = $_.Key
    $code = $_.Value

    Write-Log -Level DEBUG -Message "Dropping foreign key: $fk"

    if ($code -eq 1) {
        Write-Log -Level ERROR -Message "Failed to drop foreign key: $fk"
        $ErrorCode = 1
        $ErrorCodeDisableFk = 1
    }
}

if($ErrorCode -eq 1 -and !$ContinueOnError){
    $end = Get-Date
    $RestoreEndDatetime = $end.ToString("yyyy-MM-dd HH:mm:ss")
    $logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId,RestoreStartDatetime,RestoreEndDatetime,SourceDB,TargetDB,SchemaName,ErrorCode,Message)
    VALUES ($RestoreId,'$RestoreStartDatetime','$RestoreEndDatetime','$SourceDB','$TargetDB','$SchemaName',$ErrorCode,'Error during the DISABLED FOREIGN KEY TO TRUNCATE INSERT DATA')"
    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
    return
}

$endStep = Get-Date
$durationInSeconds = ($endStep - $startStep).TotalSeconds
$roundedDuration = [math]::Round($durationInSeconds, 2)
    
if($ErrorCodeDisableFk -eq 1){
    Write-Log -Level ERROR -Message "STEP : DISABLING FOREIGN KEYS in $roundedDuration seconds | FAILED"
}
else{
    Write-Log -Level INFO -Message "STEP : DISABLING FOREIGN KEYS in $roundedDuration seconds | SUCCEED"
}

#############################################################################################
## DROP INSERT TABLE
#############################################################################################
Write-Log -Level INFO -Message "STEP : DROP INSERT TABLES"
$ErrorCodeDropInsertTable = 0
$startStep = Get-Date


$sourceTables = Get-DbaDbTable -SqlInstance $SqlInstance -Database $SourceDB -Schema $SchemaName
Write-Log -Level INFO -Message ("Found {0} table(s) to drop insert" -f $sourceTables.Count)

$TableResults = [System.Collections.Concurrent.ConcurrentDictionary[string, object]]::new()

# truncate insert data for each table
$sourceTables | ForEach-Object -Parallel {
    $stepResults = @{}
    $ErrorCode = 0
    $tableName = $_.Name
    $qualifiedName = "[$using:SchemaName].[$tableName]"
    $SqlInstance = $using:SqlInstance
    $SourceDB = $using:SourceDB
    $SchemaName = $using:SchemaName
    $LogInstance = $using:LogInstance
    $LogDatabase = $using:LogDatabase
    $TargetDB = $using:TargetDB
    $RestoreId = $using:RestoreId
    $WhatIf = $using:WhatIf

#############################################################################################
## DROP TABLE 
#############################################################################################

    $dropResult = @{}
    $objectTable = Get-DbaDbTable -SqlInstance $SqlInstance -Database $SourceDB -Schema $SchemaName -Table $tableName
    $dropQuery = "IF OBJECT_ID('$qualifiedName', 'U') IS NOT NULL DROP TABLE $qualifiedName"

    $dropQueryLog = $dropQuery.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

    if (!$WhatIf) {
        try {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $dropQuery -EnableException

            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'DROP TABLE', 'TABLE', '$SchemaName', '$tableName', 'DROP', '$dropQueryLog', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail(RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'DROP TABLE', 'TABLE', '$SchemaName', '$tableName', 'DROP', '$dropQueryLog', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }

    $dropResult[$tableName] = $ErrorCode
    $stepResults["drop"] = $dropResult

#############################################################################################
## CREATE TABLE 
#############################################################################################

    # 2. CREATE TABLE sans contraintes
    $options = New-DbaScriptingOption
    $options.SchemaQualify = $true
    $options.IncludeHeaders = $false
    $options.ToFileOnly = $false
    $options.WithDependencies = $false
    $options.ScriptBatchTerminator = $true
    $options.DriDefaults = $true

    $ScriptTable = $objectTable | Export-DbaScript -ScriptingOptionObject $options -NoPrefix -Passthru 
    $scriptCleaned = $ScriptTable -join "`r`n"
    $tableCommandLog = $scriptCleaned.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

    $createTableResult = @{}
    
    
    if (!$WhatIf) {
        try {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $scriptCleaned -EnableException

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'CREATE TABLE', 'TABLE', '$SchemaName', '$tableName', 'CREATE', '$tableCommandLog', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'CREATE TABLE', 'TABLE', '$SchemaName', '$tableName', 'CREATE', '$tableCommandLog', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }

    $createTableResult[$tableName] = $ErrorCode
    $stepResults["create"] = $createTableResult

#############################################################################################
## CREATE CLUSTERED COLUMNSTORE INDEX 
#############################################################################################
   
    $columnstoreIndexes = $objectTable.Indexes | Where-Object { $_.IndexType -like "ClusteredColumnStoreIndex" }
    # 4. Création index Cluster Columnstore s’il y en a
    $CreateCCIResult = @{}


    foreach ($indexCCI in $columnstoreIndexes) {
        $ScriptTableIndexCCI = $indexCCI | Export-DbaScript -Passthru -NoPrefix 
        $scriptCleaned = $ScriptTableIndexCCI -join "`r`n"
        $tableIndexCCICommandLog = $scriptCleaned.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

        if (!$WhatIf) {
            try {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $scriptCleaned -EnableException
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE INDEX', 'INDEX', '$SchemaName', '$($indexCCI.Name)', 'CREATE', '$tableIndexCCICommandLog', 0, 'Success', GETDATE())"   
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            } catch {
                # Gestion des erreurs
                $errorMsg = $_.Exception.Message.Replace("'", "''")
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE INDEX', 'INDEX', '$SchemaName', '$($indexCCI.Name)', 'CREATE', '$tableIndexCCICommandLog', 1, '$errorMsg', GETDATE())"
                
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                $ErrorCode = 1
            }
        }

        $CreateCCIResult[$indexCCI.Name] = $ErrorCode
        $stepResults["indexCCI"] = $CreateCCIResult

    }

#############################################################################################
## INSERT DATA
#############################################################################################

    $insertResult = @{}

    $columns = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
        SELECT c.name, t.name AS data_type
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID('$SchemaName.$tableName') AND c.is_computed = 0
        ORDER BY c.column_id
    "

    $insertColumns = @()
    $selectColumns = @()
    foreach ($col in $columns) {
        $colName = "[$($col.name)]"
        $insertColumns += $colName
        switch ($col.data_type.ToLower()) {
            "xml"       { $selectColumns += "CONVERT(XML, $colName) AS $colName" }
            "geography" { $selectColumns += "CAST($colName AS GEOGRAPHY) AS $colName" }
            "geometry"  { $selectColumns += "CAST($colName AS GEOMETRY) AS $colName" }
            default     { $selectColumns += $colName }
        }
    }

    $insertList = $insertColumns -join ", "
    $selectList = $selectColumns -join ", "
    $insertQuery = "INSERT INTO [$TargetDB].[$SchemaName].[$tableName] WITH (TABLOCK) ($insertList) SELECT $selectList FROM [$SourceDB].[$SchemaName].[$tableName]"

    $hasIdentity = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
        SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('$SchemaName.$tableName') AND is_identity = 1
    " | Select-Object -ExpandProperty Column1

    if ($hasIdentity -gt 0) {
        $fullInsert = "SET IDENTITY_INSERT [$SchemaName].[$tableName] ON; $insertQuery; SET IDENTITY_INSERT [$SchemaName].[$tableName] OFF;"
    } else {
        $fullInsert = $insertQuery
    }
    $insertLog = $fullInsert.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

    if (!$WhatIf) {
        try {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $fullInsert -EnableException
            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'INSERT DATA', 'TABLE', '$SchemaName', '$tableName', 'INSERT', '$insertLog', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'INSERT DATA', 'TABLE', '$SchemaName', '$tableName', 'INSERT', '$insertLog', 1, '$errorMsg', GETDATE())"
            
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }

    $insertResult[$tableName] = $ErrorCode
    $stepResults["insert"] = $insertResult

#############################################################################################
## CREATE INDEXES 
#############################################################################################
    $indexResults = @{}
    $remainingIndexes = $objectTable.Indexes | Where-Object { $_.IndexType -ne 'ClusteredColumnstoreIndex' }

    foreach ($index in $remainingIndexes) {
        $ScriptTableIndex = $index | Export-DbaScript -Passthru -NoPrefix 
        $scriptCleaned = $ScriptTableIndex -join "`r`n"
        $tableIndexCommandLog = $scriptCleaned.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

        if (!$WhatIf) {
            try {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $scriptCleaned -EnableException
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE INDEX', 'INDEX', '$SchemaName', '$($index.Name)', 'CREATE', '$tableIndexCommandLog', 0, 'Success', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            }
            catch {
                $errorMsg = $_.Exception.Message.Replace("'", "''")
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE INDEX', 'INDEX', '$SchemaName', '$($index.Name)', 'CREATE', '$tableIndexCommandLog', 1, '$errorMsg', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery

                $ErrorCode = 1
            }
        }

        $indexResults[$index.Name] = $ErrorCode
        $stepResults["index"] = $indexResults
    }

#############################################################################################
## CREATE CHECK CONSTRAINTS 
#############################################################################################

    $checkConstraintResults = @{}

    foreach ($ckConstraint in $objectTable.Checks) {
        $ScriptTableCkConstraint = $ckConstraint | Export-DbaScript -Passthru -NoPrefix 
        $scriptCleaned = $ScriptTableCkConstraint -join "`r`n"
        $tableCkConstraintCommandLog = $scriptCleaned.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

        if (!$WhatIf) {
            try {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $scriptCleaned -EnableException
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE CHECK CONSTRAINT', 'CONSTRAINT', '$SchemaName', '$($ckConstraint.Name)', 'CREATE', '$tableCkConstraintCommandLog', 0, 'Success', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            } catch {
                $errorMsg = $_.Exception.Message.Replace("'", "''")
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE CHECK CONSTRAINT', 'CONSTRAINT', '$SchemaName', '$($ckConstraint.Name)', 'CREATE', '$tableCkConstraintCommandLog', 1, '$errorMsg', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                $ErrorCode = 1
            }
        }

        $checkConstraintResults[$ckConstraint.Name] = $ErrorCode
        $stepResults["check_constraint"] = $checkConstraintResults
    }

#############################################################################################
## CREATE TRIGGERS
#############################################################################################
    $triggerResults = @{}
    foreach ($trigger in $objectTable.Triggers) {
        if ($trigger.IsSystemObject -eq $false) {
            $ScriptTableTrigger = $trigger | Export-DbaScript -Passthru -NoPrefix 
            $scriptCleaned = $ScriptTableTrigger -join "`r`n"
            $tableTriggerCommandLog = $scriptCleaned.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

            if (!$WhatIf) {
                try {
                    Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $scriptCleaned -EnableException
                    $logQuery = "
                    INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                    VALUES ($RestoreId, 'CREATE TRIGGER', 'TRIGGER', '$SchemaName', '$($trigger.Name)', 'CREATE', '$tableTriggerCommandLog', 0, 'Success', GETDATE())"
                    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                } catch {
                    $errorMsg = $_.Exception.Message.Replace("'", "''")
                    $logQuery = "
                    INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                    VALUES ($RestoreId, 'CREATE TRIGGER', 'TRIGGER', '$SchemaName', '$($trigger.Name)', 'CREATE', '$tableTriggerCommandLog', 1, '$errorMsg', GETDATE())"
                    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                    $ErrorCode = 1
                }
            }

            $triggerResults[$trigger.Name] = $ErrorCode
            $stepResults["trigger"] = $triggerResults
        }
    }

    $dict = $using:TableResults
    $null = $dict.TryAdd($tableName, $stepResults)


} -ThrottleLimit $Parallel



$orderedSteps = @(
    "drop",
    "create",
    "indexcci",
    "insert",
    "index",
    "check_constraint",
    "trigger"
)

foreach ($tableEntry in $TableResults.GetEnumerator()) {
    $tableName = $tableEntry.Key
    $stepsDict = $tableEntry.Value

    Write-Log -Level DEBUG -Message ("TABLE: {0}" -f $tableName)

    foreach ($stepName in $orderedSteps) {
        if ($stepsDict.ContainsKey($stepName)) {
            $objectsDict = $stepsDict[$stepName]

            if ($null -ne $objectsDict -and $objectsDict.Count -gt 0) {

                # Log pour les étapes à objets multiples
                if ($stepName -in @('index', 'check_constraint', 'trigger')) {
                    Write-Log -Level DEBUG -Message ("Found {0} {1} operation(s)" -f $objectsDict.Count, $stepName.ToUpper())
                }

                foreach ($objectEntry in $objectsDict.GetEnumerator()) {
                    $objectName = $objectEntry.Key
                    $code = $objectEntry.Value

                    if($code -eq 1){
                        $ErrorCode = 1
                        $ErrorCodeDropInsertTable = 1
                    }

                    switch ($stepName) {
                        "drop"               { Write-Log -Level DEBUG -Message ("Drop Table : {0}" -f $objectName) }
                        "create"             { Write-Log -Level DEBUG -Message ("Create Table : {0}" -f $objectName) }
                        "indexcci"           { Write-Log -Level DEBUG -Message ("Create ClusteredColumnStoreIndex : {0}" -f $objectName) }
                        "insert"             { Write-Log -Level DEBUG -Message ("Insert Into Table : {0}" -f $objectName) }
                        "index"              { Write-Log -Level DEBUG -Message ("Create Index : {0}" -f $objectName) }
                        "check_constraint"   { Write-Log -Level DEBUG -Message ("Create Check Constraint : {0}" -f $objectName) }
                        "trigger"            { Write-Log -Level DEBUG -Message ("Create Trigger : {0}" -f $objectName) }
                    }
                }
            }
        }
    }

    Write-Log -Level DEBUG -Message "-----------------------------"
    if($ErrorCode -eq 1 ){
        Write-Log -Level ERROR -Message "Error during the DROP INSERT TABLE"
        if(!$ContinueOnError){
            $end = Get-Date
            $RestoreEndDatetime = $end.ToString("yyyy-MM-dd HH:mm:ss")
            $logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId,RestoreStartDatetime,RestoreEndDatetime,SourceDB,TargetDB,SchemaName,ErrorCode,Message)
            VALUES ($RestoreId,'$RestoreStartDatetime','$RestoreEndDatetime','$SourceDB','$TargetDB','$SchemaName',$ErrorCode,'Error during the DROP INSERT TABLE')"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            return
        }
    }
}

$endStep = Get-Date
$durationInSeconds = ($endStep - $startStep).TotalSeconds
$roundedDuration = [math]::Round($durationInSeconds, 2)

if($ErrorCodeDropInsertTable -eq 1){
    Write-Log -Level ERROR -Message "STEP : DROP INSERT TABLES in $roundedDuration seconds | FAILED"
}
else{
    Write-Log -Level INFO -Message "STEP : DROP INSERT TABLES in $roundedDuration seconds | SUCCEED"
}

#############################################################################################
## ENABLED FOREIGN KEY 
#############################################################################################
Write-Log -Level INFO -Message "STEP : ENABLING FOREIGN KEY CONSTRAINTS"
$ErrorCodeEnableFk = 0
$startStep = Get-Date


# Étape 1 : Récupération des FK à rétablir
$foreignKeyNames = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
    SELECT fk.name AS ForeignKeyName
    FROM sys.foreign_keys fk
    INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    INNER JOIN sys.tables t_parent ON fkc.parent_object_id = t_parent.object_id
    INNER JOIN sys.schemas sch_parent ON t_parent.schema_id = sch_parent.schema_id
    INNER JOIN sys.tables t_ref ON fkc.referenced_object_id = t_ref.object_id
    INNER JOIN sys.schemas sch_ref ON t_ref.schema_id = sch_ref.schema_id
    WHERE sch_parent.name = '$SchemaName' OR sch_ref.name = '$SchemaName'
"

# Étape 2 : Obtenir les objets FK
$foreignKeyObjects = Get-DbaDbForeignKey -SqlInstance $SqlInstance -Database $SourceDB |
    Where-Object { $_.Name -in $foreignKeyNames.ForeignKeyName }

# Étape 3 : Génération des scripts FK
$options = New-DbaScriptingOption
$options.SchemaQualify = $true
$options.IncludeHeaders = $false
$options.ToFileOnly = $false
$options.WithDependencies = $false
$options.ScriptBatchTerminator = $true

$fkScripts = @()

foreach ($fk in $foreignKeyObjects) {
    try {
        $scriptFkLines = $fk | Export-DbaScript -ScriptingOptionObject $options -NoPrefix -Passthru
        $scriptFk = $scriptFkLines -join "`r`n"

        $fkScripts += [pscustomobject]@{
            Schema = $fk.Parent.Schema
            Table  = $fk.Parent.Name
            Name   = $fk.Name
            Script = $scriptFk
        }
    } catch {
        Write-Log -Level WARNING -Message "Erreur lors de la génération du script FK '$($fk.Name)' : $($_.Exception.Message)"
    }
}

Write-Log -Level INFO -Message ("Found {0} foreign key(s) to enable" -f $foreignKeyObjects.Count)


# Étape 4 : Execution parallèle avec dictionnaire thread-safe
$fkEnableResults = [System.Collections.Concurrent.ConcurrentDictionary[string, int]]::new()

$fkScripts | ForEach-Object -Parallel {
    $ErrorCode = 0
    $fkName = $_.Name
    $schema = $_.Schema
    $table = $_.Table
    $script = $_.Script
    $cleanedScript = $script -join "`r`n"
    $fkCommandLog = $script.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "
    

    if (!$using:WhatIf) {
        try {
            Invoke-DbaQuery -SqlInstance $using:SqlInstance -Database $using:TargetDB -Query $cleanedScript -EnableException

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail
            (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($using:RestoreId, 'ENABLED FOREIGN KEY CONSTRAINT', 'FOREIGN KEY', '$schema', '$fkName', 'ALTER', '$fkCommandLog', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $using:LogInstance -Database $using:LogDatabase -Query $logQuery
        } catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $ErrorCode = 1

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail
            (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($using:RestoreId, 'ENABLED FOREIGN KEY CONSTRAINT', 'FOREIGN KEY', '$schema', '$fkName', 'ALTER', '$fkCommandLog', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $using:LogInstance -Database $using:LogDatabase -Query $logQuery
        }
    }

    $dict = $using:fkEnableResults
    $null = $dict.TryAdd($fkName, $ErrorCode)

} -ThrottleLimit $Parallel

# Étape 5 : Log des résultats
foreach ($entry in $fkEnableResults.GetEnumerator()) {
    $fkName = $entry.Key
    $code = $entry.Value

    Write-Log -Level DEBUG -Message "Enabling FK [$fkName]"

    if ($code -eq 1) {
        Write-Log -Level ERROR -Message "Failed to enable foreign key [$fkName]"
        $ErrorCode = 1
        $ErrorCodeEnableFk = 1
    }
}

if($ErrorCode -eq 1 -and !$ContinueOnError){
    $end = Get-Date
    $RestoreEndDatetime = $end.ToString("yyyy-MM-dd HH:mm:ss")
    $logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId,RestoreStartDatetime,RestoreEndDatetime,SourceDB,TargetDB,SchemaName,ErrorCode,Message)
    VALUES ($RestoreId,'$RestoreStartDatetime','$RestoreEndDatetime','$SourceDB','$TargetDB','$SchemaName',$ErrorCode,'Error during the ENABLED FOREIGN KEY CONSTRAINT')"
    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
    return
}

$endStep = Get-Date
$durationInSeconds = ($endStep - $startStep).TotalSeconds
$roundedDuration = [math]::Round($durationInSeconds, 2)

if($ErrorCodeEnableFk -eq 1){
    Write-Log -Level ERROR -Message "STEP : ENABLING FOREIGN KEY CONSTRAINTS in $roundedDuration seconds | FAILED"
}
else{
    Write-Log -Level INFO -Message "STEP : ENABLING FOREIGN KEY CONSTRAINTS in $roundedDuration seconds | SUCCEED"
}


#############################################################################################
## CREATE FUNCTION
#############################################################################################
#############################################################################################
## CREATE FUNCTION AFTER DROP INSERT
#############################################################################################
Write-Log -Level INFO -Message "STEP : CREATE FUNCTION"
$ErrorCodeCreateFunction = 0
$startStep = Get-Date

# Step 1: Retrieve functions from source schema
$functionsSource = Get-DbaDbUdf -SqlInstance $SqlInstance -Database $SourceDB | Where-Object { $_.Schema -eq $SchemaName }

Write-Log -Level INFO -Message ("Found {0} functions to create from source schema '{1}'." -f $functionsSource.Count, $SchemaName)

if ($functionsSource.Count -ne 0) {

    # Step 2: Build dictionary FullName => Function
    $functionDictSource = @{}
    foreach ($fn in $functionsSource) {
        $fnFullName = "$($fn.Schema).$($fn.Name)"
        $functionDictSource[$fnFullName] = $fn
    }

    # Step 3: Get dependencies between functions
    $dependenciesSource = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
    SELECT 
        OBJECT_SCHEMA_NAME(referencing_id) + '.' + OBJECT_NAME(referencing_id) AS Referencer,
        OBJECT_SCHEMA_NAME(referenced_id) + '.' + OBJECT_NAME(referenced_id) AS Referenced
    FROM sys.sql_expression_dependencies
    WHERE 
        (OBJECTPROPERTY(referencing_id, 'IsTableFunction') = 1 OR
        OBJECTPROPERTY(referencing_id, 'IsScalarFunction') = 1 OR
        OBJECTPROPERTY(referencing_id, 'IsInlineFunction') = 1)
    "

    # Step 4: Filter internal dependencies
    $functionNames = $functionDictSource.Keys
    $internalDeps = $dependenciesSource | Where-Object {
        $_.Referencer -in $functionNames -and $_.Referenced -in $functionNames
    }

    # Step 5: Topological sort
    $graph = @{}
    $inDegree = @{}

    foreach ($fnName in $functionNames) {
        $graph[$fnName] = @()
        $inDegree[$fnName] = 0
    }

    foreach ($dep in $internalDeps) {
        $graph[$dep.Referenced] += $dep.Referencer
        $inDegree[$dep.Referencer]++
    }

    $queue = [System.Collections.Generic.Queue[string]]::new()
    $inDegree.Keys | Where-Object { $inDegree[$_] -eq 0 } | ForEach-Object { $queue.Enqueue($_) }

    $sortedFunctions = @()
    while ($queue.Count -gt 0) {
        $current = $queue.Dequeue()
        $sortedFunctions += $current
        foreach ($dependent in $graph[$current]) {
            $inDegree[$dependent]--
            if ($inDegree[$dependent] -eq 0) {
                $queue.Enqueue($dependent)
            }
        }
    }

    if ($sortedFunctions.Count -ne $functionNames.Count) {
        Write-Log -Level ERROR -Message "Cycle detected in function dependencies. Cannot safely recreate functions."
        return
    }

    $sortedFunctions = [System.Collections.ArrayList]::new($sortedFunctions)

    # Step 6: Recreate each function
    foreach ($fnName in $sortedFunctions) {
        $fn = $functionDictSource[$fnName]

        $cleanedScript = "$($fn.TextHeader)`r`n$($fn.TextBody)"
        $fnCommandLog = $cleanedScript.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

        Write-Log -Level DEBUG -Message "Creating function [$($fn.Schema).$($fn.Name)]"

        if (!$WhatIf) {
            try {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $cleanedScript -EnableException
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail
                (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE FUNCTION', 'FUNCTION', '$($fn.Schema)', '$($fn.Name)', 'CREATE', '$fnCommandLog', 0, 'Success', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            }
            catch {
                $errorMsg = $_.Exception.Message.Replace("'", "''")
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail
                (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE FUNCTION', 'FUNCTION', '$($fn.Schema)', '$($fn.Name)', 'CREATE', '$fnCommandLog', 1, '$errorMsg', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery

                Write-Log -Level ERROR -Message "Failed to create function [$($fn.Schema).$($fn.Name)] : $errorMsg"
                $ErrorCode = 1
                $ErrorCodeCreateFunction = 1
                if (!$ContinueOnError) {
                    $end = Get-Date
                    $RestoreEndDatetime = $end.ToString("yyyy-MM-dd HH:mm:ss")
                    $logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId,RestoreStartDatetime,RestoreEndDatetime,SourceDB,TargetDB,SchemaName,ErrorCode,Message)
                    VALUES ($RestoreId,'$RestoreStartDatetime','$RestoreEndDatetime','$SourceDB','$TargetDB','$SchemaName',$ErrorCode,'Error during the CREATE FUNCTION')"
                    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                    return
                }
            }
        }
    }
}

$endStep = Get-Date
$durationInSeconds = ($endStep - $startStep).TotalSeconds
$roundedDuration = [math]::Round($durationInSeconds, 2)

if ($ErrorCodeCreateFunction -eq 1) {
    Write-Log -Level ERROR -Message "STEP : CREATE FUNCTION in $roundedDuration seconds | FAILED"
} else {
    Write-Log -Level INFO -Message "STEP : CREATE FUNCTION in $roundedDuration seconds | SUCCEED"
}


#############################################################################################
## CREATE VIEW AFTER DROP INSERT
#############################################################################################
Write-Log -Level INFO -Message "STEP : CREATE VIEW AFTER DROP INSERT"
$ErrorCodeCreateView = 0
$startStep = Get-Date


# Step 1: Retrieve views from source schema
$viewsSource = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query @"
SELECT 
    s.name AS SchemaName,
    v.name AS ViewName,
    OBJECT_SCHEMA_NAME(v.object_id) + '.' + v.name AS FullName,
    OBJECTPROPERTY(v.object_id, 'IsSchemaBound') AS IsSchemaBound,
    v.object_id AS ObjectId
FROM sys.views v
JOIN sys.schemas s ON s.schema_id = v.schema_id
WHERE s.name = '$SchemaName'
"@

Write-Log -Level INFO -Message ("Found {0} views to create from source schema '{1}'." -f $viewsSource.Count, $SchemaName)

if($viewsSource.Count -ne 0){
    # Step 2: Create view dictionary
    $viewDictSource = @{}
    foreach ($view in $viewsSource) {
        $viewDictSource[$view.FullName] = $view
    }

    # Step 3: Get dependencies between views
    $dependenciesSource = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
    SELECT 
        OBJECT_SCHEMA_NAME(referencing_id) + '.' + OBJECT_NAME(referencing_id) AS Referencer,
        OBJECT_SCHEMA_NAME(referenced_id) + '.' + OBJECT_NAME(referenced_id) AS Referenced
    FROM sys.sql_expression_dependencies
    WHERE 
        OBJECTPROPERTY(referencing_id, 'IsView') = 1
        AND OBJECTPROPERTY(referenced_id, 'IsView') = 1
    "

    # Step 4: Filter internal dependencies
    $viewNamesSource = $viewDictSource.Keys
    $internalDepsSource = $dependenciesSource | Where-Object {
        $_.Referencer -in $viewNamesSource -and $_.Referenced -in $viewNamesSource
    }

    # Step 5: Topological sort (Kahn)
    $graph = @{}
    $inDegree = @{}

    foreach ($viewName in $viewNamesSource) {
        $graph[$viewName] = @()
        $inDegree[$viewName] = 0
    }

    foreach ($dep in $internalDepsSource) {
        $graph[$dep.Referenced] += $dep.Referencer
        $inDegree[$dep.Referencer]++
    }

    $queue = [System.Collections.Generic.Queue[string]]::new()
    $inDegree.Keys | Where-Object { $inDegree[$_] -eq 0 } | ForEach-Object { $queue.Enqueue($_) }

    $sortedSource = @()

    while ($queue.Count -gt 0) {
        $current = $queue.Dequeue()
        $sortedSource += $current

        foreach ($dependent in $graph[$current]) {
            $inDegree[$dependent]--
            if ($inDegree[$dependent] -eq 0) {
                $queue.Enqueue($dependent)
            }
        }
    }

    if ($sortedSource.Count -ne $viewNamesSource.Count) {
        Write-Log -Level ERROR -Message "Cycle detected in view dependencies. Cannot safely recreate views."
        return
    }

    $sortedSource = [System.Collections.ArrayList]::new($sortedSource)

    # Step 6: Scripting options
    $options = New-DbaScriptingOption
    $options.SchemaQualify = $true
    $options.IncludeHeaders = $false
    $options.ToFileOnly = $false
    $options.Indexes = $true  
    $options.WithDependencies = $false
    $options.ScriptBatchTerminator = $true

    # Step 7: Recreate each view
    foreach ($index in 0..($sortedSource.Count - 1)) {
        $viewName = $sortedSource[$index]
        $view = $viewDictSource[$viewName]

        Write-Log -Level DEBUG -Message "Creating view [$($view.SchemaName).$($view.ViewName)]"

        $ScriptViewAndIndex = Get-DbaDbView -SqlInstance $SqlInstance -Database $SourceDB -View "$($view.ViewName)" |
            Where-Object { $_.Schema -eq $view.SchemaName } |
            Export-DbaScript -ScriptingOptionObject $options -NoPrefix -Passthru 

        $cleanedScript = $ScriptViewAndIndex -join "`r`n"
        $viewCommandLog = $cleanedScript.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

        if (!$WhatIf) {
            try {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $cleanedScript -EnableException
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail
                (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE VIEW', 'VIEW', '$($view.SchemaName)', '$($view.ViewName)', 'CREATE', '$viewCommandLog', 0, 'Success', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            }
            catch {
                $errorMsg = $_.Exception.Message.Replace("'", "''")
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail
                (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE VIEW', 'VIEW', '$($view.SchemaName)', '$($view.ViewName)', 'CREATE', '$viewCommandLog', 1, '$errorMsg', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery

                Write-Log -Level ERROR -Message "Failed to create view [$($view.SchemaName).$($view.ViewName)] : $errorMsg"
                $ErrorCode = 1
                $ErrorCodeCreateView = 1
                if(!$ContinueOnError){
                    $end = Get-Date
                    $RestoreEndDatetime = $end.ToString("yyyy-MM-dd HH:mm:ss")
                    $logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId,RestoreStartDatetime,RestoreEndDatetime,SourceDB,TargetDB,SchemaName,ErrorCode,Message)
                    VALUES ($RestoreId,'$RestoreStartDatetime','$RestoreEndDatetime','$SourceDB','$TargetDB','$SchemaName',$ErrorCode,'Error during the CREATE VIEW')"
                    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                    return
                }
            }
        }
    }
}

$endStep = Get-Date
$durationInSeconds = ($endStep - $startStep).TotalSeconds
$roundedDuration = [math]::Round($durationInSeconds, 2)

if($ErrorCodeCreateView -eq 1){
    Write-Log -Level ERROR -Message "STEP : CREATE VIEW AFTER DROP INSERT in $roundedDuration seconds | FAILED"
}
else{
    Write-Log -Level INFO -Message "STEP : CREATE VIEW AFTER DROP INSERT in $roundedDuration seconds | SUCCEED"
}


#############################################################################################
## DROP PROCEDURES
#############################################################################################
Write-Log -Level INFO -Message "STEP : DROP PROCEDURES"
$ErrorCodeDropProcedure = 0
$startStep = Get-Date

# Retrieve stored procedures from target schema
$procsToDelete = Get-DbaDbStoredProcedure -SqlInstance $SqlInstance -Database $TargetDB | Where-Object { $_.Schema -eq $SchemaName }

Write-Log -Level INFO -Message ("Found {0} procedures to drop in target schema '{1}'." -f $procsToDelete.Count, $SchemaName)

# Parallel-safe dictionary to store drop results
$procDropResults = [System.Collections.Concurrent.ConcurrentDictionary[string, int]]::new()

$procsToDelete | ForEach-Object -Parallel {
    $ErrorCode = 0
    $procSchema = $_.Schema
    $procName = $_.Name
    $qualifiedName = "[$procSchema].[$procName]"
    
    $dropQuery = "IF OBJECT_ID(N'$qualifiedName', N'P') IS NOT NULL DROP PROCEDURE $qualifiedName;"
    $dropQueryLog = $dropQuery.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

    if (!$using:WhatIf) {
        try {
            Invoke-DbaQuery -SqlInstance $using:SqlInstance -Database $using:TargetDB -Query $dropQuery -EnableException
            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($using:RestoreId, 'DROP PROCEDURE', 'PROCEDURE', '$procSchema', '$procName', 'DROP', '$dropQueryLog', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $using:LogInstance -Database $using:LogDatabase -Query $logQuery
        } catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($using:RestoreId, 'DROP PROCEDURE', 'PROCEDURE', '$procSchema', '$procName', 'DROP', '$dropQueryLog', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $using:LogInstance -Database $using:LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }
    $dict = $using:procDropResults
    $null = $dict.TryAdd($procName, $ErrorCode)

} -ThrottleLimit $Parallel

foreach ($entry in $procDropResults.GetEnumerator()) {
    $procName = $entry.Key
    $code = $entry.Value

    Write-Log -Level DEBUG -Message "Dropping procedure [$procName]"

    if ($code -eq 1) {
        Write-Log -Level ERROR -Message "Failed to drop procedure [$procName]."
        $ErrorCode = 1
        $ErrorCodeDropProcedure = 1
    }
}

if($ErrorCode -eq 1 -and !$ContinueOnError){
    $end = Get-Date
    $RestoreEndDatetime = $end.ToString("yyyy-MM-dd HH:mm:ss")
    $logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId,RestoreStartDatetime,RestoreEndDatetime,SourceDB,TargetDB,SchemaName,ErrorCode,Message)
    VALUES ($RestoreId,'$RestoreStartDatetime','$RestoreEndDatetime','$SourceDB','$TargetDB','$SchemaName',$ErrorCode,'Error during the DROP PROCEDURE')"
    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
    return
}

$endStep = Get-Date
$durationInSeconds = ($endStep - $startStep).TotalSeconds
$roundedDuration = [math]::Round($durationInSeconds, 2)

if($ErrorCodeDropProcedure -eq 1){
    Write-Log -Level ERROR -Message "STEP : DROP PROCEDURES in $roundedDuration seconds | FAILED"
}
else{
    Write-Log -Level INFO -Message "STEP : DROP PROCEDURES in $roundedDuration seconds | SUCCEED"
}

#############################################################################################
## CREATE PROCEDURES
#############################################################################################
Write-Log -Level INFO -Message "STEP : CREATE PROCEDURES"
$ErrorCodeCreateProcedure = 0
$startStep = Get-Date

# Get procedures from source
$storedProcedures = Get-DbaDbStoredProcedure -SqlInstance $SqlInstance -Database $SourceDB | Where-Object { $_.Schema -eq $SchemaName }

Write-Log -Level INFO -Message ("Found {0} procedures to create from source schema '{1}'." -f $storedProcedures.Count, $SchemaName)

# Scripting options
$options = New-DbaScriptingOption
$options.SchemaQualify = $true
$options.IncludeHeaders = $false
$options.ToFileOnly = $false 
$options.WithDependencies = $false
$options.ScriptBatchTerminator = $true

# Generate scripts
$spScripts = @()

foreach ($sp in $storedProcedures) {
    try {
        $ScriptProcLines = $sp | Export-DbaScript -ScriptingOptionObject $options -NoPrefix -Passthru
        $scriptProc = $ScriptProcLines -join "`r`n"

        $spScripts += [pscustomobject]@{
            Schema = $SchemaName
            Name   = $sp.Name
            Script = $scriptProc
        }
    } catch {
        Write-Log -Level WARNING -Message "Error generating script for procedure '$($sp.Name)': $($_.Exception.Message)"
    }
}

# Thread-safe dictionary to collect results
$procCreateResults = [System.Collections.Concurrent.ConcurrentDictionary[string, int]]::new()

$spScripts | ForEach-Object -Parallel {
    $ErrorCode = 0
    $procSchema = $_.Schema
    $procName = $_.Name
    $scriptContent = $_.Script
    $scriptCleaned = $scriptContent -join "`r`n"
    $procCommandLog = $scriptCleaned.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

    if (!$using:WhatIf) {
        try {
            Invoke-DbaQuery -SqlInstance $using:SqlInstance -Database $using:TargetDB -Query $scriptCleaned -EnableException

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($using:RestoreId, 'CREATE PROCEDURE', 'PROCEDURE', '$procSchema', '$procName', 'CREATE', '$procCommandLog', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $using:LogInstance -Database $using:LogDatabase -Query $logQuery
        } catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($using:RestoreId, 'CREATE PROCEDURE', 'PROCEDURE', '$procSchema', '$procName', 'CREATE', '$procCommandLog', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $using:LogInstance -Database $using:LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }
    $dict = $using:procCreateResults
    $null = $dict.TryAdd($procName, $ErrorCode)

} -ThrottleLimit $Parallel

# Post-loop result logging
foreach ($entry in $procCreateResults.GetEnumerator()) {
    $procName = $entry.Key
    $code = $entry.Value
    Write-Log -Level DEBUG -Message "Creating procedure [$procName]"

    if ($code -eq 1) {
        Write-Log -Level ERROR -Message "Failed to create procedure [$procName]"
        $ErrorCode = 1
        $ErrorCodeCreateProcedure
    }
}

if($ErrorCode -eq 1 -and !$ContinueOnError){
    $end = Get-Date
    $RestoreEndDatetime = $end.ToString("yyyy-MM-dd HH:mm:ss")
    $logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId,RestoreStartDatetime,RestoreEndDatetime,SourceDB,TargetDB,SchemaName,ErrorCode,Message)
    VALUES ($RestoreId,'$RestoreStartDatetime','$RestoreEndDatetime','$SourceDB','$TargetDB','$SchemaName',$ErrorCode,'Error during the CREATE PROCEDURE')"
    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
    return
}

$endStep = Get-Date
$durationInSeconds = ($endStep - $startStep).TotalSeconds
$roundedDuration = [math]::Round($durationInSeconds, 2)

if($ErrorCodeCreateProcedure -eq 1){
    Write-Log -Level ERROR -Message "STEP : CREATE PROCEDURES in $roundedDuration seconds | FAILED"
}
else{
    Write-Log -Level INFO -Message "STEP : CREATE PROCEDURES in $roundedDuration seconds | SUCCEED"
}

#############################################################################################
## LOG TABLE
#############################################################################################

if(!$WhatIf){
    $end = Get-Date
    $durationInSeconds = ($end - $start).TotalSeconds
    $roundedDuration = [math]::Round($durationInSeconds, 2)

    if($ErrorCode -eq 1){
        Write-Log -Level ERROR -Message "RESTORE SCHEMA FAILED in total time $roundedDuration seconds"
    }
    else{
        Write-Log -Level INFO -Message "RESTORE SCHEMA SUCCEED in total time $roundedDuration seconds"
    }
    $RestoreEndDatetime = $end.ToString("yyyy-MM-dd HH:mm:ss")
    $logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId,RestoreStartDatetime,RestoreEndDatetime,SourceDB,TargetDB,SchemaName,ErrorCode,Message)
    VALUES ($RestoreId,'$RestoreStartDatetime','$RestoreEndDatetime','$SourceDB','$TargetDB','$SchemaName',$ErrorCode,'Message temporaire')"
    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
}


