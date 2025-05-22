# .\restoreSchema.ps1 -SqlInstance "LAPTOP-R6ED0C8E\DBA01" -SourceDB "AD2019" -TargetDB "AdventureWorks2019" -SchemaName "HumanResources" -TempScriptPath "D:\pacollet\Client\MinistereJustice\RestoreSchema\script"

param 
(
    [Parameter(Mandatory)] [string] $SqlInstance,
    [Parameter(Mandatory)] [string] $SourceDB,
    [Parameter(Mandatory)] [string] $TargetDB,
    [Parameter(Mandatory)] [string] $SchemaName,
    [Parameter(Mandatory)] [string] $TempScriptPath,
    [Parameter(Mandatory)] [string] $LogInstance,
    [Parameter(Mandatory)] [string] $LogDatabase,
    [Parameter()] [switch] $WhatIf,
    [Parameter()] [switch] $PurgeScript
)

$now = Get-Date
$RestoreStartDatetime = $now.ToString("yyyy-MM-dd HH:mm:ss")
$RestoreId = [DateTime]::UtcNow.Ticks
$ErrorCode = 0



$DebugParam = $True

# Create folder if doesn't exist
if (!(Test-Path $TempScriptPath)) {
    New-Item -ItemType Directory -Path $TempScriptPath
}

#############################################################################################
## INITIALIZE LOG TABLE IN THE LOGDATABASE
#############################################################################################
# Vérifier et créer les tables de log si nécessaire
Write-Host "CREATE LOG TABLES"

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
## GET SOURCE OBJECT
#############################################################################################
$sourceTables = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = '$SchemaName' AND TABLE_TYPE = 'BASE TABLE'
"

$sourceViews = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS 
    WHERE TABLE_SCHEMA = '$SchemaName'
"

$sourceProcedures = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
    SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES 
    WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = '$SchemaName'
"

#############################################################################################
## GET TARGET OBJECT
#############################################################################################
$targetTables = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query "
    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = '$SchemaName' AND TABLE_TYPE = 'BASE TABLE'
"

$targetViews = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query "
    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS 
    WHERE TABLE_SCHEMA = '$SchemaName'
"

$targetProcedures = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query "
    SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES 
    WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = '$SchemaName'
"

#############################################################################################
## CHECK CROSS SCHEMA OBJECT
#############################################################################################

# Get Table which are not in the source 
$tablesToDelete = $targetTables | Where-Object { $_.TABLE_NAME -notin $sourceTables.TABLE_NAME }
$viewsToDelete = $targetViews | Where-Object { $_.TABLE_NAME -notin $sourceViews.TABLE_NAME }


$objectsToDelete = @(
    @($tablesToDelete) + @($viewsToDelete) | ForEach-Object {
        "$SchemaName.$($_.TABLE_NAME)"
    }
)

$dependencies = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query @"
SELECT distinct
    OBJECT_SCHEMA_NAME(referencing_id) + '.' + OBJECT_NAME(referencing_id) AS Referencer,
    OBJECT_SCHEMA_NAME(referenced_id) + '.' + OBJECT_NAME(referenced_id) AS Referenced,
    OBJECT_SCHEMA_NAME(referencing_id) AS ReferencerSchema,
    OBJECT_NAME(referencing_id) AS ReferencerObject,
    OBJECT_NAME(referenced_id) AS ReferencedObject
FROM sys.sql_expression_dependencies
WHERE referencing_id IS NOT NULL AND referenced_id IS NOT NULL
"@

$externalViewsUsingOurObjects = $dependencies | Where-Object {
    ($_.Referenced -in $objectsToDelete) -and
    ($_.ReferencerSchema -ne $SchemaName) -and
    ($_.Referencer -ne $_.Referenced) -and  
    ($_ -ne $null)
}

if (($externalViewsUsingOurObjects | Select-Object -ExpandProperty Referencer -Unique).Count -gt 0) {
    $ErrorCode = 1
    $externalViewsUsingOurObjects | ForEach-Object{
        $ErrorList += "[$($_.Referencer)/$($_.Referenced)]"
    }
    $ErrorMessage = "Views in an other schema reference objects that you want to delete : $ErrorList"

    $RestoreEndDatetime = $now.ToString("yyyy-MM-dd HH:mm:ss")
    #$externalViewsUsingOurObjects | Select-Object Referencer, Referenced | Sort-Object Referenced | Format-Table

    $logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId,RestoreStartDatetime,RestoreEndDatetime,SourceDB,TargetDB,SchemaName,ErrorCode,Message)
    VALUES ($RestoreId,'$RestoreStartDatetime','$RestoreEndDatetime','$SourceDB','$TargetDB','$SchemaName',$ErrorCode,'$ErrorMessage')"

    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
    
    if($DebugParam){
        Write-Error $ErrorMessage
    }
    return
} 

#############################################################################################
## SORT AND DROP VIEW
#############################################################################################
if($DebugParam){
    Write-Host "DROP ORPHAN VIEW"
}

$viewsToDropFullNames = $viewsToDelete | ForEach-Object {
    "$SchemaName.$($_.TABLE_NAME)"
}

$viewDeps = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query @"
SELECT 
    OBJECT_SCHEMA_NAME(referencing_id) + '.' + OBJECT_NAME(referencing_id) AS Referencer,
    OBJECT_SCHEMA_NAME(referenced_id) + '.' + OBJECT_NAME(referenced_id) AS Referenced
FROM sys.sql_expression_dependencies
WHERE 
    OBJECTPROPERTY(referencing_id, 'IsView') = 1
    AND OBJECTPROPERTY(referenced_id, 'IsView') = 1
"@

# Filter only the internal dependencies between views to remove
$internalDeps = $viewDeps | Where-Object {
    ($_.Referencer -in $viewsToDropFullNames) -and
    ($_.Referenced -in $viewsToDropFullNames)
}

$graph = @{}
$inDegree = @{}

foreach ($view in $viewsToDropFullNames) {
    $graph[$view] = @()
    $inDegree[$view] = 0
}

foreach ($dep in $internalDeps) {
    $graph[$dep.Referenced] += $dep.Referencer
    $inDegree[$dep.Referencer]++
}

$queue = New-Object System.Collections.Generic.Queue[string]
$inDegree.Keys | Where-Object { $inDegree[$_] -eq 0 } | ForEach-Object { $queue.Enqueue($_) }

$sorted = @()

while ($queue.Count -gt 0) {
    $current = $queue.Dequeue()
    $sorted += $current

    foreach ($dependent in $graph[$current]) {
        $inDegree[$dependent]--
        if ($inDegree[$dependent] -eq 0) {
            $queue.Enqueue($dependent)
        }
    }
}

# Check
if ($sorted.Count -ne $viewsToDropFullNames.Count) {
    Write-Error "Cycle détecté dans les dépendances entre vues."
    return
}

# Reverse for deletion (from the most dependent to the least dependent)
$sorted = [System.Collections.ArrayList]::new($sorted)
$sorted.Reverse()


if (!$WhatIf) {
    $sorted | ForEach-Object {
        $viewName=$_
        $dropViewQuery="DROP VIEW $_"
        

        try {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $dropViewQuery -EnableException

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate) 
            VALUES ($RestoreId, 'DROP ORPHAN VIEW', 'VIEW', '$SchemaName', '$viewName', 'DROP', '$($dropViewQuery.Replace("'", "''"))', 0, 'Success', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail(RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate) 
            VALUES ($RestoreId, 'DROP ORPHAN VIEW', 'VIEW', '$SchemaName', '$viewName', 'DROP', '$($dropViewQuery.Replace("'", "''"))', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }

}

#############################################################################################
## DELETE ORPHAN TABLE
#############################################################################################
if($DebugParam){
    Write-Host "DROP FOREIGN KEY OF ORPHAN TABLE"
}

# Delete foreign Key for each table not in the source
foreach ($table in $tablesToDelete) {
    $tableName = $table.TABLE_NAME

    # Get FK for each table
    $fkConstraints = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query "
    SELECT
        fk.name AS ForeignKeyName,
        sch_parent.name AS ReferencingSchema,
        t_parent.name AS ReferencingTable
    FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.tables t_parent ON fkc.parent_object_id = t_parent.object_id
        INNER JOIN sys.schemas sch_parent ON t_parent.schema_id = sch_parent.schema_id
        INNER JOIN sys.columns c_parent ON fkc.parent_object_id = c_parent.object_id AND fkc.parent_column_id = c_parent.column_id
        INNER JOIN sys.tables t_ref ON fkc.referenced_object_id = t_ref.object_id
        INNER JOIN sys.schemas sch_ref ON t_ref.schema_id = sch_ref.schema_id
        INNER JOIN sys.columns c_ref ON fkc.referenced_object_id = c_ref.object_id AND fkc.referenced_column_id = c_ref.column_id
    WHERE sch_ref.name = '$SchemaName' and t_ref.name='$tableName'
    "
    # Delete FK constraint
    foreach ($fk in $fkConstraints) {
        $fkName = $fk.ForeignKeyName
        $fkSchemaName = $fk.ReferencingSchema
        $fkTableName = $fk.ReferencingTable
        $dropFkQuery = "ALTER TABLE [$fkSchemaName].[$fkTableName] DROP CONSTRAINT [$fkName]"
        if($DebugParam){
            Write-Host "ALTER TABLE [$fkSchemaName].[$fkTableName] DROP CONSTRAINT [$fkName]"
        }
        if(!$WhatIf){
            try {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $dropFkQuery -EnableException

                $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'DROP FOREIGN KEY OF ORPHAN TABLE', 'FOREIGN KEY', '$fkSchemaName', '$fkName', 'DROP', '$($dropFkQuery.Replace("'", "''"))', 0, 'Success', GETDATE())
                "
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            }
            catch {
                $errorMsg = $_.Exception.Message.Replace("'", "''")  
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'DROP FOREIGN KEY OF ORPHAN TABLE', 'FOREIGN KEY', '$fkName', 'DROP', '$($dropFkQuery.Replace("'", "''"))', 1, '$errorMsg', GETDATE())
                "
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                $ErrorCode = 1
            }         
        }
    }
}

if($DebugParam){
    Write-Host "DROP ORPHAN TABLE"
}

# Drop Table which are not in the source
foreach ($table in $tablesToDelete) {
    $tableName = $table.TABLE_NAME
    $fullTable = "[$SchemaName].[$tableName]"
    $dropTableQuery = "DROP TABLE $fullTable"

    if ($DebugParam) {
        Write-Host $dropTableQuery
    }

    if (!$WhatIf) {
        try {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $dropTableQuery -EnableException

            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail(RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate) 
            VALUES ($RestoreId, 'DROP ORPHAN TABLE', 'TABLE', '$SchemaName', '$tableName', 'DROP', '$($dropTableQuery.Replace("'", "''"))', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")  
            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate) 
            VALUES ($RestoreId, 'DROP ORPHAN TABLE', 'TABLE', '$SchemaName', '$tableName', 'DROP', '$($dropTableQuery.Replace("'", "''"))', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }
}


#############################################################################################
## DROP ORPHAN STORED PROCEDURE
#############################################################################################
if($DebugParam){
    Write-Host "DROP ORPHAN STORED PROCEDURE"
}

# Get Stored Procedure which are not in the source 
$procsToDelete = $targetProcedures | Where-Object { $_.ROUTINE_NAME -notin $sourceProcedures.ROUTINE_NAME }

# Drop Procedure which are not in the source
foreach ($proc in $procsToDelete) {
    $procName = $proc.ROUTINE_NAME
    $fullProc = "[$SchemaName].[$procName]"
    $dropProcQuery = "DROP PROCEDURE $fullProc"

    if ($DebugParam) {
        Write-Host $dropProcQuery
    }

    if (!$WhatIf) {
        try {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $dropProcQuery -EnableException

            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail(RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'DROP ORPHAN STORED PROCEDURE', 'STORED PROCEDURE', '$SchemaName', '$procName', 'DROP', '$($dropProcQuery.Replace("'", "''"))', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'DROP ORPHAN STORED PROCEDURE', 'STORED PROCEDURE', '$SchemaName', '$procName', 'DROP', '$($dropProcQuery.Replace("'", "''"))', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }
}

#############################################################################################
## CHECK CROSS SCHEMA OBJECT TO DROP VIEW BEFORE TRUNCATE INSERT DATA
#############################################################################################
$dependentViewsTarget = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query @"
SELECT 
    OBJECT_SCHEMA_NAME(d.referencing_id) AS ViewSchema,
    OBJECT_NAME(d.referencing_id) AS ViewName,
    OBJECT_SCHEMA_NAME(d.referenced_id) AS ReferencedSchema,
    OBJECT_NAME(d.referenced_id) AS ReferencedObject
FROM sys.sql_expression_dependencies d
WHERE 
    OBJECTPROPERTY(d.referencing_id, 'IsView') = 1
    AND OBJECTPROPERTY(d.referencing_id, 'IsSchemaBound') = 1
    AND OBJECT_SCHEMA_NAME(d.referenced_id) = '$SchemaName'
    AND OBJECT_SCHEMA_NAME(d.referencing_id) <> '$SchemaName'
"@


if ($dependentViewsTarget.Count -gt 0) {
    Write-Error "❌ Des vues WITH SCHEMABINDING d'autres schémas référencent des objets dans le schéma '$SchemaName' de la Target. Impossible de modifier ces tables."
    $dependentViewsTarget | Format-Table ViewSchema, ViewName, ReferencedSchema, ReferencedObject
    return
} else {
    Write-Host "✅ Aucun lien SCHEMABINDING externe détecté sur la target. OK pour TRUNCATE/INSERT."
}


# --- ÉTAPE 1 : Récupérer les vues du schéma ---
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

# --- Étape 1 : Construire un dictionnaire (nom complet => objet vue)
$viewDictTarget = @{}
foreach ($view in $viewsTarget) {
    $viewDictTarget[$view.FullName] = $view
}

# --- Étape 2 : Obtenir toutes les dépendances entre vues
$dependenciesTarget = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query @"
SELECT 
    OBJECT_SCHEMA_NAME(referencing_id) + '.' + OBJECT_NAME(referencing_id) AS Referencer,
    OBJECT_SCHEMA_NAME(referenced_id) + '.' + OBJECT_NAME(referenced_id) AS Referenced
FROM sys.sql_expression_dependencies
WHERE 
    OBJECTPROPERTY(referencing_id, 'IsView') = 1
    AND OBJECTPROPERTY(referenced_id, 'IsView') = 1
"@

# --- Étape 3 : Filtrer les dépendances internes
$viewNamesTarget = $viewDictTarget.Keys
$internalDepsTarget = $dependenciesTarget | Where-Object {
    $_.Referencer -in $viewNamesTarget -and $_.Referenced -in $viewNamesTarget
}

# --- Étape 4 : Construire graphe + in-degree
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

# --- Étape 5 : Tri topologique (Kahn)
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

# --- Étape 6 : Vérification cycle
if ($sortedTarget.Count -ne $viewNamesTarget.Count) {
    Write-Error "⚠️ Cycle détecté dans les dépendances entre vues."
    return
}

# --- Étape 7 : Inverser pour DROP
$sortedTarget = [System.Collections.ArrayList]::new($sortedTarget)
$sortedTarget.Reverse()

$viewsFolder = Join-Path $TempScriptPath "views"
if (-not (Test-Path $viewsFolder)) {
    New-Item -ItemType Directory -Path $viewsFolder | Out-Null
}


# --- Étape 1 : Récupérer toutes les vues du schéma dans la source
foreach ($viewName in $sortedTarget) {
    $view = $viewDictTarget[$viewName]  # Contient FullName, SchemaName, ViewName, IsSchemaBound

    $dropViewQuery = "DROP VIEW $viewName"
    Write-Host $dropViewQuery

    try {
        Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $dropViewQuery

        $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
        VALUES ($RestoreId, 'DROP VIEW BEFORE TRUNCATE INSERT DATA', 'VIEW', '$($view.SchemaName)', '$($view.ViewName)', 'DROP', '$dropViewQuery', 0, 'Success', GETDATE())"
        Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'DROP VIEW BEFORE TRUNCATE INSERT DATA', 'VIEW', '$($view.SchemaName)', '$($view.ViewName)', 'DROP', '$dropViewQuery', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    
}

#############################################################################################
## DISABLED FOREIGN KEY TO TRUNCATE INSERT DATA
#############################################################################################
if($DebugParam){
    Write-Host "DISABLED FOREIGN KEY TO TRUNCATE INSERT DATA"
}

# Get all foreign keys that reference a table in the schema
$foreignKey = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query "
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
"

# drop the constraint for each foreign key
foreach ($fk in $foreignKey) {
    $schema = $fk.ReferencingSchema
    $table = $fk.ReferencingTable
    $fkName = $fk.ForeignKeyName

    $query = "ALTER TABLE [$schema].[$table] DROP CONSTRAINT [$fkName];"
    if($DebugParam){
        Write-Host "ALTER TABLE [$schema].[$table] DROP CONSTRAINT [$fkName];"
    }
    if(!$WhatIf){
        try {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $query -EnableException

            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'DISABLED FOREIGN KEY TO TRUNCATE INSERT DATA', 'FOREIGN KEY', '$schema', '$fkName', 'DROP', '$($query.Replace("'", "''"))', 0, 'Success', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")  
            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'DISABLED FOREIGN KEY TO TRUNCATE INSERT DATA', 'FOREIGN KEY', '$schema', '$fkName', 'DROP', '$($query.Replace("'", "''"))', 1, '$errorMsg', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                $ErrorCode = 1
            }         
        }
}

#############################################################################################
## TRUNCATE INSERT DATA
#############################################################################################
if($DebugParam){
    Write-Host "TRUNCATE INSERT DATA"
}

# truncate insert data for each table
foreach ($table in $sourceTables) {
    $tableName = $table.TABLE_NAME
    $qualifiedName = "[$SchemaName].[$tableName]"
    $truncateQuery= "TRUNCATE TABLE $qualifiedName"

    # Truncate table
    if ($DebugParam) {
        Write-Host "TRUNCATE TABLE $qualifiedName"
    }
    if (!$WhatIf) {
        try {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $truncateQuery -EnableException

            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'TRUNCATE DATA', 'TABLE', '$SchemaName', '$tableName', 'TRUNCATE', '$($truncateQuery.Replace("'", "''"))', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail(RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'TRUNCATE DATA', 'TABLE', '$SchemaName', '$tableName', 'TRUNCATE', '$($truncateQuery.Replace("'", "''"))', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }

    # Vérifier si la table a une colonne IDENTITY
    $hasIdentity = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
        SELECT COUNT(*) 
        FROM sys.columns 
        WHERE object_id = OBJECT_ID('$SchemaName.$tableName') 
          AND is_identity = 1
    " | Select-Object -ExpandProperty Column1

    # Récupérer colonnes insérables avec leur type
    $columns = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
        SELECT 
            c.name, 
            t.name AS data_type
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID('$SchemaName.$tableName')
            AND c.is_computed = 0
        ORDER BY c.column_id
    "

    # Construire les listes de colonnes
    $insertColumns = @()
    $selectColumns = @()

    foreach ($col in $columns) {
        $colName = "[$($col.name)]"
        $insertColumns += $colName

        # Gestion spéciale pour XML (ajouter d'autres types au besoin)
        switch ($col.data_type.ToLower()) {
            "xml" { $selectColumns += "CONVERT(XML, $colName) AS $colName" }
            "geography" { $selectColumns += "CAST($colName AS GEOGRAPHY) AS $colName" }
            "geometry" { $selectColumns += "CAST($colName AS GEOMETRY) AS $colName" }
            default { $selectColumns += $colName }
        }       

    }

    $insertList = $insertColumns -join ", "
    $selectList = $selectColumns -join ", "

    $insertQuery = "INSERT INTO [$TargetDB].[$SchemaName].[$tableName] ($insertList) SELECT $selectList FROM [$SourceDB].[$SchemaName].[$tableName]"

    if ($DebugParam) {
        Write-Host $insertQuery
    }

    if (!$WhatIf) {
        try {
            if ($hasIdentity -gt 0) {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query "SET IDENTITY_INSERT [$SchemaName].[$tableName] ON;$insertQuery;SET IDENTITY_INSERT [$SchemaName].[$tableName] OFF;" -EnableException
            }
            else{
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $insertQuery -EnableException
            }

            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'INSERT DATA', 'TABLE', '$SchemaName', '$tableName', 'INSERT', '$($insertQuery.Replace("'", "''"))', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'INSERT DATA', 'Table', '$SchemaName', '$tableName', 'INSERT', '$($insertQuery.Replace("'", "''"))', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }
}



#############################################################################################
## ENABLED FOREIGN KEY 
#############################################################################################
if($DebugParam){
     Write-Host "ENABLED FOREIGN KEY CONSTRAINT"
}

$fkFolder = Join-Path $TempScriptPath "foreignkey"

if (!(Test-Path -Path $fkFolder)) {
    New-Item -Path $fkFolder -ItemType Directory | Out-Null
}

# Get all foreign keys that reference a table in the schema
$foreignKey = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
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
"
# Get all foreign key object
$foreignKeyObject = Get-DbaDbForeignKey -SqlInstance $SqlInstance -Database $SourceDB | Where-Object { $_.Name -in $foreignKey.ForeignKeyName }

# Export of all foreign key script
if(!$WhatIf){
    $foreignKeyObject | ForEach-Object {
        $filePath = Join-Path $fkFolder "$($_.Name).sql"

        $null = $_ | Export-DbaScript -FilePath $filePath 
    }
}

Get-ChildItem -Path $fkFolder -Filter "*.sql" | ForEach-Object {
    $file = $_.FullName
    write-host $file
    $commandRaw = Get-Content $file -Raw
    $command = $commandRaw.Replace("'", "''")
    $command = $command -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

    if (!$WhatIf) {
        try {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -File $file -EnableException

            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES($RestoreId, 'ENABLED FOREIGN KEY CONSTRAINT', 'FOREIGN KEY', NULL, '$file', 'ALTER', '$($command.Replace("'", "''"))', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'ENABLED FOREIGN KEY CONSTRAINT', 'FOREIGN KEY', NULL, '$file', 'ALTER', '$(command.Replace("'", "''"))', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }

        
    }
}

if($ErrorCode -eq 0){
    Get-ChildItem -Path $fkFolder -Filter "*.sql" | Remove-Item -Force
}

#############################################################################################
## CREATE VIEW AFTER TRUNCATE INSERT DATA
#############################################################################################
$dependentViewsSource = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query @"
SELECT 
    OBJECT_SCHEMA_NAME(d.referencing_id) AS ViewSchema,
    OBJECT_NAME(d.referencing_id) AS ViewName,
    OBJECT_SCHEMA_NAME(d.referenced_id) AS ReferencedSchema,
    OBJECT_NAME(d.referenced_id) AS ReferencedObject
FROM sys.sql_expression_dependencies d
WHERE 
    OBJECTPROPERTY(d.referencing_id, 'IsView') = 1
    AND OBJECTPROPERTY(d.referencing_id, 'IsSchemaBound') = 1
    AND OBJECT_SCHEMA_NAME(d.referenced_id) = '$SchemaName'
    AND OBJECT_SCHEMA_NAME(d.referencing_id) <> '$SchemaName'
"@

if ($dependentViewsSource.Count -gt 0) {
    Write-Error "❌ Des vues WITH SCHEMABINDING d'autres schémas référencent des objets dans le schéma '$SchemaName' de la Target. Impossible de modifier ces tables."
    $dependentViewsSource | Format-Table ViewSchema, ViewName, ReferencedSchema, ReferencedObject
    return
} else {
    Write-Host "✅ Aucun lien SCHEMABINDING externe détecté sur la source. OK pour TRUNCATE/INSERT."
}

# --- ÉTAPE 1 : Récupérer les vues du schéma ---
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


# --- Étape 1 : Construire un dictionnaire (nom complet => objet vue)
$viewDictSource = @{}
foreach ($view in $viewsSource) {
    $viewDictSource[$view.FullName] = $view
}

# --- Étape 2 : Obtenir toutes les dépendances entre vues
$dependenciesSource = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query @"
SELECT 
    OBJECT_SCHEMA_NAME(referencing_id) + '.' + OBJECT_NAME(referencing_id) AS Referencer,
    OBJECT_SCHEMA_NAME(referenced_id) + '.' + OBJECT_NAME(referenced_id) AS Referenced
FROM sys.sql_expression_dependencies
WHERE 
    OBJECTPROPERTY(referencing_id, 'IsView') = 1
    AND OBJECTPROPERTY(referenced_id, 'IsView') = 1
"@

# --- Étape 3 : Filtrer les dépendances internes
$viewNamesSource = $viewDictSource.Keys
$internalDepsSource = $dependenciesSource | Where-Object {
    $_.Referencer -in $viewNamesSource -and $_.Referenced -in $viewNamesSource
}

# --- Étape 4 : Construire graphe + in-degree
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

# --- Étape 5 : Tri topologique (Kahn)
$queue = New-Object System.Collections.Generic.Queue[string]
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

# --- Étape 6 : Vérification cycle
if ($sortedSource.Count -ne $viewNamesSource.Count) {
    Write-Error "⚠️ Cycle détecté dans les dépendances entre vues."
    return
}

# --- Étape 7 : Inverser pour DROP
$sortedSource = [System.Collections.ArrayList]::new($sortedSource)


$smoServer = New-Object Microsoft.SqlServer.Management.Smo.Server $SqlInstance
$smoDb = $smoServer.Databases[$SourceDB]

$scripter = New-Object Microsoft.SqlServer.Management.Smo.Scripter ($smoServer)
$scripter.Options.SchemaQualify = $true
$scripter.Options.IncludeHeaders = $false
$scripter.Options.ToFileOnly = $false
$scripter.Options.Indexes = $false
$scripter.Options.WithDependencies = $false



foreach ($index in 0..($sortedSource.Count - 1)) {
    $viewName = $sortedSource[$index]
    $view = $viewDictSource[$viewName]
    $smoView = $smoDb.Views[$view.ViewName, $view.SchemaName]

    if ($smoView -eq $null) {
        Write-Warning "Vue $($view.SchemaName).$($view.ViewName) non trouvée dans SMO."
        continue
    }

    
    # ░░░ STEP 1 : CREATE VIEW
    $scriptViewOnly = $scripter.Script($smoView) -join "`r`n"

    # Delete everything before CREATE VIEW
    $createViewIndex = $scriptViewOnly.IndexOf('CREATE VIEW')
    $cleanedScript = $scriptViewOnly.Substring($createViewIndex)
    
    $viewCommandLog = $cleanedScript.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

    try {
        if (!$WhatIf) {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $cleanedScript -EnableException

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'CREATE VIEW', 'VIEW', '$($view.SchemaName)', '$($view.ViewName)', 'CREATE', '$viewCommandLog', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
    }
    catch {
        $errorMsg = $_.Exception.Message.Replace("'", "''")
        $logQuery = "
        INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
        VALUES ($RestoreId, 'CREATE VIEW', 'VIEW', '$($view.SchemaName)', '$($view.ViewName)', 'CREATE', '$viewCommandLog', 1, '$errorMsg', GETDATE())"
        Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        $ErrorCode = 1
        continue  # On ne tente pas de créer les index si la vue échoue
    }

    # ░░░ STEP 2 : CREATE INDEXES (un par un, avec try/catch)
    foreach ($indexObj in $smoView.Indexes) {
        $scriptIndex = $scripter.Script($indexObj) -join "`r`n"
        $indexCommandLog = $scriptIndex.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

        try {
            if (!$WhatIf) {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $scriptIndex -EnableException

                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE INDEX ON VIEW', 'INDEX', '$($view.SchemaName)', '$($indexObj.Name)', 'CREATE', '$indexCommandLog', 0, 'Success', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            }
        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'CREATE INDEX ON VIEW', 'INDEX', '$($view.SchemaName)', '$($indexObj.Name)', 'CREATE', '$indexCommandLog', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }
}



#############################################################################################
## CREATE/ALTER PROCEDURES
#############################################################################################
if ($DebugParam) {
    Write-Host "CREATE/ALTER PROCEDURES"
}

# Get procedures in the schema
$storedProcedures = Get-DbaDbStoredProcedure -SqlInstance $SqlInstance -Database $SourceDB | Where-Object { $_.Schema -eq $SchemaName }

if (!$WhatIf) {
    foreach ($proc in $storedProcedures) {
        $procSchema = $proc.Schema
        $procName = $proc.Name
        $qualifiedName = "[$procSchema].[$procName]"

        $dropQuery = "IF OBJECT_ID(N'$qualifiedName', N'P') IS NOT NULL DROP PROCEDURE $qualifiedName;"
        $dropQueryLog = $dropQuery.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "
        # DROP PROCEDURE
        try {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $dropQuery -EnableException

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'DROP PROCEDURE', 'PROCEDURE', '$procSchema', '$procName', 'DROP', '$dropQueryLog', 0, 'Success', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        } catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'DROP PROCEDURE', 'PROCEDURE', '$procSchema', '$procName', 'DROP', '$dropQueryLog', 1, '$errorMsg', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }

        # CREATE PROCEDURE (via SMO)
        try {
            $scriptParts = $proc.Script()
            $scriptRaw = $scriptParts -join "`r`n"

            # Nettoyer tout ce qui précède CREATE PROCEDURE (insensible à la casse)
            $createIndex = $scriptRaw.IndexOf("CREATE PROCEDURE", [System.StringComparison]::OrdinalIgnoreCase)
            

            $scriptCleaned = $scriptRaw.Substring($createIndex)
            $procCommandLog = $scriptCleaned.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "
            
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $scriptCleaned -EnableException

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'CREATE PROCEDURE', 'PROCEDURE', '$procSchema', '$procName', 'CREATE', '$procCommandLog', 0, 'Success', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        } catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'CREATE PROCEDURE', 'PROCEDURE', '$procSchema', '$procName', 'CREATE', '$procCommandLog', 1, '$errorMsg', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }
}



#############################################################################################
## LOG TABLE
#############################################################################################

$RestoreEndDatetime = $now.ToString("yyyy-MM-dd HH:mm:ss")

$logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId,RestoreStartDatetime,RestoreEndDatetime,SourceDB,TargetDB,SchemaName,ErrorCode,Message)
VALUES ($RestoreId,'$RestoreStartDatetime','$RestoreEndDatetime','$SourceDB','$TargetDB','$SchemaName',$ErrorCode,'Message temporaire')"

Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery





















