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

$smoServer = New-Object Microsoft.SqlServer.Management.Smo.Server $SqlInstance
$smoDb = $smoServer.Databases[$SourceDB]

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
## DROP INSERT DATA
#############################################################################################
if($DebugParam){
    Write-Host "DROP INSERT DATA"
}

# truncate insert data for each table
foreach ($table in $sourceTables) {
    $tableName = $table.TABLE_NAME
    $qualifiedName = "[$SchemaName].[$tableName]"
    

    $smoTable = Get-DbaDbTable -SqlInstance $SqlInstance -Database $SourceDB -Schema $SchemaName -Table $tableName
    $dropQuery = "IF OBJECT_ID('$qualifiedName', 'U') IS NOT NULL DROP TABLE $qualifiedName"

    $dropQueryLog = $dropQuery.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "


    # Truncate table
    if ($DebugParam) {
        Write-Host "$dropQuery"
    }
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

    # 2. CREATE TABLE sans contraintes
    $scripter = New-Object Microsoft.SqlServer.Management.Smo.Scripter ($smoServer)
    $scripter.Options.ScriptDrops = $false
    $scripter.Options.WithDependencies = $false
    $scripter.Options.DriPrimaryKey = $false
    $scripter.Options.DriUniqueKeys = $false
    $scripter.Options.DriForeignKeys = $false
    $scripter.Options.Indexes = $false
    $scripter.Options.DriChecks = $true


    $createTableScript = $scripter.Script($smoTable) -join "`r`n"
    $createTableScriptLog = $createTableScript.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

    try {
        if (!$WhatIf) {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $createTableScript -EnableException

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'CREATE TABLE', 'TABLE', '$SchemaName', '$tableName', 'CREATE', '$createTableScriptLog', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
    }
    catch {
        $errorMsg = $_.Exception.Message.Replace("'", "''")
        $logQuery = "
        INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
        VALUES ($RestoreId, 'CREATE TABLE', 'TABLE', '$SchemaName', '$tableName', 'CREATE', '$createTableScriptLog', 1, '$errorMsg', GETDATE())"
        Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        $ErrorCode = 1
        continue  # On ne tente pas de créer les index si la vue échoue
    }

    # 5. Création des contraintes par défaut (Default Constraints)
    $defaultConstraints = $smoTable.Columns | Where-Object { $_.DefaultConstraint -ne $null }

    foreach ($col in $defaultConstraints) {
        $df = $col.DefaultConstraint
        $dfScript = $scripter.Script($df) -join "`r`n"
        $dfScriptLog = $dfScript.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

        try {
            if (!$WhatIf) {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $dfScript -EnableException

                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE DEFAULT CONSTRAINT', 'CONSTRAINT', '$SchemaName', '$($df.Name)', 'CREATE', '$dfScriptLog', 0, 'Success', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            }
        } catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'CREATE DEFAULT CONSTRAINT', 'CONSTRAINT', '$SchemaName', '$($df.Name)', 'CREATE', '$dfScriptLog', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }

    # 4. Création index Cluster Columnstore s’il y en a
    $smoIndexes = $smoTable.Indexes | Where-Object { $_.IndexType -eq 'ClusteredColumnstoreIndex' }
    foreach ($index in $smoIndexes) {
        $indexScript = $scripter.Script($index) -join "`r`n"
        $indexScriptLog = $indexScript.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "
        
        try {
            # Tenter de créer l'index sur la base cible
            if (!$WhatIf) {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $indexScript -EnableException
            
                # Log de succès
                $indexScriptLog = $indexScript.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE INDEX', 'INDEX', '$SchemaName', '$($index.Name)', 'CREATE', '$indexScriptLog', 0, 'Success', GETDATE())"
                
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            }
        } catch {
            # Gestion des erreurs
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'CREATE INDEX', 'INDEX', '$SchemaName', '$($index.Name)', 'CREATE', '$indexScriptLog', 1, '$errorMsg', GETDATE())"
            
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }

    # 5. INSERT DATA
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
    $insertQuery = "INSERT INTO [$TargetDB].[$SchemaName].[$tableName] ($insertList) SELECT $selectList FROM [$SourceDB].[$SchemaName].[$tableName]"

    $hasIdentity = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
        SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('$SchemaName.$tableName') AND is_identity = 1
    " | Select-Object -ExpandProperty Column1

    if ($hasIdentity -gt 0) {
        $fullInsert = "SET IDENTITY_INSERT [$SchemaName].[$tableName] ON; $insertQuery; SET IDENTITY_INSERT [$SchemaName].[$tableName] OFF;"
    } else {
        $fullInsert = $insertQuery
    }
    $insertLog = $fullInsert.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "


    try {
        

        if (!$WhatIf) {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $fullInsert -EnableException

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'INSERT DATA', 'TABLE', '$SchemaName', '$tableName', 'INSERT', '$insertLog', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        }
    }
    catch {
        $errorMsg = $_.Exception.Message.Replace("'", "''")

        $logQuery = "
        INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
        VALUES ($RestoreId, 'INSERT DATA', 'TABLE', '$SchemaName', '$tableName', 'INSERT', '$insertLog', 1, '$errorMsg', GETDATE())"
        
        Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        $ErrorCode = 1
    }

    # 6. Création des autres index (hors Cluster Columnstore déjà créés)
    $remainingIndexes = $smoTable.Indexes | Where-Object { $_.IndexType -ne 'ClusteredColumnstoreIndex' }

    foreach ($index in $remainingIndexes) {
        $indexScript = $scripter.Script($index) -join "`r`n"

        $indexScriptLog = $indexScript.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "
        try {
            if (!$WhatIf) {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $indexScript -EnableException

                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE INDEX', 'INDEX', '$SchemaName', '$($index.Name)', 'CREATE', '$indexScriptLog', 0, 'Success', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            }

        }
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")
            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'CREATE INDEX', 'INDEX', '$SchemaName', '$($index.Name)', 'CREATE', '$indexScriptLog', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery

            $ErrorCode = 1
        }
    }

    # 6. Création des triggers
    foreach ($trigger in $smoTable.Triggers) {
        if ($trigger.IsSystemObject -eq $false) {
            $triggerScript = $scripter.Script($trigger) -join "`r`n"

            $createTrigger = $triggerScript.IndexOf("CREATE TRIGGER", [System.StringComparison]::OrdinalIgnoreCase)
            $triggerScriptCleaned = $triggerScript.Substring($createTrigger)
            $triggerScriptLog = $triggerScriptCleaned.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

            try {
                if (!$WhatIf) {
                    Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $triggerScriptCleaned -EnableException

                    $logQuery = "
                    INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                    VALUES ($RestoreId, 'CREATE TRIGGER', 'TRIGGER', '$SchemaName', '$($trigger.Name)', 'CREATE', '$triggerScriptLog', 0, 'Success', GETDATE())"
                    Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                }
            } catch {
                $errorMsg = $_.Exception.Message.Replace("'", "''")
                $logQuery = "
                INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE TRIGGER', 'TRIGGER', '$SchemaName', '$($trigger.Name)', 'CREATE', '$triggerScriptLog', 1, '$errorMsg', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                $ErrorCode = 1
            }
        }
    }


}



#############################################################################################
## ENABLED FOREIGN KEY 
#############################################################################################
if ($DebugParam) {
    Write-Host "ENABLED FOREIGN KEY CONSTRAINT"
}

# Récupération des FK à rétablir (celles qui pointent vers les tables du schéma cible)
$foreignKeyNames = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
    SELECT
    fk.name AS ForeignKeyName
    FROM sys.foreign_keys fk
    INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    INNER JOIN sys.tables t_parent ON fkc.parent_object_id = t_parent.object_id
    INNER JOIN sys.schemas sch_parent ON t_parent.schema_id = sch_parent.schema_id
    INNER JOIN sys.tables t_ref ON fkc.referenced_object_id = t_ref.object_id
    INNER JOIN sys.schemas sch_ref ON t_ref.schema_id = sch_ref.schema_id
    WHERE sch_parent.name = '$SchemaName'
    OR sch_ref.name = '$SchemaName'
"
# Récupération des objets FK avec SMO
$foreignKeyObjects = Get-DbaDbForeignKey -SqlInstance $SqlInstance -Database $SourceDB |
    Where-Object { $_.Name -in $foreignKeyNames.ForeignKeyName }

if (!$WhatIf) {
    foreach ($fk in $foreignKeyObjects) {
        try {
            $scriptParts = $fk.Script()
            $scriptText = $scriptParts -join "`r`n"

            # Logging SQL propre
            $commandLog = $scriptText.Replace("'", "''") -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

            # Exécution
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $scriptText -EnableException

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES($RestoreId, 'ENABLED FOREIGN KEY CONSTRAINT', 'FOREIGN KEY', '$($fk.Parent.Schema)', '$($fk.Parent.Name)', 'ALTER', '$commandLog', 0, 'Success', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
        } catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")

            $logQuery = "
            INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'ENABLED FOREIGN KEY CONSTRAINT', 'FOREIGN KEY', '$($fk.Parent.Schema)', '$($fk.Parent.Name)', 'ALTER', '$commandLog', 1, '$errorMsg', GETDATE())
            "
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }
    }
}


#############################################################################################
## CREATE VIEW AFTER TRUNCATE INSERT DATA
#############################################################################################
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





















