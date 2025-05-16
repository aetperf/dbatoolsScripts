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
        ErrorCode BIT
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
## DELETE ORPHAN TABLE
#############################################################################################
if($DebugParam){
    Write-Host "DROP FOREIGN KEY OF ORPHAN TABLE"
}

# Get Table which are not in the source 
$tablesToDelete = $targetTables | Where-Object { $_.TABLE_NAME -notin $sourceTables.TABLE_NAME }

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
## DELETE ORPHAN VIEWS
#############################################################################################
if($DebugParam){
    Write-Host "DROP ORPHAN VIEW"
}

# Get Views which are not in the source 
$viewsToDelete = $targetViews | Where-Object { $_.TABLE_NAME -notin $sourceViews.TABLE_NAME }

# Drop View which are not in the source
foreach ($view in $viewsToDelete) {
    $viewName = $view.TABLE_NAME
    $fullView = "[$SchemaName].[$viewName]"
    $dropViewQuery = "DROP VIEW $fullView"

    if ($DebugParam) {
        Write-Host $dropViewQuery
    }

    if (!$WhatIf) {
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
# Get all foreign key object
$foreignKeyObject = Get-DbaDbForeignKey -SqlInstance $SqlInstance -Database $TargetDB | Where-Object { $_.Name -in $foreignKey.ForeignKeyName }

# Export of all foreign key script
if(!$WhatIf){
    $res = $foreignKeyObject | Export-DbaScript -Path $TempScriptPath
}

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

    # Récupérer colonnes insérables avec leur type
    $columns = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $SourceDB -Query "
        SELECT 
            c.name, 
            t.name AS data_type
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID('$SchemaName.$tableName')
            AND c.is_computed = 0
            AND c.is_identity = 0
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
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $insertQuery -EnableException

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
Get-ChildItem -Path $TempScriptPath -Filter *.sql | ForEach-Object {
    $file = $_.FullName

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

        # Suppression du fichier après exécution
        Remove-Item -Path $file -Force
    }
}

#############################################################################################
## CREATE/ALTER VIEWS
#############################################################################################

if ($DebugParam) {
    Write-Host "CREATE/ALTER VIEWS"
}

# Get views in the schema
$views = Get-DbaDbView -SqlInstance $SqlInstance -Database $SourceDB | Where-Object { $_.Schema -eq $SchemaName }

if (!$WhatIf) {
    $fileviews = $views | Export-DbaScript -Path $TempScriptPath

    
    foreach($file in $fileviews){     
        $content = Get-Content $file.FullName -Raw
        $content = $content -replace '(?i)\bCREATE\s+VIEW\b', 'CREATE OR ALTER VIEW'
        Set-Content -Path $file.FullName -Value $content
    }
    
}

if (!$WhatIf) {
    foreach($file in $fileviews){   
        $fullPath = $file.FullName

        $commandRaw = Get-Content $file -Raw
        $command = $commandRaw.Replace("'", "''")
        $command = $command -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

        
            try {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -File $fullPath -EnableException

                $logQuery="INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE/ALTER VIEWS', 'VIEW', NULL, '$fullPath', 'CREATE OR ALTER', '$($command.Replace("'", "''"))', 0, 'Success', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery          
            } catch {
                $errorMsg = $_.Exception.Message.Replace("'", "''")

                $logQuery="INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
                VALUES ($RestoreId, 'CREATE/ALTER VIEWS', 'VIEW', NULL, '$fullPath', 'CREATE OR ALTER', '$($command.Replace("'", "''"))', 1, '$errorMsg', GETDATE())"
                Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query 
                $ErrorCode = 1
            }
          

        
    }

    Get-ChildItem -Path $TempScriptPath -Filter *.sql | ForEach-Object {
    Remove-Item -Path $_.FullName -Force
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
    $procFiles = $storedProcedures | Export-DbaScript -Path $TempScriptPath

    foreach($file in $procFiles){
        $content = Get-Content $file.FullName -Raw
        $content = $content -replace '(?i)\bCREATE\s+PROCEDURE\b', 'CREATE OR ALTER PROCEDURE'
        Set-Content -Path $file.FullName -Value $content
    }

    
}

if (!$WhatIf) {
    foreach($file in $procFiles){ 
        $fullPath = $file.FullName

        $commandRaw = Get-Content $file -Raw
        $command = $commandRaw.Replace("'", "''")
        $command = $command -replace "`r`n", " " -replace "`n", " " -replace "`r", " "

        
        try {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -File $fullPath -EnableException

            $logQuery="INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'CREATE/ALTER PROCEDURES', 'PROCEDURE', NULL, '$fullPath', 'CREATE OR ALTER', '$($command.Replace("'", "''"))', 0, 'Success', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
                
        } 
        catch {
            $errorMsg = $_.Exception.Message.Replace("'", "''")

            $logQuery="INSERT INTO dbo.RestoreSchemaLogDetail (RestoreId, Step, ObjectType, ObjectSchema, ObjectName, Action, Command, ErrorCode, Message, LogDate)
            VALUES ($RestoreId, 'CREATE/ALTER PROCEDURES', 'PROCEDURE', NULL, '$fullPath', 'CREATE OR ALTER', '$($command.Replace("'", "''"))', 1, '$errorMsg', GETDATE())"
            Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery
            $ErrorCode = 1
        }    
    }

    Get-ChildItem -Path $TempScriptPath -Filter *.sql | ForEach-Object {
    Remove-Item -Path $_.FullName -Force
    }

}

#############################################################################################
## LOG TABLE
#############################################################################################

$RestoreEndDatetime = $now.ToString("yyyy-MM-dd HH:mm:ss")

$logQuery = "INSERT INTO dbo.RestoreSchemaLog (RestoreId,RestoreStartDatetime,RestoreEndDatetime,SourceDB,TargetDB,SchemaName,ErrorCode)
VALUES ($RestoreId,'$RestoreStartDatetime','$RestoreEndDatetime','$SourceDB','$TargetDB','$SchemaName',$ErrorCode)"

Invoke-DbaQuery -SqlInstance $LogInstance -Database $LogDatabase -Query $logQuery





















