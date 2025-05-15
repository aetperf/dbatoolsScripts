# .\restoreSchema.ps1 -SqlInstance "LAPTOP-R6ED0C8E\DBA01" -SourceDB "AD2019" -TargetDB "AdventureWorks2019" -SchemaName "HumanResources" -TempScriptPath "D:\pacollet\Client\MinistereJustice\RestoreSchema\script"

param 
(
    [Parameter(Mandatory)] [string] $SqlInstance,
    [Parameter(Mandatory)] [string] $SourceDB,
    [Parameter(Mandatory)] [string] $TargetDB,
    [Parameter(Mandatory)] [string] $SchemaName,
    [Parameter(Mandatory)] [string] $TempScriptPath,
    [Parameter()] [switch] $WhatIf
)

$DebugParam = $True

# Create folder if doesn't exist
if (!(Test-Path $TempScriptPath)) {
    New-Item -ItemType Directory -Path $TempScriptPath
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
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $dropFkQuery
        }
    }
}

# Drop Table which are not in the source
foreach ($table in $tablesToDelete) {
    $fullTable = "[$SchemaName].[{0}]" -f $table.TABLE_NAME
    $dropTableQuery = "DROP TABLE $fullTable"
    if($DebugParam){
        Write-Host "DROP TABLE $fullTable"
    }
    if(!$WhatIf){
        Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $dropTableQuery
    }
}

#############################################################################################
## DELETE ORPHAN VIEWS
#############################################################################################

# Get Views which are not in the source 
$viewsToDelete = $targetViews | Where-Object { $_.TABLE_NAME -notin $sourceViews.TABLE_NAME }

# Drop View which are not in the source
foreach ($view in $viewsToDelete) {
    $fullView = "[$SchemaName].[{0}]" -f $view.TABLE_NAME
    if($DebugParam){
        Write-Host "DROP VIEW $fullView"
    }
    if(!$WhatIf){
        Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query "DROP VIEW $fullView"
    }
}

#############################################################################################
## DELETE ORPHAN STORED PROCEDURE
#############################################################################################

# Get Stored Procedure which are not in the source 
$procsToDelete = $targetProcedures | Where-Object { $_.ROUTINE_NAME -notin $sourceProcedures.ROUTINE_NAME }

# Drop Procedure which are not in the source
foreach ($proc in $procsToDelete) {
    $fullProc = "[$SchemaName].[{0}]" -f $proc.ROUTINE_NAME
    if($DebugParam){
        Write-Host "DROP PROCEDURE $fullProc"
    }
    if(!$WhatIf){
        Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query "DROP PROCEDURE $fullProc"
    }
}


#############################################################################################
## DISABLED FOREIGN KEY TO TRUNCATE INSERT DATA
#############################################################################################

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
        Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query $query
    }
}

#############################################################################################
## TRUNCATE INSERT DATA
#############################################################################################

# truncate insert data for each table
foreach ($table in $sourceTables) {
    $tableName = $table.TABLE_NAME
    $qualifiedName = "[$SchemaName].[$tableName]"

    # Truncate table
    if($DebugParam){
        Write-Host "TRUNCATE TABLE $qualifiedName"
    }
    if(!$WhatIf){
        Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -Query "IF OBJECT_ID('$qualifiedName') IS NOT NULL TRUNCATE TABLE $qualifiedName"
    }
    
    # copy data
    if($DebugParam){
        Write-Host "COPY TABLE $qualifiedName"
    }
    if(!$WhatIf){
        $res = Copy-DbaDbTableData -SqlInstance $SqlInstance -Database $SourceDB -Destination $SqlInstance -DestinationDatabase $TargetDB -Table "$SchemaName.$tableName" -AutoCreateTable:$false
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
    if(!$WhatIf){
        Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -File $file
        Remove-Item -Path $file -Force
    }
}



#############################################################################################
## CREATE/ALTER PROCEDURE AND VIEW 
#############################################################################################

# Get Object views in the schema
$views = Get-DbaDbView -SqlInstance $SqlInstance -Database $SourceDB | Where-Object { $_.Schema -eq $SchemaName }

# Get Object procedure in the schema
$storedProcedure =  Get-DbaDbStoredProcedure -SqlInstance $SqlInstance -Database $SourceDB | Where-Object { $_.Schema -eq $SchemaName }

# Export view and procedure script
if(!$WhatIf){
    $viewsFile = $views | Export-DbaScript -Path $TempScriptPath
    $storedProcedureFile = $storedProcedure | Export-DbaScript -Path $TempScriptPath


    $ExportFile = $viewsFile+$storedProcedureFile

    # Modify the script to replace CREATE by CREATE OR REPLACE
    Get-ChildItem $ExportFile | ForEach-Object {
        $file = $_.FullName
        $content = Get-Content $file -Raw

        $content = $content -replace '(?i)\bCREATE\s+PROCEDURE\b', 'CREATE OR ALTER PROCEDURE'
        $content = $content -replace '(?i)\bCREATE\s+VIEW\b', 'CREATE OR ALTER VIEW'

        Set-Content -Path $file -Value $content
    }
}

# Create or alter view and procedure
if($DebugParam){
        Write-Host "CREATE/ALTER VIEW AND PROCEDURE"
}
Get-ChildItem -Path $TempScriptPath -Filter *.sql | ForEach-Object {
    $file = $_.FullName
    if(!$WhatIf){
        Invoke-DbaQuery -SqlInstance $SqlInstance -Database $TargetDB -File $file
        Remove-Item -Path $file -Force
    }
    
}























