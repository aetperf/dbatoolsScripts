
/****** Object:  StoredProcedure [dba].[CheckIndexes]    Script Date: 11/02/2025 16:29:30 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [dba].[CheckIndexesDetails] 
    @SchemaParam SYSNAME = 'dba',
    @TableParam SYSNAME = 'IndexLandscape',
    @SchemaTargetLike NVARCHAR(50) = '%' 
AS
BEGIN
    WITH IndexDifferences AS (
        SELECT 
            il.SchemaName COLLATE DATABASE_DEFAULT AS SchemaName, 
            il.TableName COLLATE DATABASE_DEFAULT AS TableName, 
            il.IndexName COLLATE DATABASE_DEFAULT AS IndexName, 
            il.IndexType COLLATE DATABASE_DEFAULT AS IndexType, 
            il.IndexedColumns COLLATE DATABASE_DEFAULT AS IndexedColumns, 
            il.IncludeColumns COLLATE DATABASE_DEFAULT AS IncludeColumns, 
            il.FilterCondition COLLATE DATABASE_DEFAULT AS FilterCondition
        FROM [dba].[IndexLandscape] il
        WHERE il.SchemaName LIKE @SchemaTargetLike

        EXCEPT

        SELECT 
            s.name COLLATE DATABASE_DEFAULT AS SchemaName,  
            t.name COLLATE DATABASE_DEFAULT AS TableName,
            i.name COLLATE DATABASE_DEFAULT AS IndexName,
            i.type_desc COLLATE DATABASE_DEFAULT AS IndexType,
            STRING_AGG(
                CASE 
                    WHEN i.type_desc LIKE '%COLUMNSTORE%' THEN c.name
                    WHEN ic.is_included_column = 0 THEN c.name 
                END, ', ') 
                WITHIN GROUP (ORDER BY ic.index_column_id) 
                COLLATE DATABASE_DEFAULT AS IndexedColumns,
            STRING_AGG(
                CASE 
                    WHEN i.type_desc LIKE '%COLUMNSTORE%' THEN NULL
                    WHEN ic.is_included_column = 1 THEN c.name 
                END, ', ') 
                WITHIN GROUP (ORDER BY ic.index_column_id) 
                COLLATE DATABASE_DEFAULT AS IncludeColumns,
            i.filter_definition COLLATE DATABASE_DEFAULT AS FilterCondition
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id  
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.type > 0  -- Exclut les index HEAP (tables sans index clusterisé)
        AND s.name LIKE @SchemaTargetLike
        GROUP BY s.name, t.name, i.name, i.type_desc, i.filter_definition
    )

    --Vérifier si l'index existe dans les vues systèmes et comparer les colonnes
    SELECT 
        id.SchemaName,
        id.TableName,
        id.IndexName,
        id.IndexType,
        id.IndexedColumns,
        id.IncludeColumns,
        id.FilterCondition,
        CASE 
            WHEN sys.IndexName IS NULL THEN 'Suppression'	
            ELSE 
                CASE 
                    WHEN id.IndexedColumns != sys.IndexedColumns THEN 'IndexedColumns modifié'
                    WHEN id.IncludeColumns != sys.IncludeColumns THEN 'IncludeColumns modifié'
                    WHEN id.FilterCondition != sys.FilterCondition THEN 'FilterCondition modifié'
                    ELSE 'Aucune différence'
                END
        END AS DiffDetails
    FROM IndexDifferences id
    LEFT JOIN (
        SELECT 
            s.name COLLATE DATABASE_DEFAULT AS SchemaName,  
            t.name COLLATE DATABASE_DEFAULT AS TableName,
            i.name COLLATE DATABASE_DEFAULT AS IndexName,
            STRING_AGG(
                CASE 
                    WHEN i.type_desc LIKE '%COLUMNSTORE%' THEN c.name
                    WHEN ic.is_included_column = 0 THEN c.name 
                END, ', ') 
                WITHIN GROUP (ORDER BY ic.index_column_id) 
                COLLATE DATABASE_DEFAULT AS IndexedColumns,
            STRING_AGG(
                CASE 
                    WHEN i.type_desc LIKE '%COLUMNSTORE%' THEN NULL
                    WHEN ic.is_included_column = 1 THEN c.name 
                END, ', ') 
                WITHIN GROUP (ORDER BY ic.index_column_id) 
                COLLATE DATABASE_DEFAULT AS IncludeColumns,
            i.filter_definition COLLATE DATABASE_DEFAULT AS FilterCondition
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id  
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.type > 0 -- Exclut les index HEAP
        GROUP BY s.name, t.name, i.name, i.filter_definition
    ) sys
    ON id.SchemaName = sys.SchemaName 
    AND id.TableName = sys.TableName 
    AND id.IndexName = sys.IndexName
END;

--EXECUTE [dba].[CheckIndexesDetails] @SchemaTargetLike='dbo'