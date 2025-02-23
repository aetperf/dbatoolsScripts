USE [AdventureWorksDW2022]
GO
/****** Object:  StoredProcedure [dba].[CheckIndexes]    Script Date: 11/02/2025 13:41:47 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
--DECLARE @RowCount int; EXECUTE [dba].[CheckIndexes] @DiffIndexRowCount=@RowCount; Select @RowCount

ALTER PROCEDURE [dba].[CheckIndexes] 
    @SchemaParam SYSNAME = 'dba',
    @TableParam SYSNAME = 'IndexLandscape',
    @DiffIndexRowCount INT OUTPUT
	
AS
BEGIN
    SELECT @DiffIndexRowCount = COUNT(*)
    FROM (
        SELECT SchemaName, TableName, IndexName, IndexedColumns FROM [dba].[IndexLandscape] 
        EXCEPT
        SELECT 
            s.name AS SchemaName,  
            t.name AS TableName,
            i.name AS IndexName,
            STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.index_column_id) AS IndexedColumns
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.type > 0 -- Exclut les index "HEAP"
        GROUP BY s.name, t.name, i.name
    ) AS Diff;
	RETURN;
	
END;
