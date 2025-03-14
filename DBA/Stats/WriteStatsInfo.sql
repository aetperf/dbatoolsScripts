USE [DBATOOLS]
GO
/****** Object:  StoredProcedure [dbo].[GetStatisticsInfo]    Script Date: 05/02/2025 16:54:18 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[GetStatisticsInfo]
    @TargetDatabaseName NVARCHAR(128),
	@AuditDatabaseName NVARCHAR(128) = 'AdventureWorks2019',
	@AuditSchemaName NVARCHAR(128) = 'dbo',
	@AuditTableName NVARCHAR(128) = 'AuditStat'
AS
BEGIN
    DECLARE @SQLGetStats NVARCHAR(MAX);
	DECLARE @SQLCreateTable NVARCHAR(MAX);
	DECLARE @SQLTableExist NVARCHAR(MAX);
	DECLARE @TableExists INT;

	SET @SQLCreateTable= N'
	CREATE TABLE '+QUOTENAME(@AuditDatabaseName)+'.'+QUOTENAME(@AuditSchemaName)+'.'+QUOTENAME(@AuditTableName)+'(
	[Database_Name] [nvarchar](128) NULL,
	[Schema_Name] [nvarchar](128) NULL,
	[Table_Name] [sysname] NOT NULL,
	[Column_Name] [sysname] NULL,
	[Statistic_Name] [nvarchar](128) NULL,
	[Last_Updated] [datetime2](7) NULL,
	[Snapshot_Date] [datetime] NOT NULL
	) ON [PRIMARY];
	'

	DECLARE @SQL NVARCHAR(MAX);

    SET @SQLTableExist = N'SELECT @TableExists = COUNT(*) 
               FROM ' + QUOTENAME(@AuditDatabaseName) + '.sys.tables t
               JOIN ' + QUOTENAME(@AuditDatabaseName) + '.sys.schemas s ON t.schema_id = s.schema_id
               WHERE t.name = '''+@AuditTableName+''' 
               AND s.name = '''+@AuditSchemaName+''';'
	PRINT @SQLTableExist

    EXEC sp_executesql @SQLTableExist,N'@TableExists INT OUTPUT', @TableExists OUTPUT;
	PRINT @TableExists

	IF @TableExists=0 
		EXEC sp_executesql @SQLCreateTable;


    SET @SQLGetStats = N'
	INSERT INTO '+QUOTENAME(@AuditDatabaseName)+'.'+QUOTENAME(@AuditSchemaName)+'.'+QUOTENAME(@AuditTableName)+'
    SELECT 
		'''+@TargetDatabaseName+''' AS Database_Name,               
		s2.name AS Schema_Name,  
		t.name AS Table_Name,  
		c.name AS Column_Name,  
		s1.name AS Statistic_Name,                 
		st.last_updated AS Last_Updated,
		GETDATE() AS Snapshot_Date
	FROM 
		'+QUOTENAME(@TargetDatabaseName)+'.sys.stats s1
	JOIN 
		'+QUOTENAME(@TargetDatabaseName)+'.sys.tables t ON s1.object_id = t.object_id
	CROSS APPLY 
		'+QUOTENAME(@TargetDatabaseName)+'.sys.dm_db_stats_properties(t.object_id, s1.stats_id) st
	JOIN 
		'+QUOTENAME(@TargetDatabaseName)+'.sys.stats_columns sc ON s1.stats_id = sc.stats_id AND s1.object_id = sc.object_id 
	JOIN 
		'+QUOTENAME(@TargetDatabaseName)+'.sys.columns c ON sc.column_id = c.column_id AND sc.object_id = c.object_id
	JOIN 
		'+QUOTENAME(@TargetDatabaseName)+'.sys.schemas s2 ON t.schema_id = s2.schema_id  
	WHERE 
		t.is_ms_shipped = 0  
	ORDER BY 
		Database_Name, Schema_Name, Table_Name, Column_Name, Statistic_Name;

    ';
	
    EXEC sp_executesql @SQLGetStats;
END;

