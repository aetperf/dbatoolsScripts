
CREATE OR ALTER PROCEDURE [dbo].[DropColumnStoreIndexFromParamTable] 
	@schemaNameParamTable sysname ='dbo',
	@tableNameParamTable sysname ='ParamIndexTable',
	@IndexNameParam nvarchar(100)='NCCI_I0',
	@execute BIT=1,
	@debug BIT=1
	
AS
BEGIN
	DECLARE
	@SchemaNameParam NVARCHAR(128), 
	@TableNameParam NVARCHAR(200), 
	@UpdateTimeParam DATETIME,
	@sqlcursor NVARCHAR(MAX),
	@sqlupdate NVARCHAR(MAX),
	@sqldrop NVARCHAR(MAX)


	SET @sqlcursor='SELECT [SchemaName],[TableName],[Updatetime] FROM '+QUOTENAME(@schemaNameParamTable)+'.'+QUOTENAME(@tableNameParamTable)+' WHERE DropIndex = 1;';
	
	DECLARE @ResultTable TABLE (SchemaName NVARCHAR(128), TableName NVARCHAR(128), Updatetime DATETIME);
	
	INSERT INTO @ResultTable
	EXEC sp_executesql @sqlcursor;

	
	DECLARE TableCursor CURSOR FOR
	SELECT SchemaName, TableName, Updatetime FROM @ResultTable;
	
	OPEN TableCursor

	FETCH NEXT FROM TableCursor INTO @SchemaNameParam, @TableNameParam,@UpdateTimeParam;

    WHILE @@FETCH_STATUS = 0
    BEGIN
		SET @sqldrop='DROP INDEX IF EXISTS '+QUOTENAME(@IndexNameParam)+' ON '+QUOTENAME(@SchemaNameParam)+'.'+QUOTENAME(@TableNameParam)+';';


		--delete index if exists
		IF @debug=1
			BEGIN
						RAISERROR('Drop index %s on [%s].[%s]', 1, 1, @IndexNameParam, @SchemaNameParam,@TableNameParam) WITH NOWAIT;
						RAISERROR('SQL Statement : %s', 1, 1, @sqldrop) WITH NOWAIT;
			END
		IF @execute=1
			BEGIN
						EXEC sp_executesql @sqldrop
			END
		
		

		--Update time 
		SET @sqlupdate='UPDATE '+QUOTENAME(@schemaNameParamTable)+'.'+QUOTENAME(@tableNameParamTable)+' SET Updatetime=GETDATE() WHERE SchemaName='''+@SchemaNameParam+''' and TableName='''+@TableNameParam+''';';
		IF @debug=1
			BEGIN
						RAISERROR('Update Param table [%s].[%s]', 1, 1, @schemaNameParamTable, @tableNameParamTable) WITH NOWAIT;
						RAISERROR('SQL Statement : %s', 1, 1, @sqlupdate) WITH NOWAIT;
			END
		IF @execute=1
			BEGIN
						EXEC sp_executesql @sqlupdate
			END
		--Update time 
		
	FETCH NEXT FROM TableCursor INTO @SchemaNameParam, @TableNameParam,@UpdateTimeParam;
    END

    CLOSE TableCursor;
    DEALLOCATE TableCursor;
END;