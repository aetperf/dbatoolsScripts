
CREATE OR ALTER PROCEDURE [dbo].[CreateColumnStoreIndexFromParamTable] 
	@schemaNameParamTable sysname ='dbo',
	@tableNameParamTable sysname ='ParamIndexTable',
	@IndexNameParam nvarchar(100)='NCCI_I0',
	@execute BIT=1,
	@debug BIT=1,
	@force BIT=0
AS
BEGIN
	DECLARE
	@SchemaNameParam NVARCHAR(128), 
	@TableNameParam NVARCHAR(200), 
	@FileGroupNameParam NVARCHAR(100),
	@UpdateTimeParam DATETIME,
	@sqlcursor NVARCHAR(MAX),
	@sqlupdate NVARCHAR(MAX),
	@sqlindex NVARCHAR(MAX),
	@sqldrop NVARCHAR(MAX)


	SET @sqlcursor='SELECT [SchemaName],[TableName],[Updatetime],[FileGroupName] FROM '+QUOTENAME(@schemaNameParamTable)+'.'+QUOTENAME(@tableNameParamTable)+';';
	
	DECLARE @ResultTable TABLE (SchemaName NVARCHAR(128), TableName NVARCHAR(128), Updatetime DATETIME, FileGroupName NVARCHAR(100));
	
	INSERT INTO @ResultTable
	EXEC sp_executesql @sqlcursor;

	
	DECLARE TableCursor CURSOR FOR
	SELECT SchemaName, TableName, Updatetime, FileGroupName FROM @ResultTable;
	
	OPEN TableCursor

	FETCH NEXT FROM TableCursor INTO @SchemaNameParam, @TableNameParam,@UpdateTimeParam, @FileGroupNameParam;

    WHILE @@FETCH_STATUS = 0
    BEGIN
		SET @sqldrop='DROP INDEX IF EXISTS '+QUOTENAME(@IndexNameParam)+' ON '+QUOTENAME(@SchemaNameParam)+'.'+QUOTENAME(@TableNameParam)+';';


		--delete index if it already exists
		IF @debug=1
			BEGIN
				IF @force=1
					BEGIN
						RAISERROR('Drop index %s on [%s].[%s]', 1, 1, @IndexNameParam, @SchemaNameParam,@TableNameParam) WITH NOWAIT;
						RAISERROR('SQL Statement : %s', 1, 1, @sqldrop) WITH NOWAIT;
					END
			END
		IF @execute=1
			BEGIN
				IF @force=1
					BEGIN
						EXEC sp_executesql @sqldrop
					END
			END
		
		--Create index and update param table
		SELECT 
        @sqlindex = 'CREATE NONCLUSTERED COLUMNSTORE INDEX '+QUOTENAME(@IndexNameParam)+' ON ' 
                + QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME) + ' (' 
                + STRING_AGG(QUOTENAME(c.COLUMN_NAME), ', ') WITHIN GROUP (ORDER BY c.ORDINAL_POSITION)  
                + ')' 
		FROM 
			INFORMATION_SCHEMA.COLUMNS c
		INNER JOIN 
			INFORMATION_SCHEMA.TABLES t 
			ON c.TABLE_NAME = t.TABLE_NAME
		WHERE 
			t.TABLE_TYPE = 'BASE TABLE'
			AND c.TABLE_SCHEMA = @SchemaNameParam
			AND c.TABLE_NAME = @TableNameParam
			AND c.DATA_TYPE NOT IN ('image','ntext','text')
			AND (c.CHARACTER_MAXIMUM_LENGTH != -1 OR c.CHARACTER_MAXIMUM_LENGTH IS NULL)
		GROUP BY 
			c.TABLE_SCHEMA, c.TABLE_NAME

		IF @FileGroupNameParam IS NOT NULL
			BEGIN
				SET @sqlindex = @sqlindex + ' ON ' + QUOTENAME(@FileGroupNameParam)+';';
			END


		IF @debug=1
			BEGIN
						RAISERROR('Create index %s on [%s].[%s]', 1, 1, @IndexNameParam, @SchemaNameParam,@TableNameParam) WITH NOWAIT;
						RAISERROR('SQL Statement : %s', 1, 1, @sqlindex) WITH NOWAIT;
			END
		IF @execute=1
			BEGIN
						EXEC sp_executesql @sqlindex
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
		
	FETCH NEXT FROM TableCursor INTO @SchemaNameParam, @TableNameParam,@UpdateTimeParam,@FileGroupNameParam;
    END

    CLOSE TableCursor;
    DEALLOCATE TableCursor;
END;