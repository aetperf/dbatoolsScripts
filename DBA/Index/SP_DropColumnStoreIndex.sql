

CREATE OR ALTER PROCEDURE [dbo].[DropColumnStoreIndex] 
	@schemaNameLike NVARCHAR(100)='%',
	@tableNameLike NVARCHAR(100)='%',
	@indexNameLike NVARCHAR(100)='%',
	@columnStoreIndexNC BIT = 1,
	@columnStoreIndexC BIT = 1,
	--@indexWithAllColumn BIT=0,
	@truncateTableBeforeDrop BIT=0,
	@execute BIT=0,
	@debug BIT=1
AS
BEGIN
	DECLARE
	@SchemaNameParam NVARCHAR(128), 
	@TableNameParam NVARCHAR(200), 
	@IndexNameParam NVARCHAR(200),
	@RowCount BIGINT,
	@sqldrop NVARCHAR(MAX),
	@sqltruncate NVARCHAR(MAX);

	DECLARE indexCursor CURSOR FOR
		SELECT 
			s.name AS Schema_Name,
			t.name AS Table_Name,
			i.name AS Index_Name,
			COUNT(ic.column_id) AS Column_Count
		FROM 
			sys.indexes i
		JOIN 
			sys.tables t ON i.object_id = t.object_id
		JOIN 
			sys.schemas s ON t.schema_id = s.schema_id
		JOIN 
			sys.index_columns ic ON i.index_id = ic.index_id AND i.object_id = ic.object_id
		JOIN 
			sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
		WHERE 
			s.name like @schemaNameLike
			AND t.name LIKE @tableNameLike
			AND (i.type = CASE 
				WHEN @columnStoreIndexNC=1 THEN 6
				END
			OR i.type = CASE 
				WHEN @columnStoreIndexC=1 THEN 5
				END)
			AND i.name like @indexNameLike
		GROUP BY 
			i.name, t.name, s.name
		--HAVING
		--	Column_Count = CASE WHEN @indexWithAllColumn=1 THEN (SELECT COUNT(*) AS Column_Count FROM INFORMATION_SCHEMA.COLUMNS c JOIN INFORMATION_SCHEMA.TABLES t ON c.TABLE_NAME = t.TABLE_NAME WHERE t. = @tableName AND (c.CHARACTER_MAXIMUM_LENGTH != -1 OR c.CHARACTER_MAXIMUM_LENGTH IS NULL) )
		ORDER BY 
			i.name;

		OPEN indexCursor;

		FETCH NEXT FROM indexCursor INTO @SchemaNameParam, @TableNameParam, @IndexNameParam, @RowCount;

		WHILE @@FETCH_STATUS = 0
			BEGIN
				SET @sqldrop='DROP INDEX IF EXISTS '+QUOTENAME(@IndexNameParam)+' ON '+QUOTENAME(@SchemaNameParam)+'.'+QUOTENAME(@TableNameParam)+';';
				SET @sqltruncate='TRUNCATE TABLE '+QUOTENAME(@SchemaNameParam)+'.'+QUOTENAME(@TableNameParam)+';';
				
				IF @debug=1
					BEGIN
					IF @truncateTableBeforeDrop=1
							BEGIN
								RAISERROR('Truncating table index [%s].[%s]', 1, 1, @SchemaNameParam,@TableNameParam) WITH NOWAIT;
								RAISERROR('SQL Statement : %s', 1, 1, @sqltruncate) WITH NOWAIT;
							END
						
							RAISERROR('Dropping index %s on [%s].[%s]', 1, 1, @IndexNameParam,@SchemaNameParam,@TableNameParam) WITH NOWAIT;
							RAISERROR('SQL Statement : %s', 1, 1, @sqldrop) WITH NOWAIT;
					END
				IF @execute=1
					BEGIN
						IF @truncateTableBeforeDrop=1
							BEGIN
								EXEC sp_execute @sqltruncate;
							END
						EXEC sp_execute @sqldrop;
					END
				FETCH NEXT FROM indexCursor INTO @SchemaNameParam, @TableNameParam, @IndexNameParam, @RowCount;
			END


		CLOSE indexCursor;
		DEALLOCATE indexCursor;
END;
	

