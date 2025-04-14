CREATE OR ALTER PROCEDURE [dbo].[DropColumnStoreIndex] 
	@SchemaNameLike NVARCHAR(100)='%',
	@TableNameLike NVARCHAR(100)='%',
	@IndexNameLike NVARCHAR(100)='%',
	@ColumnStoreIndexNC BIT = 1,
	@ColumnStoreIndexC BIT = 1,
	--@indexWithAllColumn BIT=0,
	@TruncateTableBeforeDrop BIT=0,
	@Execute BIT=0,
	@Debug BIT=1
AS
BEGIN
	DECLARE
	@SchemaNameParam NVARCHAR(128), 
	@TableNameParam NVARCHAR(200), 
	@IndexNameParam NVARCHAR(200),
	@RowCount BIGINT,
	@Sqldrop NVARCHAR(MAX),
	@Sqltruncate NVARCHAR(MAX);

	CREATE TABLE #TableCursor(
		Schema_Name sysname,
		Table_Name sysname,
		Index_Name sysname,
		Column_Count INT,
	)

	INSERT INTO #TableCursor(Schema_Name,Table_Name,Index_Name,Column_Count)
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
			s.name like @SchemaNameLike
			AND t.name LIKE @TableNameLike
			AND (i.type = CASE 
				WHEN @ColumnStoreIndexNC=1 THEN 6
				END
			OR i.type = CASE 
				WHEN @ColumnStoreIndexC=1 THEN 5
				END)
			AND i.name like @IndexNameLike
		GROUP BY 
			i.name, t.name, s.name
		--HAVING
		--	Column_Count = CASE WHEN @indexWithAllColumn=1 THEN (SELECT COUNT(*) AS Column_Count FROM INFORMATION_SCHEMA.COLUMNS c JOIN INFORMATION_SCHEMA.TABLES t ON c.TABLE_NAME = t.TABLE_NAME WHERE t. = @tableName AND (c.CHARACTER_MAXIMUM_LENGTH != -1 OR c.CHARACTER_MAXIMUM_LENGTH IS NULL) )
		ORDER BY 
			i.name;

		DECLARE indexCursor CURSOR FOR
		SELECT Schema_Name,Table_Name,Index_Name,Column_Count
		FROM #TableCursor;

		OPEN indexCursor;

		FETCH NEXT FROM indexCursor INTO @SchemaNameParam, @TableNameParam, @IndexNameParam, @RowCount;

		WHILE @@FETCH_STATUS = 0
			BEGIN
				SET @Sqldrop='DROP INDEX IF EXISTS '+QUOTENAME(@IndexNameParam)+' ON '+QUOTENAME(@SchemaNameParam)+'.'+QUOTENAME(@TableNameParam)+';';
				SET @Sqltruncate='TRUNCATE TABLE '+QUOTENAME(@SchemaNameParam)+'.'+QUOTENAME(@TableNameParam)+';';
				
				IF @Debug=1
					BEGIN
					IF @TruncateTableBeforeDrop=1
							BEGIN
								RAISERROR('Truncating table index [%s].[%s]', 1, 1, @SchemaNameParam,@TableNameParam) WITH NOWAIT;
								RAISERROR('SQL Statement : %s', 1, 1, @Sqltruncate) WITH NOWAIT;
							END
						
							RAISERROR('Dropping index %s on [%s].[%s]', 1, 1, @IndexNameParam,@SchemaNameParam,@TableNameParam) WITH NOWAIT;
							RAISERROR('SQL Statement : %s', 1, 1, @Sqldrop) WITH NOWAIT;
					END
				IF @Execute=1
					BEGIN
						IF @TruncateTableBeforeDrop=1
							BEGIN
								EXEC sp_executesql @Sqltruncate;
							END
						EXEC sp_executesql @Sqldrop;
					END
				FETCH NEXT FROM indexCursor INTO @SchemaNameParam, @TableNameParam, @IndexNameParam, @RowCount;
			END


		CLOSE indexCursor;
		DEALLOCATE indexCursor;
		DROP TABLE #TableCursor
END;