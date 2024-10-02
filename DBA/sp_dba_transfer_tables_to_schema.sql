--a procedure that will transfer all tables from one schema to another schema
CREATE OR ALTER PROCEDURE sp_dba_transfer_tables_to_schema
    @source_schema sysname,
    @destination_schema sysname
AS
BEGIN

-- Get all tables in the source schema
DECLARE @sql nvarchar(max);
DECLARE @table_name sysname;


DECLARE table_cursor CURSOR FOR
SELECT name
FROM sys.tables
WHERE schema_id = SCHEMA_ID( @source_schema );

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @table_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = 'ALTER SCHEMA ' + QUOTENAME(@destination_schema) + ' TRANSFER ' + QUOTENAME(@source_schema) + '.' + QUOTENAME(@table_name);
    EXEC sp_executesql @sql;

    FETCH NEXT FROM table_cursor INTO @table_name;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

END



