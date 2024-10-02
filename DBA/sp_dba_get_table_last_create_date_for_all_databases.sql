-- A store procedure that will iterate over all user databases and return the last create date of the tables in the database.

CREATE OR ALTER PROCEDURE sp_dba_get_table_last_create_date_for_all_databases
AS
BEGIN
    DECLARE @dbname sysname;
    DECLARE @sql nvarchar(max);
    DECLARE @table_name sysname;
    DECLARE @create_date datetime2;
    DECLARE @sql_create_date nvarchar(max);
    DECLARE @sql_db nvarchar(max);
	SET NOCOUNT ON;
    CREATE TABLE #table_create_date
    (
        dbname sysname,
        last_create_date datetime2,
        number_of_tables int
    );

    DECLARE db_cursor CURSOR FOR
    SELECT name
    FROM sys.databases
    WHERE state_desc = 'ONLINE'
          AND name NOT IN ('master', 'tempdb', 'model', 'msdb');

    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @dbname;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql_db = QUOTENAME(@dbname) + '.sys.tables';
        SET @sql = 'SELECT '''+@dbname+''',max(create_date), count(*) FROM ' + @sql_db;
        

        INSERT INTO #table_create_date
        EXEC sp_executesql @sql;

        FETCH NEXT FROM db_cursor INTO @dbname;
    END

    CLOSE db_cursor;
    DEALLOCATE db_cursor;

    SELECT *
    FROM #table_create_date
    ORDER BY dbname

    DROP TABLE #table_create_date;
END
