CREATE OR ALTER PROCEDURE [dbo].[sp_get_tables_sizes_all_dbs]
    @IncludeDatabase NVARCHAR(MAX) = NULL,     -- ex: 'DB_%,DWH_%'
    @ExcludeDatabase NVARCHAR(MAX) = NULL      -- ex: 'temp%,master'
AS
BEGIN
    SET NOCOUNT ON;


    ---------------------------------------------------------------
    -- Clear temp table if exists
    ---------------------------------------------------------------
    IF OBJECT_ID('tempdb..##TABLESIZES_ALLDB') IS NOT NULL
        DROP TABLE ##TABLESIZES_ALLDB;

    CREATE TABLE ##TABLESIZES_ALLDB (
        snapdate datetime,
        srv nvarchar(1000),
        sv nvarchar(1000),
        dbname sysname,
        SchemaName sysname,
        TableName sysname,
        partition_id bigint,
        partition_number int,
        lignes bigint,
        [memory (kB)] bigint,
        [data (kB)] bigint,
        [indexes (kb)] bigint,
        data_compression int,
        data_compression_desc nvarchar(1000)
    );

    ---------------------------------------------------------------
    -- Build database list with filters
    ---------------------------------------------------------------
    DECLARE @DBs TABLE (dbname sysname);

    ;WITH Split AS (
        SELECT 
            LTRIM(RTRIM(value)) AS pattern
        FROM string_split(ISNULL(@IncludeDatabase,''), ',')
        WHERE value <> ''
    ),
    SplitEx AS (
        SELECT 
            LTRIM(RTRIM(value)) AS pattern
        FROM string_split(ISNULL(@ExcludeDatabase,''), ',')
        WHERE value <> ''
    )
    INSERT INTO @DBs
    SELECT name
    FROM sys.databases
    WHERE name NOT IN ('tempdb')  -- exclusion d'origine
      AND (
            NOT EXISTS (SELECT 1 FROM Split)  -- pas de include => tout est autorisé
            OR EXISTS (SELECT 1 FROM Split s WHERE name LIKE s.pattern)
          )
      AND NOT EXISTS (SELECT 1 FROM SplitEx e WHERE name LIKE e.pattern);

    ---------------------------------------------------------------
    -- Loop on filtered DB list
    ---------------------------------------------------------------
    DECLARE @db sysname, @sql NVARCHAR(MAX);
    DECLARE db_cursor CURSOR FOR SELECT dbname FROM @DBs;
    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @db;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = N'
        USE [' + @db + N'];

        INSERT INTO ##TABLESIZES_ALLDB
        SELECT 
            GETDATE(),
            CAST(SERVERPROPERTY(''MachineName'') AS NVARCHAR(1000)),
            CAST(@@SERVICENAME AS NVARCHAR(1000)),
            ''' + @db + N''',
            OBJECT_SCHEMA_NAME(p.object_id),
            OBJECT_NAME(p.object_id),
            p.partition_id,
            p.partition_number,
            SUM(CASE WHEN (p.index_id < 2 AND a.type = 1) THEN p.rows ELSE 0 END),
            SUM(a.total_pages) * 8,
            SUM(CASE WHEN a.type <> 1 THEN a.used_pages
                     WHEN p.index_id < 2 THEN a.data_pages ELSE 0 END) * 8,
            (SUM(a.used_pages) -
                SUM(CASE WHEN a.type <> 1 THEN a.used_pages
                         WHEN p.index_id < 2 THEN a.data_pages ELSE 0 END)
            ) * 8,
            p.data_compression,
            p.data_compression_desc
        FROM sys.partitions p
        JOIN sys.allocation_units a ON p.partition_id = a.container_id
        JOIN sys.objects s ON p.object_id = s.object_id
        WHERE s.type = ''U''
        GROUP BY 
            p.object_id, p.partition_id, p.partition_number,
            p.data_compression, p.data_compression_desc;
        ';

        EXEC sys.sp_executesql @sql;

        FETCH NEXT FROM db_cursor INTO @db;
    END

    CLOSE db_cursor;
    DEALLOCATE db_cursor;

    ---------------------------------------------------------------
    -- Final output
    ---------------------------------------------------------------
    SELECT * FROM ##TABLESIZES_ALLDB;
END;
GO
