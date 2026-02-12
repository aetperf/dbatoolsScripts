CREATE OR ALTER PROCEDURE dbo.DropAutoCreatedStatistics
(
    @Execute           BIT = 0,               -- 0 = Dry run, 1 = Execute
    @IncludeDatabases  NVARCHAR(MAX) = NULL,  -- Comma-separated LIKE patterns
    @IncludeTables     NVARCHAR(MAX) = NULL,  -- Comma-separated LIKE patterns (table name only)
    @ExcludeTables     NVARCHAR(MAX) = NULL   -- Comma-separated LIKE patterns (table name only)
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @DBName SYSNAME,
        @Msg    NVARCHAR(4000);

    ---------------------------------------------------------------------
    -- Normalize database include filter
    ---------------------------------------------------------------------
    DECLARE @IncludeDB TABLE
    (
        Pattern NVARCHAR(255) COLLATE DATABASE_DEFAULT
    );

    IF NULLIF(LTRIM(RTRIM(@IncludeDatabases)), '') IS NOT NULL
    BEGIN
        INSERT INTO @IncludeDB (Pattern)
        SELECT LTRIM(RTRIM(value)) COLLATE DATABASE_DEFAULT
        FROM STRING_SPLIT(@IncludeDatabases, ',');
    END

    ---------------------------------------------------------------------
    -- Build database execution list
    ---------------------------------------------------------------------
    DECLARE @Databases TABLE
    (
        RowNum INT IDENTITY(1,1) PRIMARY KEY,
        DBName SYSNAME
    );

    INSERT INTO @Databases (DBName)
    SELECT d.name
    FROM sys.databases d
    WHERE
        d.state_desc = 'ONLINE'
        AND d.database_id > 4  -- exclude system DBs
        AND
        (
            NOT EXISTS (SELECT 1 FROM @IncludeDB)
            OR EXISTS
            (
                SELECT 1
                FROM @IncludeDB i
                WHERE d.name COLLATE DATABASE_DEFAULT
                      LIKE i.Pattern COLLATE DATABASE_DEFAULT
            )
        );

    DECLARE
        @DBRow INT = 1,
        @DBMax INT;

    SELECT @DBMax = MAX(RowNum) FROM @Databases;

    IF @DBMax IS NULL
    BEGIN
        RAISERROR('No databases matched @IncludeDatabases filter.', 10, 1) WITH NOWAIT;
        RETURN;
    END

    SET @Msg = CASE WHEN @Execute = 1
             THEN 'EXECUTION MODE: Auto-created statistics will be DROPPED.'
             ELSE 'DRY RUN MODE: No statistics will be dropped.'
        END;

    RAISERROR(@Msg,10, 1) WITH NOWAIT;

    ---------------------------------------------------------------------
    -- Database loop
    ---------------------------------------------------------------------
    WHILE @DBRow <= @DBMax
    BEGIN
        SELECT @DBName = DBName
        FROM @Databases
        WHERE RowNum = @DBRow;

        SET @Msg = CONCAT('=== Processing database [', @DBName, '] ===');
        RAISERROR(@Msg, 10, 1) WITH NOWAIT;

        -----------------------------------------------------------------
        -- Build and run per-database batch:
        --  - gather stats using 3-part sys views
        --  - drop stats by switching context with USE
        -----------------------------------------------------------------
        DECLARE @Batch NVARCHAR(MAX);

        SET @Batch = N'
SET NOCOUNT ON;

DECLARE
    @SchemaName SYSNAME,
    @TableName  SYSNAME,
    @StatName   SYSNAME,
    @DropSQL    NVARCHAR(MAX),
    @Msg        NVARCHAR(4000);

DECLARE @Include TABLE (Pattern NVARCHAR(255) COLLATE DATABASE_DEFAULT);
DECLARE @Exclude TABLE (Pattern NVARCHAR(255) COLLATE DATABASE_DEFAULT);

IF NULLIF(LTRIM(RTRIM(@IncludeTables)), '''') IS NOT NULL
BEGIN
    INSERT INTO @Include
    SELECT LTRIM(RTRIM(value)) COLLATE DATABASE_DEFAULT
    FROM STRING_SPLIT(@IncludeTables, '','');
END

IF NULLIF(LTRIM(RTRIM(@ExcludeTables)), '''') IS NOT NULL
BEGIN
    INSERT INTO @Exclude
    SELECT LTRIM(RTRIM(value)) COLLATE DATABASE_DEFAULT
    FROM STRING_SPLIT(@ExcludeTables, '','');
END

DECLARE @Stats TABLE
(
    RowNum     INT IDENTITY(1,1),
    SchemaName SYSNAME,
    TableName  SYSNAME,
    StatName   SYSNAME
);

INSERT INTO @Stats (SchemaName, TableName, StatName)
SELECT
    sch.name,
    tbl.name,
    st.name
FROM ' + QUOTENAME(@DBName) + N'.sys.stats st
JOIN ' + QUOTENAME(@DBName) + N'.sys.tables tbl
    ON st.object_id = tbl.object_id
JOIN ' + QUOTENAME(@DBName) + N'.sys.schemas sch
    ON tbl.schema_id = sch.schema_id
WHERE
    tbl.is_ms_shipped = 0
    AND st.auto_created = 1
    AND st.name LIKE ''_WA_Sys%''
    AND
    (
        NOT EXISTS (SELECT 1 FROM @Include)
        OR EXISTS
        (
            SELECT 1
            FROM @Include i
            WHERE tbl.name COLLATE DATABASE_DEFAULT
                  LIKE i.Pattern COLLATE DATABASE_DEFAULT
        )
    )
    AND
    (
        NOT EXISTS
        (
            SELECT 1
            FROM @Exclude e
            WHERE tbl.name COLLATE DATABASE_DEFAULT
                  LIKE e.Pattern COLLATE DATABASE_DEFAULT
        )
    );

DECLARE
    @Row INT = 1,
    @Max INT;

SELECT @Max = MAX(RowNum) FROM @Stats;

IF @Max IS NULL
BEGIN
    RAISERROR(''No matching statistics found.'', 10, 1) WITH NOWAIT;
    RETURN;
END

WHILE @Row <= @Max
BEGIN
    SELECT
        @SchemaName = SchemaName,
        @TableName  = TableName,
        @StatName   = StatName
    FROM @Stats
    WHERE RowNum = @Row;

    -- DROP STATISTICS requires execution in the target DB context (no 3-part DB qualifier allowed)
    SET @DropSQL =
        N''USE ' + QUOTENAME(@DBName) + N'; ''
        + N''DROP STATISTICS ''
        + QUOTENAME(@SchemaName) + N''.''
        + QUOTENAME(@TableName)  + N''.''
        + QUOTENAME(@StatName)   + N'';''

    RAISERROR(@DropSQL, 10, 1) WITH NOWAIT;

    IF @Execute = 1
    BEGIN
        BEGIN TRY
            EXEC sys.sp_executesql @DropSQL;
        END TRY
        BEGIN CATCH
            SET @Msg = CONCAT(
                ''ERROR dropping statistics '',
                ' + QUOTENAME(@DBName, '''') + N', ''.'',
                @SchemaName, ''.'', @TableName, ''.'', @StatName,
                '' : '', ERROR_MESSAGE()
            );
            RAISERROR(@Msg, 11, 1) WITH NOWAIT;
        END CATCH
    END

    SET @Row += 1;
END
';

        EXEC sys.sp_executesql
            @Batch,
            N'@Execute BIT, @IncludeTables NVARCHAR(MAX), @ExcludeTables NVARCHAR(MAX)',
            @Execute        = @Execute,
            @IncludeTables  = @IncludeTables,
            @ExcludeTables  = @ExcludeTables;

        SET @DBRow += 1;
    END

    RAISERROR('=== Completed processing all databases ===', 10, 1) WITH NOWAIT;
END
GO
