CREATE OR ALTER PROCEDURE dbo.dba_CollectStatsHistory
(
      @Databases      nvarchar(max) = N'USER_DATABASES'
    , @Tables         nvarchar(max) = N'%'
    , @Stats          nvarchar(max) = N'%'
    , @RetentionDays  int           = NULL
    , @GetDetails     char(1)       = 'N'
    , @Execute        char(1)       = 'Y'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -------------------------------------------------------------------------
    -- Variables
    -------------------------------------------------------------------------
    DECLARE
          @CollectionStartTime       datetime2(3) = SYSDATETIME()
        , @CollectionStartTimeText   varchar(30)
        , @CurrentDatabase           sysname
        , @SQL                       nvarchar(max)
        , @Msg                       nvarchar(max)
        , @ProcDatabase              sysname
        , @RetentionDaysText         varchar(20)
        , @MetadataRows              bigint
        , @DetailsRows               bigint
        , @RowsDeleted               int;

    SET @CollectionStartTimeText = CONVERT(varchar(30), @CollectionStartTime, 121);
    SET @ProcDatabase = DB_NAME();
    SET @RetentionDaysText = COALESCE(CONVERT(varchar(20), @RetentionDays), '<NULL>');

    -------------------------------------------------------------------------
    -- Validation
    -------------------------------------------------------------------------
    IF @Execute NOT IN ('Y', 'N')
    BEGIN
        RAISERROR('@Execute must be Y or N.', 16, 1);
        RETURN;
    END;

    IF @GetDetails NOT IN ('Y', 'N')
    BEGIN
        RAISERROR('@GetDetails must be Y or N.', 16, 1);
        RETURN;
    END;

    IF @RetentionDays IS NOT NULL AND @RetentionDays < 0
    BEGIN
        RAISERROR('@RetentionDays must be NULL or >= 0.', 16, 1);
        RETURN;
    END;

    SET @Msg = N'Starting dbo.dba_CollectStatsHistory at '
             + CONVERT(nvarchar(30), @CollectionStartTimeText)
             + N' in database [' + @ProcDatabase + N'].';

    RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

    SET @Msg = N'Parameters: @Databases=' + COALESCE(@Databases, N'<NULL>')
             + N', @Tables=' + COALESCE(@Tables, N'<NULL>')
             + N', @Stats=' + COALESCE(@Stats, N'<NULL>')
             + N', @RetentionDays=' + CONVERT(nvarchar(20), @RetentionDaysText)
             + N', @GetDetails=' + @GetDetails
             + N', @Execute=' + @Execute;

    RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

    -------------------------------------------------------------------------
    -- Create history tables only in execution mode
    -------------------------------------------------------------------------
    IF @Execute = 'Y'
    BEGIN
        IF OBJECT_ID(N'dbo.StatsHistory', N'U') IS NULL
        BEGIN
            SET @Msg = N'Creating table dbo.StatsHistory.';
            RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

            CREATE TABLE dbo.StatsHistory
            (
                  StatsHistoryId             bigint IDENTITY(1,1) NOT NULL
                    CONSTRAINT PK_StatsHistory PRIMARY KEY CLUSTERED

                , CollectionStartTime        datetime2(3) NOT NULL
                , CollectionTime             datetime2(3) NOT NULL

                , DatabaseName               sysname NOT NULL
                , SchemaName                 sysname NOT NULL
                , TableName                  sysname NOT NULL
                , ObjectId                   int NOT NULL
                , StatsId                    int NOT NULL
                , StatsName                  sysname NOT NULL

                , IndexId                    int NULL
                , IndexName                  sysname NULL
                , IndexTypeDesc              nvarchar(60) NULL
                , IsPrimaryKey               bit NULL
                , IsUnique                   bit NULL
                , IsUniqueConstraint         bit NULL

                , AutoCreated                bit NOT NULL
                , UserCreated                bit NOT NULL
                , NoRecompute                bit NOT NULL
                , HasFilter                  bit NOT NULL
                , FilterDefinition           nvarchar(max) NULL
                , IsIncremental              bit NOT NULL
                , IsTemporary                bit NOT NULL
                , HasPersistedSample         bit NOT NULL

                , StatsColumns               nvarchar(max) NULL
                , StatsColumnsCount          int NOT NULL

                , LastUpdated                datetime2(3) NULL
                , Rows                       bigint NULL
                , RowsSampled                bigint NULL
                , SamplePercent              decimal(9,4) NULL
                , Steps                      int NULL
                , UnfilteredRows             bigint NULL
                , ModificationCounter        bigint NULL
                , PersistedSamplePercent     decimal(9,4) NULL
            )
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX IX_StatsHistory_CollectionStartTime
                ON dbo.StatsHistory(CollectionStartTime)
                WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX IX_StatsHistory_ObjectStats
                ON dbo.StatsHistory(DatabaseName, SchemaName, TableName, StatsName, CollectionStartTime)
                INCLUDE
                (
                      LastUpdated
                    , Rows
                    , RowsSampled
                    , ModificationCounter
                    , Steps
                    , SamplePercent
                )
                WITH (DATA_COMPRESSION = PAGE);
        END;

        IF OBJECT_ID(N'dbo.StatsHistoryDetails', N'U') IS NULL
        BEGIN
            SET @Msg = N'Creating table dbo.StatsHistoryDetails.';
            RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

            CREATE TABLE dbo.StatsHistoryDetails
            (
                  StatsHistoryDetailsId      bigint IDENTITY(1,1) NOT NULL
                    CONSTRAINT PK_StatsHistoryDetails PRIMARY KEY CLUSTERED

                , CollectionStartTime        datetime2(3) NOT NULL
                , CollectionTime             datetime2(3) NOT NULL

                , DatabaseName               sysname NOT NULL
                , SchemaName                 sysname NOT NULL
                , TableName                  sysname NOT NULL
                , ObjectId                   int NOT NULL
                , StatsId                    int NOT NULL
                , StatsName                  sysname NOT NULL

                , StepNumber                 int NOT NULL
                , RangeHighKey               sql_variant NULL
                , RangeRows                  float NULL
                , EqualRows                  float NULL
                , DistinctRangeRows          bigint NULL
                , AverageRangeRows           float NULL
            )
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX IX_StatsHistoryDetails_CollectionStartTime
                ON dbo.StatsHistoryDetails(CollectionStartTime)
                WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX IX_StatsHistoryDetails_ObjectStats
                ON dbo.StatsHistoryDetails(DatabaseName, SchemaName, TableName, StatsName, CollectionStartTime, StepNumber)
                WITH (DATA_COMPRESSION = PAGE);
        END;
    END
    ELSE
    BEGIN
        SET @Msg = N'Simulation mode: history tables will not be created.';
        RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;
    END;

    -------------------------------------------------------------------------
    -- Temporary filter and staging tables
    -------------------------------------------------------------------------
    CREATE TABLE #DatabaseFilter
    (
          Pattern       nvarchar(512) COLLATE DATABASE_DEFAULT NOT NULL
        , IsExclude     bit NOT NULL
        , IsKeyword     bit NOT NULL
    );

    CREATE TABLE #TableFilter
    (
          Pattern       nvarchar(512) COLLATE DATABASE_DEFAULT NOT NULL
        , IsExclude     bit NOT NULL
    );

    CREATE TABLE #StatsFilter
    (
          Pattern       nvarchar(512) COLLATE DATABASE_DEFAULT NOT NULL
        , IsExclude     bit NOT NULL
    );

    CREATE TABLE #SelectedDatabases
    (
          DatabaseName  sysname COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY
    );

    CREATE TABLE #StatsHistoryStage
    (
          CollectionStartTime        datetime2(3) NOT NULL
        , CollectionTime             datetime2(3) NOT NULL

        , DatabaseName               sysname COLLATE DATABASE_DEFAULT NOT NULL
        , SchemaName                 sysname COLLATE DATABASE_DEFAULT NOT NULL
        , TableName                  sysname COLLATE DATABASE_DEFAULT NOT NULL
        , ObjectId                   int NOT NULL
        , StatsId                    int NOT NULL
        , StatsName                  sysname COLLATE DATABASE_DEFAULT NOT NULL

        , IndexId                    int NULL
        , IndexName                  sysname COLLATE DATABASE_DEFAULT NULL
        , IndexTypeDesc              nvarchar(60) COLLATE DATABASE_DEFAULT NULL
        , IsPrimaryKey               bit NULL
        , IsUnique                   bit NULL
        , IsUniqueConstraint         bit NULL

        , AutoCreated                bit NOT NULL
        , UserCreated                bit NOT NULL
        , NoRecompute                bit NOT NULL
        , HasFilter                  bit NOT NULL
        , FilterDefinition           nvarchar(max) COLLATE DATABASE_DEFAULT NULL
        , IsIncremental              bit NOT NULL
        , IsTemporary                bit NOT NULL
        , HasPersistedSample         bit NOT NULL

        , StatsColumns               nvarchar(max) COLLATE DATABASE_DEFAULT NULL
        , StatsColumnsCount          int NOT NULL

        , LastUpdated                datetime2(3) NULL
        , Rows                       bigint NULL
        , RowsSampled                bigint NULL
        , SamplePercent              decimal(9,4) NULL
        , Steps                      int NULL
        , UnfilteredRows             bigint NULL
        , ModificationCounter        bigint NULL
        , PersistedSamplePercent     decimal(9,4) NULL
    );

    CREATE TABLE #StatsHistoryDetailsStage
    (
          CollectionStartTime        datetime2(3) NOT NULL
        , CollectionTime             datetime2(3) NOT NULL

        , DatabaseName               sysname COLLATE DATABASE_DEFAULT NOT NULL
        , SchemaName                 sysname COLLATE DATABASE_DEFAULT NOT NULL
        , TableName                  sysname COLLATE DATABASE_DEFAULT NOT NULL
        , ObjectId                   int NOT NULL
        , StatsId                    int NOT NULL
        , StatsName                  sysname COLLATE DATABASE_DEFAULT NOT NULL

        , StepNumber                 int NOT NULL
        , RangeHighKey               sql_variant NULL
        , RangeRows                  float NULL
        , EqualRows                  float NULL
        , DistinctRangeRows          bigint NULL
        , AverageRangeRows           float NULL
    );

    -------------------------------------------------------------------------
    -- Parse database filter
    -------------------------------------------------------------------------
    ;WITH src AS
    (
        SELECT LTRIM(RTRIM(value)) AS Item
        FROM string_split(COALESCE(@Databases, N'USER_DATABASES'), N',')
        WHERE LTRIM(RTRIM(value)) <> N''
    )
    INSERT INTO #DatabaseFilter
    (
          Pattern
        , IsExclude
        , IsKeyword
    )
    SELECT
          CASE
              WHEN LEFT(Item, 1) = N'-'
              THEN LTRIM(RTRIM(SUBSTRING(Item, 2, 4000)))
              ELSE Item
          END
        , CASE
              WHEN LEFT(Item, 1) = N'-'
              THEN 1
              ELSE 0
          END
        , CASE
              WHEN UPPER
                   (
                       CASE
                           WHEN LEFT(Item, 1) = N'-'
                           THEN LTRIM(RTRIM(SUBSTRING(Item, 2, 4000)))
                           ELSE Item
                       END
                   ) IN (N'ALL_DATABASES', N'USER_DATABASES', N'SYSTEM_DATABASES')
              THEN 1
              ELSE 0
          END
    FROM src;

    -------------------------------------------------------------------------
    -- Parse table filter
    -------------------------------------------------------------------------
    ;WITH src AS
    (
        SELECT LTRIM(RTRIM(value)) AS Item
        FROM string_split(COALESCE(@Tables, N'%'), N',')
        WHERE LTRIM(RTRIM(value)) <> N''
    )
    INSERT INTO #TableFilter
    (
          Pattern
        , IsExclude
    )
    SELECT
          CASE
              WHEN LEFT(Item, 1) = N'-'
              THEN LTRIM(RTRIM(SUBSTRING(Item, 2, 4000)))
              ELSE Item
          END
        , CASE
              WHEN LEFT(Item, 1) = N'-'
              THEN 1
              ELSE 0
          END
    FROM src;

    -------------------------------------------------------------------------
    -- Parse stats filter
    -------------------------------------------------------------------------
    ;WITH src AS
    (
        SELECT LTRIM(RTRIM(value)) AS Item
        FROM string_split(COALESCE(@Stats, N'%'), N',')
        WHERE LTRIM(RTRIM(value)) <> N''
    )
    INSERT INTO #StatsFilter
    (
          Pattern
        , IsExclude
    )
    SELECT
          CASE
              WHEN LEFT(Item, 1) = N'-'
              THEN LTRIM(RTRIM(SUBSTRING(Item, 2, 4000)))
              ELSE Item
          END
        , CASE
              WHEN LEFT(Item, 1) = N'-'
              THEN 1
              ELSE 0
          END
    FROM src;

    IF NOT EXISTS (SELECT 1 FROM #DatabaseFilter WHERE IsExclude = 0)
    BEGIN
        INSERT INTO #DatabaseFilter
        (
              Pattern
            , IsExclude
            , IsKeyword
        )
        VALUES
        (
              N'USER_DATABASES'
            , 0
            , 1
        );
    END;

    IF NOT EXISTS (SELECT 1 FROM #TableFilter WHERE IsExclude = 0)
    BEGIN
        INSERT INTO #TableFilter
        (
              Pattern
            , IsExclude
        )
        VALUES
        (
              N'%'
            , 0
        );
    END;

    IF NOT EXISTS (SELECT 1 FROM #StatsFilter WHERE IsExclude = 0)
    BEGIN
        INSERT INTO #StatsFilter
        (
              Pattern
            , IsExclude
        )
        VALUES
        (
              N'%'
            , 0
        );
    END;

    -------------------------------------------------------------------------
    -- Resolve selected databases
    -------------------------------------------------------------------------
    INSERT INTO #SelectedDatabases
    (
        DatabaseName
    )
    SELECT d.name COLLATE DATABASE_DEFAULT
    FROM sys.databases AS d
    WHERE
        d.state_desc = N'ONLINE'
        AND d.source_database_id IS NULL
        AND d.name <> N'tempdb'
        AND EXISTS
        (
            SELECT 1
            FROM #DatabaseFilter AS f
            WHERE
                f.IsExclude = 0
                AND
                (
                       UPPER(f.Pattern) = N'ALL_DATABASES'
                    OR (UPPER(f.Pattern) = N'USER_DATABASES' AND d.database_id > 4)
                    OR (UPPER(f.Pattern) = N'SYSTEM_DATABASES' AND d.database_id <= 4)
                    OR
                    (
                        f.IsKeyword = 0
                        AND d.name COLLATE DATABASE_DEFAULT LIKE f.Pattern COLLATE DATABASE_DEFAULT
                    )
                )
        )
        AND NOT EXISTS
        (
            SELECT 1
            FROM #DatabaseFilter AS f
            WHERE
                f.IsExclude = 1
                AND
                (
                       UPPER(f.Pattern) = N'ALL_DATABASES'
                    OR (UPPER(f.Pattern) = N'USER_DATABASES' AND d.database_id > 4)
                    OR (UPPER(f.Pattern) = N'SYSTEM_DATABASES' AND d.database_id <= 4)
                    OR
                    (
                        f.IsKeyword = 0
                        AND d.name COLLATE DATABASE_DEFAULT LIKE f.Pattern COLLATE DATABASE_DEFAULT
                    )
                )
        );

    SET @Msg = N'Selected databases:';
    RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

    DECLARE db_print CURSOR LOCAL FAST_FORWARD FOR
        SELECT DatabaseName
        FROM #SelectedDatabases
        ORDER BY DatabaseName;

    OPEN db_print;

    FETCH NEXT FROM db_print INTO @CurrentDatabase;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @Msg = N' - [' + @CurrentDatabase + N']';
        RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

        FETCH NEXT FROM db_print INTO @CurrentDatabase;
    END;

    CLOSE db_print;
    DEALLOCATE db_print;

    -------------------------------------------------------------------------
    -- Simulation output for purge
    -------------------------------------------------------------------------
    IF @RetentionDays IS NOT NULL
    BEGIN
        SET @Msg = N'Simulation purge command: DELETE FROM dbo.StatsHistoryDetails '
                 + N'WHERE CollectionStartTime < DATEADD(DAY, -'
                 + CONVERT(nvarchar(20), @RetentionDays)
                 + N', @CollectionStartTime);';

        RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

        SET @Msg = N'Simulation purge command: DELETE FROM dbo.StatsHistory '
                 + N'WHERE CollectionStartTime < DATEADD(DAY, -'
                 + CONVERT(nvarchar(20), @RetentionDays)
                 + N', @CollectionStartTime);';

        RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;
    END;

    -------------------------------------------------------------------------
    -- Collect metadata and optional histogram details
    -------------------------------------------------------------------------
    DECLARE db_collect CURSOR LOCAL FAST_FORWARD FOR
        SELECT DatabaseName
        FROM #SelectedDatabases
        ORDER BY DatabaseName;

    OPEN db_collect;

    FETCH NEXT FROM db_collect INTO @CurrentDatabase;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @Msg = N'Collecting statistics metadata from [' + @CurrentDatabase + N'].';
        RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

        SET @SQL = N'
USE ' + QUOTENAME(@CurrentDatabase) + N';

;WITH StatsColumns AS
(
    SELECT
          sc.object_id
        , sc.stats_id
        , COUNT_BIG(*) AS StatsColumnsCount
        , STRING_AGG
          (
              CONVERT(nvarchar(max), QUOTENAME(c.name) COLLATE DATABASE_DEFAULT),
              N'', ''
          ) WITHIN GROUP (ORDER BY sc.stats_column_id) AS StatsColumns
    FROM sys.stats_columns AS sc
    INNER JOIN sys.columns AS c
        ON c.object_id = sc.object_id
       AND c.column_id = sc.column_id
    GROUP BY
          sc.object_id
        , sc.stats_id
)
INSERT INTO #StatsHistoryStage
(
      CollectionStartTime
    , CollectionTime
    , DatabaseName
    , SchemaName
    , TableName
    , ObjectId
    , StatsId
    , StatsName
    , IndexId
    , IndexName
    , IndexTypeDesc
    , IsPrimaryKey
    , IsUnique
    , IsUniqueConstraint
    , AutoCreated
    , UserCreated
    , NoRecompute
    , HasFilter
    , FilterDefinition
    , IsIncremental
    , IsTemporary
    , HasPersistedSample
    , StatsColumns
    , StatsColumnsCount
    , LastUpdated
    , Rows
    , RowsSampled
    , SamplePercent
    , Steps
    , UnfilteredRows
    , ModificationCounter
    , PersistedSamplePercent
)
SELECT
      @CollectionStartTime
    , SYSDATETIME()
    , CONVERT(sysname, DB_NAME() COLLATE DATABASE_DEFAULT)
    , CONVERT(sysname, sch.name COLLATE DATABASE_DEFAULT)
    , CONVERT(sysname, obj.name COLLATE DATABASE_DEFAULT)
    , obj.object_id
    , st.stats_id
    , CONVERT(sysname, st.name COLLATE DATABASE_DEFAULT)
    , ix.index_id
    , CONVERT(sysname, ix.name COLLATE DATABASE_DEFAULT)
    , CONVERT(nvarchar(60), ix.type_desc COLLATE DATABASE_DEFAULT)
    , ix.is_primary_key
    , ix.is_unique
    , ix.is_unique_constraint
    , st.auto_created
    , st.user_created
    , st.no_recompute
    , st.has_filter
    , CONVERT(nvarchar(max), st.filter_definition COLLATE DATABASE_DEFAULT)
    , st.is_incremental
    , st.is_temporary
    , st.has_persisted_sample
    , CONVERT(nvarchar(max), cols.StatsColumns COLLATE DATABASE_DEFAULT)
    , CONVERT(int, ISNULL(cols.StatsColumnsCount, 0))
    , sp.last_updated
    , sp.rows
    , sp.rows_sampled
    , CONVERT
      (
          decimal(9,4),
          CASE
              WHEN sp.rows IS NULL OR sp.rows = 0
              THEN NULL
              ELSE
                  (
                      CONVERT(decimal(19,4), sp.rows_sampled)
                      * CONVERT(decimal(19,4), 100.0)
                  )
                  / CONVERT(decimal(19,4), sp.rows)
          END
      )
    , sp.steps
    , sp.unfiltered_rows
    , sp.modification_counter
    , sp.persisted_sample_percent
FROM sys.stats AS st
INNER JOIN sys.objects AS obj
    ON obj.object_id = st.object_id
INNER JOIN sys.schemas AS sch
    ON sch.schema_id = obj.schema_id
LEFT JOIN sys.indexes AS ix
    ON ix.object_id = st.object_id
   AND ix.index_id = st.stats_id
LEFT JOIN StatsColumns AS cols
    ON cols.object_id = st.object_id
   AND cols.stats_id = st.stats_id
OUTER APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) AS sp
WHERE
    obj.type = ''U''
    AND obj.is_ms_shipped = 0
    AND EXISTS
    (
        SELECT 1
        FROM #TableFilter AS tf
        WHERE
            tf.IsExclude = 0
            AND
            (
                   (
                       QUOTENAME(sch.name) COLLATE DATABASE_DEFAULT
                       + N''.''
                       + QUOTENAME(obj.name) COLLATE DATABASE_DEFAULT
                   ) LIKE tf.Pattern COLLATE DATABASE_DEFAULT

                OR (
                       sch.name COLLATE DATABASE_DEFAULT
                       + N''.''
                       + obj.name COLLATE DATABASE_DEFAULT
                   ) LIKE tf.Pattern COLLATE DATABASE_DEFAULT

                OR obj.name COLLATE DATABASE_DEFAULT
                   LIKE tf.Pattern COLLATE DATABASE_DEFAULT
            )
    )
    AND NOT EXISTS
    (
        SELECT 1
        FROM #TableFilter AS tf
        WHERE
            tf.IsExclude = 1
            AND
            (
                   (
                       QUOTENAME(sch.name) COLLATE DATABASE_DEFAULT
                       + N''.''
                       + QUOTENAME(obj.name) COLLATE DATABASE_DEFAULT
                   ) LIKE tf.Pattern COLLATE DATABASE_DEFAULT

                OR (
                       sch.name COLLATE DATABASE_DEFAULT
                       + N''.''
                       + obj.name COLLATE DATABASE_DEFAULT
                   ) LIKE tf.Pattern COLLATE DATABASE_DEFAULT

                OR obj.name COLLATE DATABASE_DEFAULT
                   LIKE tf.Pattern COLLATE DATABASE_DEFAULT
            )
    )
    AND EXISTS
    (
        SELECT 1
        FROM #StatsFilter AS sf
        WHERE
            sf.IsExclude = 0
            AND st.name COLLATE DATABASE_DEFAULT
                LIKE sf.Pattern COLLATE DATABASE_DEFAULT
    )
    AND NOT EXISTS
    (
        SELECT 1
        FROM #StatsFilter AS sf
        WHERE
            sf.IsExclude = 1
            AND st.name COLLATE DATABASE_DEFAULT
                LIKE sf.Pattern COLLATE DATABASE_DEFAULT
    );
';

        IF @Execute = 'N'
        BEGIN
            SET @Msg = N'Simulation: command that would collect metadata from [' + @CurrentDatabase + N']:';
            RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

            RAISERROR('%s', 10, 1, @SQL) WITH NOWAIT;
        END
        ELSE
        BEGIN
            EXEC sys.sp_executesql
                  @SQL
                , N'@CollectionStartTime datetime2(3)'
                , @CollectionStartTime = @CollectionStartTime;
        END;

        IF @GetDetails = 'Y'
        BEGIN
            SET @Msg = N'Collecting histogram details from [' + @CurrentDatabase + N'].';
            RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

            SET @SQL = N'
USE ' + QUOTENAME(@CurrentDatabase) + N';

INSERT INTO #StatsHistoryDetailsStage
(
      CollectionStartTime
    , CollectionTime
    , DatabaseName
    , SchemaName
    , TableName
    , ObjectId
    , StatsId
    , StatsName
    , StepNumber
    , RangeHighKey
    , RangeRows
    , EqualRows
    , DistinctRangeRows
    , AverageRangeRows
)
SELECT
      @CollectionStartTime
    , SYSDATETIME()
    , CONVERT(sysname, DB_NAME() COLLATE DATABASE_DEFAULT)
    , CONVERT(sysname, sch.name COLLATE DATABASE_DEFAULT)
    , CONVERT(sysname, obj.name COLLATE DATABASE_DEFAULT)
    , obj.object_id
    , st.stats_id
    , CONVERT(sysname, st.name COLLATE DATABASE_DEFAULT)
    , h.step_number
    , h.range_high_key
    , h.range_rows
    , h.equal_rows
    , h.distinct_range_rows
    , h.average_range_rows
FROM sys.stats AS st
INNER JOIN sys.objects AS obj
    ON obj.object_id = st.object_id
INNER JOIN sys.schemas AS sch
    ON sch.schema_id = obj.schema_id
CROSS APPLY sys.dm_db_stats_histogram(st.object_id, st.stats_id) AS h
WHERE
    obj.type = ''U''
    AND obj.is_ms_shipped = 0
    AND EXISTS
    (
        SELECT 1
        FROM #TableFilter AS tf
        WHERE
            tf.IsExclude = 0
            AND
            (
                   (
                       QUOTENAME(sch.name) COLLATE DATABASE_DEFAULT
                       + N''.''
                       + QUOTENAME(obj.name) COLLATE DATABASE_DEFAULT
                   ) LIKE tf.Pattern COLLATE DATABASE_DEFAULT

                OR (
                       sch.name COLLATE DATABASE_DEFAULT
                       + N''.''
                       + obj.name COLLATE DATABASE_DEFAULT
                   ) LIKE tf.Pattern COLLATE DATABASE_DEFAULT

                OR obj.name COLLATE DATABASE_DEFAULT
                   LIKE tf.Pattern COLLATE DATABASE_DEFAULT
            )
    )
    AND NOT EXISTS
    (
        SELECT 1
        FROM #TableFilter AS tf
        WHERE
            tf.IsExclude = 1
            AND
            (
                   (
                       QUOTENAME(sch.name) COLLATE DATABASE_DEFAULT
                       + N''.''
                       + QUOTENAME(obj.name) COLLATE DATABASE_DEFAULT
                   ) LIKE tf.Pattern COLLATE DATABASE_DEFAULT

                OR (
                       sch.name COLLATE DATABASE_DEFAULT
                       + N''.''
                       + obj.name COLLATE DATABASE_DEFAULT
                   ) LIKE tf.Pattern COLLATE DATABASE_DEFAULT

                OR obj.name COLLATE DATABASE_DEFAULT
                   LIKE tf.Pattern COLLATE DATABASE_DEFAULT
            )
    )
    AND EXISTS
    (
        SELECT 1
        FROM #StatsFilter AS sf
        WHERE
            sf.IsExclude = 0
            AND st.name COLLATE DATABASE_DEFAULT
                LIKE sf.Pattern COLLATE DATABASE_DEFAULT
    )
    AND NOT EXISTS
    (
        SELECT 1
        FROM #StatsFilter AS sf
        WHERE
            sf.IsExclude = 1
            AND st.name COLLATE DATABASE_DEFAULT
                LIKE sf.Pattern COLLATE DATABASE_DEFAULT
    );
';

            IF @Execute = 'N'
            BEGIN
                SET @Msg = N'Simulation: command that would collect histogram details from [' + @CurrentDatabase + N']:';
                RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

                RAISERROR('%s', 10, 1, @SQL) WITH NOWAIT;
            END
            ELSE
            BEGIN
                EXEC sys.sp_executesql
                      @SQL
                    , N'@CollectionStartTime datetime2(3)'
                    , @CollectionStartTime = @CollectionStartTime;
            END;
        END;

        FETCH NEXT FROM db_collect INTO @CurrentDatabase;
    END;

    CLOSE db_collect;
    DEALLOCATE db_collect;

    -------------------------------------------------------------------------
    -- Final insert and purge
    -------------------------------------------------------------------------
    IF @Execute = 'Y'
    BEGIN
        SELECT @MetadataRows = COUNT_BIG(*)
        FROM #StatsHistoryStage;

        SELECT @DetailsRows = COUNT_BIG(*)
        FROM #StatsHistoryDetailsStage;

        SET @Msg = N'Inserting ' + CONVERT(nvarchar(30), @MetadataRows) + N' rows into dbo.StatsHistory.';
        RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

        INSERT INTO dbo.StatsHistory
        (
              CollectionStartTime
            , CollectionTime
            , DatabaseName
            , SchemaName
            , TableName
            , ObjectId
            , StatsId
            , StatsName
            , IndexId
            , IndexName
            , IndexTypeDesc
            , IsPrimaryKey
            , IsUnique
            , IsUniqueConstraint
            , AutoCreated
            , UserCreated
            , NoRecompute
            , HasFilter
            , FilterDefinition
            , IsIncremental
            , IsTemporary
            , HasPersistedSample
            , StatsColumns
            , StatsColumnsCount
            , LastUpdated
            , Rows
            , RowsSampled
            , SamplePercent
            , Steps
            , UnfilteredRows
            , ModificationCounter
            , PersistedSamplePercent
        )
        SELECT
              CollectionStartTime
            , CollectionTime
            , DatabaseName
            , SchemaName
            , TableName
            , ObjectId
            , StatsId
            , StatsName
            , IndexId
            , IndexName
            , IndexTypeDesc
            , IsPrimaryKey
            , IsUnique
            , IsUniqueConstraint
            , AutoCreated
            , UserCreated
            , NoRecompute
            , HasFilter
            , FilterDefinition
            , IsIncremental
            , IsTemporary
            , HasPersistedSample
            , StatsColumns
            , StatsColumnsCount
            , LastUpdated
            , Rows
            , RowsSampled
            , SamplePercent
            , Steps
            , UnfilteredRows
            , ModificationCounter
            , PersistedSamplePercent
        FROM #StatsHistoryStage;

        IF @GetDetails = 'Y'
        BEGIN
            SET @Msg = N'Inserting ' + CONVERT(nvarchar(30), @DetailsRows) + N' rows into dbo.StatsHistoryDetails.';
            RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

            INSERT INTO dbo.StatsHistoryDetails
            (
                  CollectionStartTime
                , CollectionTime
                , DatabaseName
                , SchemaName
                , TableName
                , ObjectId
                , StatsId
                , StatsName
                , StepNumber
                , RangeHighKey
                , RangeRows
                , EqualRows
                , DistinctRangeRows
                , AverageRangeRows
            )
            SELECT
                  CollectionStartTime
                , CollectionTime
                , DatabaseName
                , SchemaName
                , TableName
                , ObjectId
                , StatsId
                , StatsName
                , StepNumber
                , RangeHighKey
                , RangeRows
                , EqualRows
                , DistinctRangeRows
                , AverageRangeRows
            FROM #StatsHistoryDetailsStage;
        END;

        IF @RetentionDays IS NOT NULL
        BEGIN
            SET @Msg = N'Purging history older than '
                     + CONVERT(nvarchar(20), @RetentionDays)
                     + N' days based on CollectionStartTime.';

            RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

            DELETE FROM dbo.StatsHistoryDetails
            WHERE CollectionStartTime < DATEADD(DAY, -@RetentionDays, @CollectionStartTime);

            SET @RowsDeleted = @@ROWCOUNT;

            SET @Msg = N'Purged ' + CONVERT(nvarchar(30), @RowsDeleted) + N' rows from dbo.StatsHistoryDetails.';
            RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;

            DELETE FROM dbo.StatsHistory
            WHERE CollectionStartTime < DATEADD(DAY, -@RetentionDays, @CollectionStartTime);

            SET @RowsDeleted = @@ROWCOUNT;

            SET @Msg = N'Purged ' + CONVERT(nvarchar(30), @RowsDeleted) + N' rows from dbo.StatsHistory.';
            RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;
        END;
    END
    ELSE
    BEGIN
        SET @Msg = N'Simulation mode: no insert and no purge executed.';
        RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;
    END;

    SET @Msg = N'dbo.dba_CollectStatsHistory completed.';
    RAISERROR('%s', 10, 1, @Msg) WITH NOWAIT;
END;
GO