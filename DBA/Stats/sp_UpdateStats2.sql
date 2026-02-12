SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[sp_UpdateStats2]')
      AND type IN (N'P', N'PC')
)
BEGIN
    EXEC(N'CREATE PROCEDURE [dbo].[sp_UpdateStats2] AS BEGIN RETURN 0; END');
END
GO

ALTER PROCEDURE dbo.sp_UpdateStats2
      @Databases nvarchar(max) = NULL
    , @UpdateStatistics nvarchar(max) = N'ALL'
    , @OnlyModifiedStatistics nvarchar(max) = 'N'
    , @StatisticsModificationLevel int = NULL
    , @StatisticsSample int = NULL
    , @StatisticsResample nvarchar(max) = 'N'
    , @PartitionLevel nvarchar(max) = 'Y'
    , @TimeLimit int = NULL
    , @Delay int = NULL
    , @LogToTable nvarchar(max) = 'N'
    , @Execute nvarchar(max) = 'Y'

    , @IncludeSchemas nvarchar(max) = NULL
    , @ExcludeSchemas nvarchar(max) = NULL
    , @IncludeTables  nvarchar(max) = NULL
    , @ExcludeTables  nvarchar(max) = NULL
    , @IncludeStats   nvarchar(max) = NULL
    , @ExcludeStats   nvarchar(max) = NULL

    , @SamplePercentSmallTables int = 100
    , @SamplePercentBigTables int = 10
    , @SamplePercentVeryBigTables int = 1
    , @ThresholdBigTables int = 1000000
    , @ThresholdVeryBigTables int = 100000000
AS
BEGIN
    SET NOCOUNT ON;
    SET ARITHABORT ON;
    SET NUMERIC_ROUNDABORT OFF;

    ----------------------------------------------------------------------------------------------------
    -- Environment / feature detection
    ----------------------------------------------------------------------------------------------------
    DECLARE @ProcDB sysname = DB_NAME();
    DECLARE @StartTime datetime2(7) = SYSDATETIME();
    DECLARE @ErrorMsg nvarchar(max);
    DECLARE @ErrorNumber int;

    DECLARE @HasDmDbStatsProperties bit =
        CASE WHEN OBJECT_ID(N'sys.dm_db_stats_properties') IS NOT NULL THEN 1 ELSE 0 END;

    DECLARE @HasDmDbIncrementalStatsProperties bit =
        CASE WHEN OBJECT_ID(N'sys.dm_db_incremental_stats_properties') IS NOT NULL THEN 1 ELSE 0 END;

    DECLARE @HasExternalTables bit =
        CASE WHEN COL_LENGTH(N'sys.tables', N'is_external') IS NOT NULL THEN 1 ELSE 0 END;

    DECLARE @HasHadr bit =
        CASE WHEN OBJECT_ID(N'sys.dm_hadr_database_replica_states') IS NOT NULL THEN 1 ELSE 0 END;

    ----------------------------------------------------------------------------------------------------
    -- Normalize parameters
    ----------------------------------------------------------------------------------------------------
    SET @Databases = NULLIF(LTRIM(RTRIM(@Databases)), N'');
    SET @UpdateStatistics = UPPER(NULLIF(LTRIM(RTRIM(@UpdateStatistics)), N''));

    SET @OnlyModifiedStatistics = UPPER(NULLIF(LTRIM(RTRIM(@OnlyModifiedStatistics)), N'')); IF @OnlyModifiedStatistics IS NULL SET @OnlyModifiedStatistics = N'N';
    SET @StatisticsResample     = UPPER(NULLIF(LTRIM(RTRIM(@StatisticsResample)), N''));     IF @StatisticsResample IS NULL     SET @StatisticsResample     = N'N';
    SET @PartitionLevel         = UPPER(NULLIF(LTRIM(RTRIM(@PartitionLevel)), N''));         IF @PartitionLevel IS NULL         SET @PartitionLevel         = N'Y';
    SET @LogToTable             = UPPER(NULLIF(LTRIM(RTRIM(@LogToTable)), N''));             IF @LogToTable IS NULL             SET @LogToTable             = N'N';
    SET @Execute                = UPPER(NULLIF(LTRIM(RTRIM(@Execute)), N''));                IF @Execute IS NULL                SET @Execute                = N'Y';

    SET @IncludeSchemas = NULLIF(LTRIM(RTRIM(@IncludeSchemas)), N'');
    SET @ExcludeSchemas = NULLIF(LTRIM(RTRIM(@ExcludeSchemas)), N'');
    SET @IncludeTables  = NULLIF(LTRIM(RTRIM(@IncludeTables)),  N'');
    SET @ExcludeTables  = NULLIF(LTRIM(RTRIM(@ExcludeTables)),  N'');
    SET @IncludeStats   = NULLIF(LTRIM(RTRIM(@IncludeStats)),   N'');
    SET @ExcludeStats   = NULLIF(LTRIM(RTRIM(@ExcludeStats)),   N'');

    ----------------------------------------------------------------------------------------------------
    -- Validate parameters
    ----------------------------------------------------------------------------------------------------
    IF @UpdateStatistics IS NULL
    BEGIN
        RAISERROR(N'sp_UpdateStats2: @UpdateStatistics is NULL. No statistics will be updated.', 10, 1) WITH NOWAIT;
        RETURN 0;
    END;

    IF @UpdateStatistics NOT IN (N'ALL', N'INDEX', N'COLUMNS')
    BEGIN
        RAISERROR(N'sp_UpdateStats2: @UpdateStatistics must be ALL, INDEX, COLUMNS, or NULL.', 16, 1);
        RETURN 1;
    END;

    IF @OnlyModifiedStatistics NOT IN (N'Y', N'N')
    BEGIN
        RAISERROR(N'sp_UpdateStats2: @OnlyModifiedStatistics must be Y or N.', 16, 1);
        RETURN 1;
    END;

    IF @StatisticsResample NOT IN (N'Y', N'N')
    BEGIN
        RAISERROR(N'sp_UpdateStats2: @StatisticsResample must be Y or N.', 16, 1);
        RETURN 1;
    END;

    IF @PartitionLevel NOT IN (N'Y', N'N')
    BEGIN
        RAISERROR(N'sp_UpdateStats2: @PartitionLevel must be Y or N.', 16, 1);
        RETURN 1;
    END;

    IF @LogToTable NOT IN (N'Y', N'N')
    BEGIN
        RAISERROR(N'sp_UpdateStats2: @LogToTable must be Y or N.', 16, 1);
        RETURN 1;
    END;

    IF @Execute NOT IN (N'Y', N'N')
    BEGIN
        RAISERROR(N'sp_UpdateStats2: @Execute must be Y or N.', 16, 1);
        RETURN 1;
    END;

    IF @StatisticsSample IS NOT NULL AND (@StatisticsSample < 0 OR @StatisticsSample > 100)
    BEGIN
        RAISERROR(N'sp_UpdateStats2: @StatisticsSample must be between 0 and 100.', 16, 1);
        RETURN 1;
    END;

    IF @StatisticsSample IS NOT NULL AND @StatisticsResample = N'Y'
    BEGIN
        RAISERROR(N'sp_UpdateStats2: You cannot combine @StatisticsSample and @StatisticsResample = Y.', 16, 1);
        RETURN 1;
    END;

    IF @StatisticsModificationLevel IS NOT NULL AND (@StatisticsModificationLevel < 0 OR @StatisticsModificationLevel > 100)
    BEGIN
        RAISERROR(N'sp_UpdateStats2: @StatisticsModificationLevel must be between 0 and 100.', 16, 1);
        RETURN 1;
    END;

    IF @OnlyModifiedStatistics = N'Y' AND @StatisticsModificationLevel IS NOT NULL
    BEGIN
        RAISERROR(N'sp_UpdateStats2: Do not combine @OnlyModifiedStatistics = Y with @StatisticsModificationLevel.', 16, 1);
        RETURN 1;
    END;

    IF (@OnlyModifiedStatistics = N'Y' OR @StatisticsModificationLevel IS NOT NULL) AND @HasDmDbStatsProperties = 0
    BEGIN
        RAISERROR(N'sp_UpdateStats2: sys.dm_db_stats_properties is required for @OnlyModifiedStatistics or @StatisticsModificationLevel.', 16, 1);
        RETURN 1;
    END;

    IF @TimeLimit IS NOT NULL AND @TimeLimit < 0
    BEGIN
        RAISERROR(N'sp_UpdateStats2: @TimeLimit must be >= 0 seconds or NULL.', 16, 1);
        RETURN 1;
    END;

    IF @Delay IS NOT NULL AND (@Delay < 0 OR @Delay > 86399)
    BEGIN
        RAISERROR(N'sp_UpdateStats2: @Delay must be between 0 and 86399 seconds (or NULL).', 16, 1);
        RETURN 1;
    END;

    IF @SamplePercentSmallTables NOT BETWEEN 1 AND 100
        OR @SamplePercentBigTables NOT BETWEEN 1 AND 100
        OR @SamplePercentVeryBigTables NOT BETWEEN 1 AND 100
    BEGIN
        RAISERROR(N'sp_UpdateStats2: Sample percent parameters must be between 1 and 100.', 16, 1);
        RETURN 1;
    END;

    IF @ThresholdBigTables < 0 OR @ThresholdVeryBigTables < 0 OR @ThresholdVeryBigTables < @ThresholdBigTables
    BEGIN
        RAISERROR(N'sp_UpdateStats2: Threshold parameters invalid. Ensure 0 <= Big <= VeryBig.', 16, 1);
        RETURN 1;
    END;

    IF @LogToTable = N'Y' AND OBJECT_ID(N'dbo.CommandLog', N'U') IS NULL
    BEGIN
        RAISERROR(N'sp_UpdateStats2: @LogToTable = Y requires dbo.CommandLog in database %s.', 16, 1, @ProcDB);
        RETURN 1;
    END;

    ----------------------------------------------------------------------------------------------------
    -- Build filter temp tables (patterns)
    ----------------------------------------------------------------------------------------------------
    CREATE TABLE #IncludeSchemas (Pattern nvarchar(256) NOT NULL );
    CREATE TABLE #ExcludeSchemas (Pattern nvarchar(256) NOT NULL );
    CREATE TABLE #IncludeTables  (Pattern nvarchar(256) NOT NULL );
    CREATE TABLE #ExcludeTables  (Pattern nvarchar(256) NOT NULL );
    CREATE TABLE #IncludeStats   (Pattern nvarchar(256) NOT NULL );
    CREATE TABLE #ExcludeStats   (Pattern nvarchar(256) NOT NULL );

    DECLARE @x xml;
    

    IF @IncludeSchemas IS NOT NULL
    BEGIN
        DECLARE @SafeString nvarchar(max);
        SET @SafeString =
            N'<i>'
            + REPLACE(
                  REPLACE(
                      REPLACE(@IncludeSchemas, N'&', N'&amp;'),
                      N'<', N'&lt;'
                  ),
                  N'>', N'&gt;'
              );

        SET @SafeString = REPLACE(@SafeString, N',', N'</i><i>') + N'</i>';
        SET @x = TRY_CAST(@SafeString AS xml);

        INSERT INTO #IncludeSchemas(Pattern)
        SELECT LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(256)')))
        FROM @x.nodes(N'/i') T(c)
        WHERE LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(256)'))) <> N'';
    END;

    IF @ExcludeSchemas IS NOT NULL
    BEGIN
        SET @SafeString =
        N'<i>'
        + REPLACE(
                REPLACE(
                    REPLACE(@ExcludeSchemas, N'&', N'&amp;'),
                    N'<', N'&lt;'
                ),
                N'>', N'&gt;'
            );
        SET @SafeString = REPLACE(@SafeString, N',', N'</i><i>') + N'</i>';

        SET @x = TRY_CAST(@SafeString AS xml);
        INSERT INTO #ExcludeSchemas(Pattern)
        SELECT LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(256)')))
        FROM @x.nodes(N'/i') T(c)
        WHERE LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(256)'))) <> N'';
    END;

    IF @IncludeTables IS NOT NULL
    BEGIN

        SET @SafeString =
        N'<i>'
        + REPLACE(
                REPLACE(
                    REPLACE(@IncludeTables, N'&', N'&amp;'),
                    N'<', N'&lt;'
                ),
                N'>', N'&gt;'
            );
        SET @SafeString = REPLACE(@SafeString, N',', N'</i><i>') + N'</i>';

        SET @x = TRY_CAST(@SafeString AS xml);
        INSERT INTO #IncludeTables(Pattern)
        SELECT LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(256)')))
        FROM @x.nodes(N'/i') T(c)
        WHERE LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(256)'))) <> N'';
    END;

    IF @ExcludeTables IS NOT NULL
    BEGIN

        SET @SafeString =
        N'<i>'
        + REPLACE(
                REPLACE(
                    REPLACE(@ExcludeTables, N'&', N'&amp;'),
                    N'<', N'&lt;'
                ),
                N'>', N'&gt;'
            );
        SET @SafeString = REPLACE(@SafeString, N',', N'</i><i>') + N'</i>';

        INSERT INTO #ExcludeTables(Pattern)
        SELECT LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(256)')))
        FROM @x.nodes(N'/i') T(c)
        WHERE LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(256)'))) <> N'';
    END;

    IF @IncludeStats IS NOT NULL
    BEGIN
        SET @SafeString =
        N'<i>'
        + REPLACE(
                REPLACE(
                    REPLACE(@IncludeStats, N'&', N'&amp;'),
                    N'<', N'&lt;'
                ),
                N'>', N'&gt;'
            );
        SET @SafeString = REPLACE(@SafeString, N',', N'</i><i>') + N'</i>';
        INSERT INTO #IncludeStats(Pattern)
        SELECT LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(256)')))
        FROM @x.nodes(N'/i') T(c)
        WHERE LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(256)'))) <> N'';
    END;

    IF @ExcludeStats IS NOT NULL
    BEGIN
        SET @SafeString =
        N'<i>'
        + REPLACE(
                REPLACE(
                    REPLACE(@ExcludeStats, N'&', N'&amp;'),
                    N'<', N'&lt;'
                ),
                N'>', N'&gt;'
            );
        SET @SafeString = REPLACE(@SafeString, N',', N'</i><i>') + N'</i>';
        INSERT INTO #ExcludeStats(Pattern)
        SELECT LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(256)')))
        FROM @x.nodes(N'/i') T(c)
        WHERE LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(256)'))) <> N'';
    END;

    ----------------------------------------------------------------------------------------------------
    -- Select databases (supports keywords, wildcards, exclusions)
    ----------------------------------------------------------------------------------------------------
    CREATE TABLE #DatabaseTokens
    (
        ID int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Token nvarchar(4000) NOT NULL,
        IsExclude bit NOT NULL
    );

    IF @Databases IS NULL
        SET @Databases = @ProcDB;

   SET @SafeString =
        N'<i>'
        + REPLACE(
                REPLACE(
                    REPLACE(@Databases, N'&', N'&amp;'),
                    N'<', N'&lt;'
                ),
                N'>', N'&gt;'
            );
    SET @SafeString = REPLACE(@SafeString, N',', N'</i><i>') + N'</i>';
    SET @x = TRY_CAST(@SafeString AS xml);

    INSERT INTO #DatabaseTokens(Token, IsExclude)
    SELECT
        CASE WHEN LEFT(LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(4000)'))), 1) = N'-'
             THEN LTRIM(RTRIM(SUBSTRING(LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(4000)'))), 2, 4000)))
             ELSE LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(4000)')))
        END AS Token,
        CASE WHEN LEFT(LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(4000)'))), 1) = N'-' THEN 1 ELSE 0 END AS IsExclude
    FROM @x.nodes(N'/i') T(c)
    WHERE LTRIM(RTRIM(T.c.value(N'.', N'nvarchar(4000)'))) <> N'';

    CREATE TABLE #DatabaseList
    (
        DatabaseName sysname NOT NULL PRIMARY KEY,
        Selected bit NOT NULL DEFAULT(0)
    );

    INSERT INTO #DatabaseList(DatabaseName, Selected)
    SELECT d.name, 0
    FROM sys.databases d
    WHERE d.name <> N'tempdb';

    DECLARE @HasIncludeTokens bit = CASE WHEN EXISTS (SELECT 1 FROM #DatabaseTokens WHERE IsExclude = 0) THEN 1 ELSE 0 END;

    IF @HasIncludeTokens = 0
    BEGIN
        UPDATE #DatabaseList SET Selected = 1; -- treat as ALL_DATABASES when only exclusions are provided
    END;

    DECLARE @Tok nvarchar(4000), @IsEx bit;

    DECLARE dbtok CURSOR FAST_FORWARD FOR
        SELECT Token, IsExclude
        FROM #DatabaseTokens
        ORDER BY ID;

    OPEN dbtok;
    FETCH NEXT FROM dbtok INTO @Tok, @IsEx;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @TokU nvarchar(4000) = UPPER(@Tok);

        IF @TokU IN (N'ALL_DATABASES', N'SYSTEM_DATABASES', N'USER_DATABASES', N'AVAILABILITY_GROUP_DATABASES')
        BEGIN
            IF @TokU = N'ALL_DATABASES'
            BEGIN
                UPDATE #DatabaseList SET Selected = CASE WHEN @IsEx = 1 THEN 0 ELSE 1 END;
            END
            ELSE IF @TokU = N'SYSTEM_DATABASES'
            BEGIN
                UPDATE dl
                    SET Selected = CASE WHEN @IsEx = 1 THEN 0 ELSE 1 END
                FROM #DatabaseList dl
                INNER JOIN sys.databases d ON d.name = dl.DatabaseName
                WHERE d.database_id IN (1,2,3); -- master, tempdb(ignored), model? actually tempdb excluded earlier; msdb is 4
                UPDATE dl
                    SET Selected = CASE WHEN @IsEx = 1 THEN 0 ELSE 1 END
                FROM #DatabaseList dl
                INNER JOIN sys.databases d ON d.name = dl.DatabaseName
                WHERE d.database_id = 4; -- msdb
            END
            ELSE IF @TokU = N'USER_DATABASES'
            BEGIN
                UPDATE dl
                    SET Selected = CASE WHEN @IsEx = 1 THEN 0 ELSE 1 END
                FROM #DatabaseList dl
                INNER JOIN sys.databases d ON d.name = dl.DatabaseName
                WHERE d.database_id > 4;
            END
            ELSE IF @TokU = N'AVAILABILITY_GROUP_DATABASES'
            BEGIN
                IF @HasHadr = 1
                BEGIN
                    UPDATE dl
                        SET Selected = CASE WHEN @IsEx = 1 THEN 0 ELSE 1 END
                    FROM #DatabaseList dl
                    INNER JOIN sys.databases d ON d.name = dl.DatabaseName
                    INNER JOIN sys.dm_hadr_database_replica_states rs
                        ON rs.database_id = d.database_id;
                END
            END
        END
        ELSE
        BEGIN
            -- Wildcard support via LIKE
            UPDATE dl
                SET Selected = CASE WHEN @IsEx = 1 THEN 0 ELSE 1 END
            FROM #DatabaseList dl
            WHERE dl.DatabaseName COLLATE DATABASE_DEFAULT LIKE @Tok;
        END;

        FETCH NEXT FROM dbtok INTO @Tok, @IsEx;
    END;

    CLOSE dbtok;
    DEALLOCATE dbtok;

    CREATE TABLE #SelectedDatabases
    (
        DatabaseName sysname NOT NULL PRIMARY KEY
    );

    INSERT INTO #SelectedDatabases(DatabaseName)
    SELECT dl.DatabaseName
    FROM #DatabaseList dl
    INNER JOIN sys.databases d ON d.name = dl.DatabaseName
    WHERE dl.Selected = 1
      AND d.state_desc = N'ONLINE'
      AND d.is_read_only = 0
      AND ISNULL(d.is_in_standby, 0) = 0;

    IF NOT EXISTS (SELECT 1 FROM #SelectedDatabases)
    BEGIN
        RAISERROR(N'sp_UpdateStats2: No databases matched @Databases selection.', 16, 1);
        RETURN 1;
    END;

    ----------------------------------------------------------------------------------------------------
    -- Worklist table for stats
    ----------------------------------------------------------------------------------------------------
    CREATE TABLE #StatsWorklist
    (
        ID int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        DatabaseName sysname NOT NULL,
        SchemaName sysname NOT NULL,
        ObjectName sysname NOT NULL,
        ObjectType char(2) NOT NULL,
        ObjectID int NOT NULL,
        StatsID int NOT NULL,
        StatisticsName sysname NOT NULL,
        IsIncremental bit NOT NULL,
        NoRecompute bit NOT NULL,
        IndexName sysname NULL,
        IndexType tinyint NULL,
        CountRows bigint NULL,
        StatRows bigint NULL,
        ModificationCounter bigint NULL,
        PartitionsToUpdate nvarchar(max) NULL,
        EffectiveSample int NULL
    );

    ----------------------------------------------------------------------------------------------------
    -- Iterate databases and execute UPDATE STATISTICS
    ----------------------------------------------------------------------------------------------------
    DECLARE @DB sysname;

    DECLARE dbcur CURSOR FAST_FORWARD FOR
        SELECT DatabaseName
        FROM #SelectedDatabases
        ORDER BY DatabaseName;

    OPEN dbcur;
    FETCH NEXT FROM dbcur INTO @DB;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @TimeLimit IS NOT NULL AND DATEDIFF(SECOND, @StartTime, SYSDATETIME()) >= @TimeLimit
        BEGIN
            RAISERROR(N'sp_UpdateStats2: TimeLimit reached. Stopping.', 10, 1) WITH NOWAIT;
            BREAK;
        END;

        RAISERROR(N'sp_UpdateStats2: Processing database %s', 10, 1, @DB) WITH NOWAIT;

        DELETE FROM #StatsWorklist;

        DECLARE @InnerParams nvarchar(max) =
            N'@UpdateStatistics nvarchar(max),
              @OnlyModifiedStatistics nvarchar(max),
              @StatisticsModificationLevel int,
              @StatisticsSample int,
              @StatisticsResample nvarchar(max),
              @PartitionLevel nvarchar(max),
              @ThresholdBigTables int,
              @ThresholdVeryBigTables int,
              @SamplePercentSmallTables int,
              @SamplePercentBigTables int,
              @SamplePercentVeryBigTables int,
              @HasDmDbIncrementalStatsProperties bit';

        DECLARE @InnerSQL nvarchar(max) = N'
SET NOCOUNT ON;

;WITH Base AS
(
    SELECT
          DatabaseName = DB_NAME()
        , SchemaName = sc.name
        , ObjectName = o.name
        , ObjectType = o.type
        , ObjectID = o.object_id
        , StatsID = s.stats_id
        , StatisticsName = s.name
        , IsIncremental = CONVERT(bit, s.is_incremental)
        , NoRecompute = CONVERT(bit, s.no_recompute)
        , IndexName = i.name
        , IndexType = i.type
        , CountRows = rc.CountRows
        , StatRows = CASE
                        WHEN s.is_incremental = 1 AND @HasDmDbIncrementalStatsProperties = 1 THEN ispAgg.RowsTotal
                        ELSE sp.[rows]
                     END
        , ModificationCounter = CASE
                        WHEN s.is_incremental = 1 AND @HasDmDbIncrementalStatsProperties = 1 THEN ispAgg.ModificationCounterTotal
                        ELSE sp.modification_counter
                     END
        , PartitionsToUpdate = CASE
                        WHEN s.is_incremental = 1
                         AND @HasDmDbIncrementalStatsProperties = 1
                         AND @PartitionLevel = N''Y''
                         AND @StatisticsResample = N''Y''
                        THEN ispAgg.PartitionsToUpdate
                        ELSE NULL
                     END
        , EffectiveSample = CASE
                        WHEN @StatisticsResample = N''Y'' THEN NULL
                        WHEN @StatisticsSample IS NOT NULL THEN @StatisticsSample
                        ELSE CASE
                                WHEN ISNULL(rc.CountRows,0) >= @ThresholdVeryBigTables THEN @SamplePercentVeryBigTables
                                WHEN ISNULL(rc.CountRows,0) >= @ThresholdBigTables THEN @SamplePercentBigTables
                                ELSE @SamplePercentSmallTables
                             END
                     END
    FROM sys.stats s
    INNER JOIN sys.objects o ON o.object_id = s.object_id
    INNER JOIN sys.schemas sc ON sc.schema_id = o.schema_id
    LEFT JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.stats_id
';

        IF @HasExternalTables = 1
        BEGIN
            SET @InnerSQL += N'LEFT JOIN sys.tables t ON t.object_id = o.object_id
';
        END;

        SET @InnerSQL += N'
    OUTER APPLY (
        SELECT CountRows = SUM(CONVERT(bigint, ps.row_count))
        FROM sys.dm_db_partition_stats ps
        WHERE ps.object_id = s.object_id
          AND ps.index_id IN (0,1)
    ) rc
    OUTER APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
';

        IF @HasDmDbIncrementalStatsProperties = 1
        BEGIN
            SET @InnerSQL += N'
    OUTER APPLY
    (
        SELECT
              RowsTotal = SUM(CONVERT(bigint, isp.rows))
            , ModificationCounterTotal = SUM(CONVERT(bigint, isp.modification_counter))
            , PartitionsToUpdate =
                STUFF((
                    SELECT N'','' + CONVERT(nvarchar(12), isp2.partition_number)
                    FROM sys.dm_db_incremental_stats_properties(s.object_id, s.stats_id) isp2
                    WHERE
                        (
                            (@OnlyModifiedStatistics = N''Y'' AND ISNULL(isp2.modification_counter,0) > 0)
                            OR
                            (@StatisticsModificationLevel IS NOT NULL AND
                                (
                                    ISNULL(isp2.modification_counter,0) >= ISNULL(isp2.rows,0) * (@StatisticsModificationLevel / 100.0)
                                    OR
                                    ISNULL(isp2.modification_counter,0) >= SQRT(CONVERT(float, ISNULL(isp2.rows,0)) * 1000.0)
                                )
                            )
                            OR
                            (@OnlyModifiedStatistics = N''N'' AND @StatisticsModificationLevel IS NULL)
                        )
                    ORDER BY isp2.partition_number
                    FOR XML PATH(N''''), TYPE
                ).value(N''.'', N''nvarchar(max)''), 1, 1, N'''')
        FROM sys.dm_db_incremental_stats_properties(s.object_id, s.stats_id) isp
    ) ispAgg
';
        END
        ELSE
        BEGIN
            SET @InnerSQL += N'
    OUTER APPLY (SELECT CAST(NULL AS bigint) AS RowsTotal,
                       CAST(NULL AS bigint) AS ModificationCounterTotal,
                       CAST(NULL AS nvarchar(max)) AS PartitionsToUpdate) ispAgg
';
        END;

        SET @InnerSQL += N'
    WHERE o.type IN (N''U'', N''V'')
      AND o.is_ms_shipped = 0
      AND (i.index_id IS NULL OR i.type NOT IN (5,6)) -- avoid columnstore index stats that cannot be updated
';

        IF @HasExternalTables = 1
        BEGIN
            SET @InnerSQL += N'      AND ISNULL(t.is_external, 0) = 0
';
        END;

        SET @InnerSQL += N')
SELECT
      DatabaseName
    , SchemaName
    , ObjectName
    , ObjectType
    , ObjectID
    , StatsID
    , StatisticsName
    , IsIncremental
    , NoRecompute
    , IndexName
    , IndexType
    , CountRows
    , StatRows
    , ModificationCounter
    , PartitionsToUpdate
    , EffectiveSample
FROM Base
WHERE
    (
        (@UpdateStatistics = N''ALL'')
        OR (@UpdateStatistics = N''INDEX''   AND IndexName IS NOT NULL)
        OR (@UpdateStatistics = N''COLUMNS'' AND IndexName IS NULL)
    )
    AND
    (
        (@OnlyModifiedStatistics = N''N'' AND @StatisticsModificationLevel IS NULL)
        OR (@OnlyModifiedStatistics = N''Y'' AND ISNULL(ModificationCounter,0) > 0)
        OR (@StatisticsModificationLevel IS NOT NULL AND
            (
                ISNULL(ModificationCounter,0) >= ISNULL(StatRows,0) * (@StatisticsModificationLevel / 100.0)
                OR
                ISNULL(ModificationCounter,0) >= SQRT(CONVERT(float, ISNULL(StatRows,0)) * 1000.0)
            )
        )
    )
    AND
    (
        NOT (IsIncremental = 1 AND @PartitionLevel = N''Y'' AND @StatisticsResample = N''Y'')
        OR NULLIF(PartitionsToUpdate, N'''') IS NOT NULL
    )
    AND (NOT EXISTS (SELECT 1 FROM #IncludeSchemas)
         OR EXISTS (SELECT 1 FROM #IncludeSchemas f WHERE SchemaName COLLATE DATABASE_DEFAULT LIKE f.Pattern))
    AND NOT EXISTS (SELECT 1 FROM #ExcludeSchemas f WHERE SchemaName COLLATE DATABASE_DEFAULT LIKE f.Pattern)

    AND (NOT EXISTS (SELECT 1 FROM #IncludeTables)
         OR EXISTS (
                SELECT 1
                FROM #IncludeTables f
                WHERE
                    (CASE WHEN CHARINDEX(N''.'', f.Pattern) > 0
                          THEN SchemaName + N''.'' + ObjectName
                          ELSE ObjectName
                     END) COLLATE DATABASE_DEFAULT LIKE f.Pattern
         ))
    AND NOT EXISTS (
            SELECT 1
            FROM #ExcludeTables f
            WHERE
                (CASE WHEN CHARINDEX(N''.'', f.Pattern) > 0
                      THEN SchemaName + N''.'' + ObjectName
                      ELSE ObjectName
                 END) COLLATE DATABASE_DEFAULT LIKE f.Pattern
    )

    AND (NOT EXISTS (SELECT 1 FROM #IncludeStats)
         OR EXISTS (
                SELECT 1
                FROM #IncludeStats f
                WHERE
                    (CASE WHEN CHARINDEX(N''.'', f.Pattern) > 0
                          THEN SchemaName + N''.'' + ObjectName + N''.'' + StatisticsName
                          ELSE StatisticsName
                     END) COLLATE DATABASE_DEFAULT LIKE f.Pattern
         ))
    AND NOT EXISTS (
            SELECT 1
            FROM #ExcludeStats f
            WHERE
                (CASE WHEN CHARINDEX(N''.'', f.Pattern) > 0
                      THEN SchemaName + N''.'' + ObjectName + N''.'' + StatisticsName
                      ELSE StatisticsName
                 END) COLLATE DATABASE_DEFAULT LIKE f.Pattern
    )
ORDER BY SchemaName, ObjectName, StatisticsName;
';


        DECLARE @OuterSQL nvarchar(max) =
            N'EXEC ' + QUOTENAME(@DB) + N'.sys.sp_executesql
                 @stmt = @stmt,
                 @params = @params,
                 @UpdateStatistics = @UpdateStatistics,
                 @OnlyModifiedStatistics = @OnlyModifiedStatistics,
                 @StatisticsModificationLevel = @StatisticsModificationLevel,
                 @StatisticsSample = @StatisticsSample,
                 @StatisticsResample = @StatisticsResample,
                 @PartitionLevel = @PartitionLevel,
                 @ThresholdBigTables = @ThresholdBigTables,
                 @ThresholdVeryBigTables = @ThresholdVeryBigTables,
                 @SamplePercentSmallTables = @SamplePercentSmallTables,
                 @SamplePercentBigTables = @SamplePercentBigTables,
                 @SamplePercentVeryBigTables = @SamplePercentVeryBigTables,
                 @HasDmDbIncrementalStatsProperties = @HasDmDbIncrementalStatsProperties;';

        BEGIN TRY
            INSERT INTO #StatsWorklist
            (
                DatabaseName, SchemaName, ObjectName, ObjectType, ObjectID, StatsID, StatisticsName,
                IsIncremental, NoRecompute, IndexName, IndexType, CountRows, StatRows,
                ModificationCounter, PartitionsToUpdate, EffectiveSample
            )
            EXEC sys.sp_executesql
                @OuterSQL,
                N'@stmt nvarchar(max), @params nvarchar(max),
                  @UpdateStatistics nvarchar(max), @OnlyModifiedStatistics nvarchar(max),
                  @StatisticsModificationLevel int, @StatisticsSample int,
                  @StatisticsResample nvarchar(max), @PartitionLevel nvarchar(max),
                  @ThresholdBigTables int, @ThresholdVeryBigTables int,
                  @SamplePercentSmallTables int, @SamplePercentBigTables int, @SamplePercentVeryBigTables int,
                  @HasDmDbIncrementalStatsProperties bit',
                @stmt = @InnerSQL,
                @params = @InnerParams,
                @UpdateStatistics = @UpdateStatistics,
                @OnlyModifiedStatistics = @OnlyModifiedStatistics,
                @StatisticsModificationLevel = @StatisticsModificationLevel,
                @StatisticsSample = @StatisticsSample,
                @StatisticsResample = @StatisticsResample,
                @PartitionLevel = @PartitionLevel,
                @ThresholdBigTables = @ThresholdBigTables,
                @ThresholdVeryBigTables = @ThresholdVeryBigTables,
                @SamplePercentSmallTables = @SamplePercentSmallTables,
                @SamplePercentBigTables = @SamplePercentBigTables,
                @SamplePercentVeryBigTables = @SamplePercentVeryBigTables,
                @HasDmDbIncrementalStatsProperties = @HasDmDbIncrementalStatsProperties;
        END TRY
        BEGIN CATCH
            SELECT @ErrorMsg=ERROR_MESSAGE(), @ErrorNumber=ERROR_NUMBER();
            
            RAISERROR(N'sp_UpdateStats2: Failed to read stats list in database %s. Error %d: %s',
                      10, 1, @DB, @ErrorNumber, @ErrorMsg) WITH NOWAIT;
            GOTO NextDatabase;
        END CATCH;

        DECLARE @CountRows int = (SELECT COUNT(*) FROM #StatsWorklist);
        RAISERROR(N'sp_UpdateStats2: %d statistics queued in %s', 10, 1, @CountRows, @DB) WITH NOWAIT;

        ------------------------------------------------------------------------------------------------
        -- Execute / print queued commands
        ------------------------------------------------------------------------------------------------
        DECLARE
              @SchemaName sysname
            , @ObjectName sysname
            , @ObjectType char(2)
            , @StatsName sysname
            , @IndexName sysname
            , @IndexType tinyint
            , @Partitions nvarchar(max)
            , @EffSample int
            , @Cmd nvarchar(max)
            , @With nvarchar(max)
            , @CmdStart datetime2(7)
            , @CmdEnd datetime2(7)
            , @ErrNumber int
            , @ErrMessage nvarchar(max)
            , @CommandLogID int
            , @Ext xml;

        DECLARE statcur CURSOR FAST_FORWARD FOR
            SELECT SchemaName, ObjectName, ObjectType, StatisticsName,
                   IndexName, IndexType, PartitionsToUpdate, EffectiveSample
            FROM #StatsWorklist
            ORDER BY SchemaName, ObjectName, StatisticsName;

        OPEN statcur;
        FETCH NEXT FROM statcur INTO @SchemaName, @ObjectName, @ObjectType, @StatsName, @IndexName, @IndexType, @Partitions, @EffSample;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @TimeLimit IS NOT NULL AND DATEDIFF(SECOND, @StartTime, SYSDATETIME()) >= @TimeLimit
            BEGIN
                RAISERROR(N'sp_UpdateStats2: TimeLimit reached. Stopping.', 10, 1) WITH NOWAIT;
                BREAK;
            END;

            SET @With = N'';

            IF NULLIF(@Partitions, N'') IS NOT NULL
            BEGIN
                -- Incremental stats partition update: requires RESAMPLE ON PARTITIONS
                SET @With = N' WITH RESAMPLE ON PARTITIONS(' + @Partitions + N')';
            END
            ELSE IF @StatisticsResample = N'Y'
            BEGIN
                SET @With = N' WITH RESAMPLE';
            END
            ELSE
            BEGIN
                -- Use explicit sampling rules if @StatisticsSample is NULL and @StatisticsResample='N'
                IF @EffSample IS NULL
                    SET @With = N''; -- should not happen, but safe fallback
                ELSE IF @EffSample >= 100
                    SET @With = N' WITH FULLSCAN';
                ELSE
                    SET @With = N' WITH SAMPLE ' + CAST(@EffSample AS nvarchar(12)) + N' PERCENT';
            END;

            SET @Cmd = N'UPDATE STATISTICS '
                     + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@ObjectName)
                     + N' (' + QUOTENAME(@StatsName) + N')'
                     + @With
                     + N';';

            SET @CmdStart = SYSDATETIME();
            SET @ErrNumber = NULL;
            SET @ErrMessage = NULL;
            SET @CommandLogID = NULL;

            SET @Ext =
            (
                SELECT
                      @EffSample AS [SamplePercent]
                    , @Partitions AS [PartitionsToUpdate]
                FOR XML PATH(N'Statistics'), TYPE
            );

            IF @LogToTable = N'Y'
            BEGIN
                INSERT INTO dbo.CommandLog
                (
                    DatabaseName, SchemaName, ObjectName, ObjectType,
                    IndexName, IndexType, StatisticsName, PartitionNumber,
                    ExtendedInfo, Command, CommandType, StartTime
                )
                VALUES
                (
                    @DB, @SchemaName, @ObjectName, @ObjectType,
                    @IndexName, @IndexType, @StatsName, NULL,
                    @Ext, @Cmd, N'UPDATE_STATISTICS', @CmdStart
                );

                SET @CommandLogID = SCOPE_IDENTITY();
            END;

            IF @Execute = N'Y'
            BEGIN
                BEGIN TRY
                    DECLARE @ExecOuter nvarchar(max) =
                        N'EXEC ' + QUOTENAME(@DB) + N'.sys.sp_executesql @stmt=@stmt;';

                    EXEC sys.sp_executesql
                        @ExecOuter,
                        N'@stmt nvarchar(max)',
                        @stmt = @Cmd;
                END TRY
                BEGIN CATCH
                    SET @ErrNumber = ERROR_NUMBER();
                    SET @ErrMessage = ERROR_MESSAGE();
                    RAISERROR(N'sp_UpdateStats2: Error in %s.%s.%s (%s). Error %d: %s',
                              10, 1, @DB, @SchemaName, @ObjectName, @StatsName, @ErrNumber, @ErrMessage) WITH NOWAIT;
                END CATCH
            END
            ELSE
            BEGIN
                -- Print-only
                RAISERROR(N'%s', 10, 1, @Cmd) WITH NOWAIT;
            END;

            SET @CmdEnd = SYSDATETIME();

            IF @LogToTable = N'Y'
            BEGIN
                UPDATE dbo.CommandLog
                    SET EndTime = @CmdEnd,
                        ErrorNumber = @ErrNumber,
                        ErrorMessage = @ErrMessage
                WHERE ID = @CommandLogID;
            END;

            IF @Delay IS NOT NULL AND @Delay > 0
            BEGIN
                DECLARE @DelayTime time(0) = CAST(DATEADD(SECOND, @Delay, CAST('00:00:00' AS time(0))) AS time(0));
                WAITFOR DELAY @DelayTime;
            END;

            FETCH NEXT FROM statcur INTO @SchemaName, @ObjectName, @ObjectType, @StatsName, @IndexName, @IndexType, @Partitions, @EffSample;
        END;

        CLOSE statcur;
        DEALLOCATE statcur;

        NextDatabase:
        FETCH NEXT FROM dbcur INTO @DB;
    END;

    CLOSE dbcur;
    DEALLOCATE dbcur;

    RAISERROR(N'sp_UpdateStats2: Completed.', 10, 1) WITH NOWAIT;
    RETURN 0;
END
GO
