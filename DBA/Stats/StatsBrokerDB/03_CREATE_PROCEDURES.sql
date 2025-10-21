USE [StatsBrokerDB]
GO


--------------------------------------------------------------------------------------
-- Procedure: usp_WakeWorkers
-- Purpose:   Send wake messages to BrokerActivationService to start workers
--------------------------------------------------------------------------------------

CREATE OR ALTER   PROCEDURE [mx].[usp_WakeWorkers]
(
  @Count INT = 1   -- number of wake messages to send
)
AS
BEGIN
  SET NOCOUNT ON;
  IF @Count IS NULL OR @Count < 1 SET @Count = 1;

  DECLARE @i INT = 0, @h UNIQUEIDENTIFIER;

  WHILE @i < @Count
  BEGIN
    BEGIN DIALOG CONVERSATION @h
      FROM SERVICE [mx.BrokerActivationService]
      TO SERVICE   N'mx.BrokerActivationService'
      ON CONTRACT  [mx//WakeContract]
      WITH ENCRYPTION = OFF, LIFETIME = 600;

    SEND ON CONVERSATION @h
      MESSAGE TYPE [mx//Wake] (CAST(N'<wake/>' AS VARBINARY(MAX)));

    -- NOTE: no END CONVERSATION here; target will close it.
    SET @i += 1;
  END

  INSERT INTO mx.UpdateStatsLog(phase, msg)
  VALUES (N'ACTIVATION', CONCAT('Sent ', @Count, ' wake message(s)'));
END
GO



--------------------------------------------------------------------------------------
-- Procedure: usp_Enqueue_UpdateStatistics
-- Purpose:   Enqueue statistics update work items for a given database
--------------------------------------------------------------------------------------


CREATE OR ALTER   PROCEDURE [mx].[usp_Enqueue_UpdateStatistics]
(
    @databaseName           SYSNAME,
    @MaxSize                BIGINT          = -1,            -- max rows per table; -1 = no limit
    @IncludeTableFilter     NVARCHAR(MAX)   = NULL,          -- CSV LIKE patterns on table
    @ExcludeTableFilter     NVARCHAR(MAX)   = NULL,
    @IncludeSchema          NVARCHAR(MAX)   = NULL,          -- CSV LIKE patterns on schema
    @ExcludeTSchemaFilter   NVARCHAR(MAX)   = NULL,          -- (kept spelling per your spec)
    @OnlyModifiedStats      BIT             = 0,
    @ModifiedStatsThreshold INT             = 20,
    @SampleMode             NVARCHAR(16)    = N'FULLSCAN',   -- FULLSCAN | ADAPTIVE | SAMPLED
    @SampleSize             INT             = 20,
    @AdaptiveThreshold      BIGINT          = 1000000,
    @RunId                  UNIQUEIDENTIFIER = NULL,         -- optional grouping
    @DefaultPriority        INT              = 100,
    @WakeReaders            INT              = NULL          -- optional: wake N workers
)
AS
BEGIN
  SET NOCOUNT ON;

  IF UPPER(@SampleMode) NOT IN (N'FULLSCAN',N'ADAPTIVE',N'SAMPLED')
    THROW 50001, 'Invalid @SampleMode. Use FULLSCAN | ADAPTIVE | SAMPLED', 1;

  SET @SampleMode = UPPER(@SampleMode);
  IF @RunId IS NULL SET @RunId = NEWID();

  DECLARE @IncTab TABLE (pat NVARCHAR(256));
  INSERT INTO @IncTab SELECT value FROM mx.SplitCsv(@IncludeTableFilter);

  DECLARE @ExcTab TABLE (pat NVARCHAR(256));
  INSERT INTO @ExcTab SELECT value FROM mx.SplitCsv(@ExcludeTableFilter);

  DECLARE @IncSch TABLE (pat NVARCHAR(256));
  INSERT INTO @IncSch SELECT value FROM mx.SplitCsv(@IncludeSchema);

  DECLARE @ExcSch TABLE (pat NVARCHAR(256));
  INSERT INTO @ExcSch SELECT value FROM mx.SplitCsv(@ExcludeTSchemaFilter);

  CREATE TABLE #tempfilter(schema_name SYSNAME, table_name SYSNAME, rows_count BIGINT);

  DECLARE @sql NVARCHAR(MAX) = N'
USE ' + QUOTENAME(@databaseName) + N';
WITH rows_cte AS
(
  SELECT o.object_id,
         rows_count = SUM(CASE WHEN p.index_id IN (0,1) THEN p.[rows] ELSE 0 END)
  FROM sys.objects o
  JOIN sys.partitions p ON p.object_id = o.object_id
  WHERE o.type = ''U'' AND o.is_ms_shipped = 0
  GROUP BY o.object_id
)
SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name AS table_name, rows_count
FROM sys.objects o
LEFT JOIN rows_cte r ON r.object_id = o.object_id
WHERE o.type = ''U'' AND o.is_ms_shipped = 0 ' +
  CASE WHEN @MaxSize >= 0
       THEN N' AND (rows_count IS NULL OR rows_count <= ' + CONVERT(NVARCHAR(30),@MaxSize) + N')'
       ELSE N'' END + N';';

  DECLARE @stub TABLE(schema_name SYSNAME, table_name SYSNAME, rows_count BIGINT);
  INSERT @stub EXEC sp_executesql @sql;
  INSERT #tempfilter SELECT * FROM @stub;

  -- Apply schema/table filters
  IF EXISTS (SELECT 1 FROM @IncSch)
    DELETE t FROM #tempfilter t WHERE NOT EXISTS (SELECT 1 FROM @IncSch s WHERE t.schema_name LIKE s.pat ESCAPE N'\');
  
  IF EXISTS (SELECT 1 FROM @IncTab)
    DELETE t FROM #tempfilter t WHERE NOT EXISTS (SELECT 1 FROM @IncTab s WHERE t.table_name LIKE s.pat ESCAPE N'\');

  IF EXISTS (SELECT 1 FROM @ExcSch)
    DELETE t FROM #tempfilter t WHERE EXISTS (SELECT 1 FROM @ExcSch s WHERE t.schema_name LIKE s.pat ESCAPE N'\');

  IF EXISTS (SELECT 1 FROM @ExcTab)
    DELETE t FROM #tempfilter t WHERE EXISTS (SELECT 1 FROM @ExcTab s WHERE t.table_name LIKE s.pat ESCAPE N'\');

  INSERT INTO mx.WorkQueue (run_id, db_name, schema_name, table_name,
                            sample_mode, sample_pct, adaptive_thres, only_modified, mod_threshold, priority)
  SELECT @RunId, @databaseName, schema_name, table_name,
         @SampleMode, @SampleSize, @AdaptiveThreshold, @OnlyModifiedStats, @ModifiedStatsThreshold, @DefaultPriority
  FROM #tempfilter;

  DECLARE @rows INT = @@ROWCOUNT;

  INSERT INTO mx.UpdateStatsLog(run_id, phase, db_name, msg)
  VALUES (@RunId, N'ENQUEUE', @databaseName, CONCAT('Queued ', @rows, ' table(s)'));

  -- Optionally wake workers immediately
  IF @WakeReaders IS NOT NULL AND @WakeReaders > 0
    EXEC mx.usp_WakeWorkers @Count = @WakeReaders;

END
GO


--------------------------------------------------------------------------------------
-- Procedure: usp_Worker_Tick 
-- Purpose:   Worker procedure to claim and process work items from WorkQueue
--------------------------------------------------------------------------------------

CREATE OR ALTER   PROCEDURE [mx].[usp_Worker_Tick]
(
  @MaxItems INT = 1
)
AS
BEGIN
  SET NOCOUNT ON;

  DECLARE @cnt INT = 0;

  WHILE @cnt < @MaxItems
  BEGIN
    -- Buffer claimed row
    DECLARE @picked TABLE
    (
      work_id        BIGINT,
      db_name        SYSNAME,
      schema_name    SYSNAME,
      table_name     SYSNAME,
      sample_mode    NVARCHAR(16),
      sample_pct     INT,
      adaptive_thres BIGINT,
      only_modified  BIT,
      mod_threshold  INT,
      channel_key    NVARCHAR(512),
      run_id         UNIQUEIDENTIFIER
    );

    DECLARE
      @work_id BIGINT, @db SYSNAME, @sch SYSNAME, @tab SYSNAME, @mode NVARCHAR(16),
      @pct INT, @thr BIGINT, @only BIT, @mod INT, @channel NVARCHAR(512), @run UNIQUEIDENTIFIER;

    -- CLAIM (short, non-blocking)
    BEGIN TRAN;

      ;WITH c AS
      (
        SELECT TOP(1)
          work_id, db_name, schema_name, table_name, sample_mode, sample_pct,
          adaptive_thres, only_modified, mod_threshold, channel_key, run_id,locked_at,locked_by
        FROM mx.WorkQueue WITH (READPAST, UPDLOCK, ROWLOCK)
        WHERE locked_at IS NULL AND done_at IS NULL
        ORDER BY priority, work_id
      )
      UPDATE c
        SET locked_at = SYSUTCDATETIME(),
            locked_by = CONCAT(COALESCE(HOST_NAME(),''),'|',COALESCE(SUSER_SNAME(),''),'|',@@SPID)
        OUTPUT inserted.work_id, inserted.db_name, inserted.schema_name, inserted.table_name,
               inserted.sample_mode, inserted.sample_pct, inserted.adaptive_thres,
               inserted.only_modified, inserted.mod_threshold, inserted.channel_key, inserted.run_id
          INTO @picked;

    SELECT TOP(1)
      @work_id = work_id, @db = db_name, @sch = schema_name, @tab = table_name,
      @mode = sample_mode, @pct = sample_pct, @thr = adaptive_thres,
      @only = only_modified, @mod = mod_threshold, @channel = channel_key, @run = run_id
    FROM @picked;

    IF @work_id IS NULL
    BEGIN
      ROLLBACK TRAN;
      BREAK; -- no work
    END

    -- Reserve channel (1 worker per table)
    BEGIN TRY
      INSERT INTO mx.ActiveChannel(channel_key, work_id, holder)
      VALUES(@channel, @work_id, CONCAT(SUSER_SNAME(),':',@@SPID));
      COMMIT TRAN;
    END TRY
    BEGIN CATCH
      ROLLBACK TRAN;
      UPDATE mx.WorkQueue SET locked_at = NULL, locked_by = NULL
      WHERE work_id = @work_id AND done_at IS NULL;
      CONTINUE;
    END CATCH

    -- Build dynamic SQL for this table (serial per-stat updates)
    DECLARE @execsql NVARCHAR(MAX) = N'
USE ' + QUOTENAME(@db) + N';
DECLARE @schema SYSNAME = N''' + REPLACE(@sch,'''','''''') + N''';
DECLARE @table  SYSNAME = N''' + REPLACE(@tab,'''','''''') + N''';

DECLARE @rows BIGINT =
 (SELECT SUM(p.[rows]) FROM sys.partitions p
  WHERE p.object_id = OBJECT_ID(QUOTENAME(@schema)+''.''+QUOTENAME(@table))
    AND p.index_id IN (0,1));

DECLARE @directive NVARCHAR(200) =
  CASE
    WHEN UPPER(N''' + @mode + N''') = N''FULLSCAN'' THEN N''FULLSCAN''
    WHEN UPPER(N''' + @mode + N''') = N''SAMPLED''  THEN N''SAMPLE ' + CONVERT(NVARCHAR(10),@pct) + N' PERCENT''
    WHEN UPPER(N''' + @mode + N''') = N''ADAPTIVE'' THEN
      CASE WHEN @rows IS NULL OR @rows <= ' + CONVERT(NVARCHAR(20),@thr) + N'
           THEN N''FULLSCAN''
           ELSE N''SAMPLE ' + CONVERT(NVARCHAR(10),@pct) + N' PERCENT'' END
    ELSE N''FULLSCAN''
  END;

IF OBJECT_ID(''tempdb..#stats'') IS NOT NULL DROP TABLE #stats;
CREATE TABLE #stats(stat_name SYSNAME);

WITH s AS
(
  SELECT s.stats_id, s.name AS stat_name
  FROM sys.stats s
  WHERE s.object_id = OBJECT_ID(QUOTENAME(@schema)+''.''+QUOTENAME(@table))
    AND s.is_temporary = 0
),
sp AS
(
  SELECT s.stats_id, s.stat_name, dps.[rows] AS stats_rows, dps.modification_counter
  FROM s OUTER APPLY sys.dm_db_stats_properties(OBJECT_ID(QUOTENAME(@schema)+''.''+QUOTENAME(@table)), s.stats_id) dps
)
INSERT #stats(stat_name)
SELECT stat_name
FROM sp
' + CASE WHEN @only = 1
         THEN N'WHERE ISNULL(CASE WHEN stats_rows IS NULL OR stats_rows = 0 THEN 100.0
                                   WHEN modification_counter IS NULL THEN 0.0
                                   ELSE (100.0*modification_counter)/NULLIF(stats_rows,0) END,0.0) >= ' + CONVERT(NVARCHAR(10),@mod) + NCHAR(10)
         ELSE N'' END + N';

DECLARE @stat SYSNAME;
DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT stat_name FROM #stats ORDER BY stat_name;
OPEN c; FETCH NEXT FROM c INTO @stat;
WHILE @@FETCH_STATUS = 0
BEGIN
  DECLARE @cmd NVARCHAR(MAX) = N''UPDATE STATISTICS '' + QUOTENAME(@schema)+''.''+QUOTENAME(@table)
                                + N'' '' + QUOTENAME(@stat) + N'' WITH '' + @directive + N'';'';
  EXEC (@cmd);
  FETCH NEXT FROM c INTO @stat;
END
CLOSE c; DEALLOCATE c;';

    DECLARE @t0 DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @elapsed BIGINT;

    BEGIN TRY
      EXEC (@execsql);
      SET @elapsed = DATEDIFF(MILLISECOND, @t0, SYSUTCDATETIME());

      UPDATE mx.WorkQueue
      SET done_at = SYSUTCDATETIME(), err_msg = NULL
      WHERE work_id = @work_id;

      INSERT INTO mx.UpdateStatsLog(run_id, phase, db_name, schema_name, table_name, msg, sqlcommand, ElapsedMS)
      VALUES (@run, N'DONE', @db, @sch, @tab, CONCAT('OK (', @mode, ')'), @execsql, @elapsed);
    END TRY
    BEGIN CATCH
      SET @elapsed = DATEDIFF(MILLISECOND, @t0, SYSUTCDATETIME());

      UPDATE mx.WorkQueue
      SET done_at = SYSUTCDATETIME(),
          err_msg = ERROR_MESSAGE()
      WHERE work_id = @work_id;

      INSERT INTO mx.UpdateStatsLog
        (run_id, phase, db_name, schema_name, table_name, msg, error_number, error_message, sqlcommand, ElapsedMS)
      VALUES
        (@run, N'ERROR', @db, @sch, @tab, N'Failed', ERROR_NUMBER(), ERROR_MESSAGE(), @execsql, @elapsed);
    END CATCH

    -- Release channel
    DELETE FROM mx.ActiveChannel WHERE channel_key = @channel AND work_id = @work_id;

    SET @cnt += 1;
  END
  RETURN @cnt;
END
GO


---------------------------------------------------------------------------------------
-- Procedure: usp_BrokerActivationWorker
-- Purpose:   Service Broker activation procedure to process work items
---------------------------------------------------------------------------------------

CREATE OR ALTER   PROCEDURE [mx].[usp_BrokerActivationWorker]
WITH EXECUTE AS OWNER
AS
BEGIN
  SET NOCOUNT ON;

  DECLARE @h UNIQUEIDENTIFIER, @mt SYSNAME, @body VARBINARY(MAX);
  

  WHILE (1=1)
  BEGIN
  SET @h = NULL; SET @mt = NULL; SET @body = NULL;
    WAITFOR (
      RECEIVE TOP(1)
        @h   = conversation_handle,
        @mt  = message_type_name,
        @body= message_body
      FROM mx.BrokerActivationQueue
    ), TIMEOUT 4000;

    IF @h IS NULL BREAK;

    BEGIN TRY
      IF @mt IN (N'http://schemas.microsoft.com/SQL/ServiceBroker/Error',
                 N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog')
      BEGIN
        BEGIN TRY END CONVERSATION @h; END TRY
        BEGIN CATCH IF ERROR_NUMBER() NOT IN (8426,8429) THROW; END CATCH;
        CONTINUE;
      END;

      -- Drain multiple tables in this activation session:
      DECLARE @did INT = 1;
      WHILE (@did > 0)
      BEGIN
        EXEC @did = mx.usp_Worker_Tick @MaxItems = 1;  -- returns 1 when it processed a table, 0 when none
      END

      BEGIN TRY END CONVERSATION @h; END TRY
      BEGIN CATCH IF ERROR_NUMBER() NOT IN (8426,8429) THROW; END CATCH;
    END TRY
    BEGIN CATCH
      INSERT INTO mx.UpdateStatsLog(phase, msg, error_number, error_message)
      VALUES (N'ACTIVATION', N'Worker activation failed', ERROR_NUMBER(), ERROR_MESSAGE());

      IF @h IS NOT NULL
      BEGIN
        BEGIN TRY END CONVERSATION @h; END TRY
        BEGIN CATCH IF ERROR_NUMBER() NOT IN (8426,8429) CONTINUE ; END CATCH;
      END
    END CATCH;

    SET @h = NULL; SET @mt = NULL; SET @body = NULL;
  END
END
GO











