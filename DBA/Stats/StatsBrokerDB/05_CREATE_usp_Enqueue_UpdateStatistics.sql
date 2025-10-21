USE StatsBrokerDB;
GO


CREATE OR ALTER PROCEDURE mx.usp_Enqueue_UpdateStatistics
(
    @databaseName         SYSNAME,
    @MaxSize              BIGINT          = -1,   -- max rows per table, -1 = no limit
    @IncludeTableFilter   NVARCHAR(MAX)   = NULL, -- CSV of LIKE patterns on table name
    @ExcludeTableFilter   NVARCHAR(MAX)   = NULL,
    @IncludeSchema        NVARCHAR(MAX)   = NULL, -- CSV of LIKE patterns on schema name
    @ExcludeTSchemaFilter NVARCHAR(MAX)   = NULL,
    @OnlyModifiedStats    BIT             = 0,    -- only stats with modification
    @ModifiedStatsThreshold INT           = 20,   -- percent (0-100)
    @SampleMode           NVARCHAR(16)    = N'FULLSCAN',  -- FULLSCAN | ADAPTIVE | SAMPLED
    @SampleSize           INT             = 20,   -- percent for sampled/adaptive large tables
    @AdaptiveThreshold    INT             = 1000000    -- rows threshold for ADAPTIVE
)
AS
BEGIN
  SET NOCOUNT ON;

  -- Validate mode
  IF UPPER(@SampleMode) NOT IN (N'FULLSCAN', N'ADAPTIVE', N'SAMPLED')
  BEGIN
    RAISERROR('Invalid @SampleMode. Use FULLSCAN | ADAPTIVE | SAMPLED',16,1);
    RETURN;
  END

  -- Normalize
  SET @SampleMode = UPPER(@SampleMode);

  -- Build pattern tables
  DECLARE @IncTab TABLE (pat NVARCHAR(256));
  INSERT INTO @IncTab(pat) SELECT value FROM mx.SplitCsv(@IncludeTableFilter);

  DECLARE @ExcTab TABLE (pat NVARCHAR(256));
  INSERT INTO @ExcTab(pat) SELECT value FROM mx.SplitCsv(@ExcludeTableFilter);

  DECLARE @IncSch TABLE (pat NVARCHAR(256));
  INSERT INTO @IncSch(pat) SELECT value FROM mx.SplitCsv(@IncludeSchema);

  DECLARE @ExcSch TABLE (pat NVARCHAR(256));
  INSERT INTO @ExcSch(pat) SELECT value FROM mx.SplitCsv(@ExcludeTSchemaFilter);

  -- Temp results: stats to update
  IF OBJECT_ID('tempdb..#todo') IS NOT NULL DROP TABLE #todo;
  CREATE TABLE #todo
  (
    schema_name SYSNAME,
    table_name  SYSNAME,
    stat_name   SYSNAME,
    rows_count  BIGINT,
    mod_pct     DECIMAL(9,2)
  );

  -- Build the list inside target DB and push into our temp table
  DECLARE @sql NVARCHAR(MAX) = N'
USE ' + QUOTENAME(@databaseName) + N';
WITH rows_cte AS
(
  SELECT
    o.object_id,
    rows_count = SUM(CASE WHEN p.index_id IN (0,1) THEN p.[rows] ELSE 0 END)
  FROM sys.objects o
  JOIN sys.partitions p ON p.object_id = o.object_id
  WHERE o.type = ''U'' AND o.is_ms_shipped = 0
  GROUP BY o.object_id
),
stats_cte AS
(
  SELECT
    s.object_id, s.stats_id, s.name AS stat_name,
    o.name AS table_name, SCHEMA_NAME(o.schema_id) AS schema_name,
    rc.rows_count,
    sp.modification_counter,
    sp.[rows] AS stats_rows
  FROM sys.stats s
  JOIN sys.objects o ON o.object_id = s.object_id AND o.type = ''U'' AND o.is_ms_shipped = 0
  LEFT JOIN rows_cte rc ON rc.object_id = s.object_id
  OUTER APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
  WHERE s.is_temporary = 0  -- ignore temps
)
SELECT schema_name, table_name, stat_name, rows_count,
       mod_pct = CASE
                   WHEN stats_rows IS NULL OR stats_rows = 0 THEN 100.00
                   WHEN modification_counter IS NULL THEN 0.00
                   ELSE (100.0 * modification_counter) / NULLIF(stats_rows,0)
                 END
FROM stats_cte
WHERE 1=1
' +
CASE WHEN @MaxSize >= 0 THEN N'  AND (rows_count IS NULL OR rows_count <= ' + CAST(@MaxSize AS NVARCHAR(30)) + N')' ELSE N'' END + N'
;';

  -- Create temp table signature for INSERT..EXEC
  DECLARE @stub TABLE(schema_name SYSNAME, table_name SYSNAME, stat_name SYSNAME, rows_count BIGINT, mod_pct DECIMAL(9,2));
  INSERT @stub EXEC sp_executesql @sql;
  INSERT #todo SELECT * FROM @stub;

  -- Apply include/exclude filters (schema, table)
  -- Include lists mean "must match at least one" if provided.
  IF EXISTS (SELECT 1 FROM @IncSch)
    DELETE t FROM #todo t
    WHERE NOT EXISTS (SELECT 1 FROM @IncSch s WHERE t.schema_name LIKE s.pat ESCAPE N'\');

  IF EXISTS (SELECT 1 FROM @IncTab)
    DELETE t FROM #todo t
    WHERE NOT EXISTS (SELECT 1 FROM @IncTab s WHERE t.table_name LIKE s.pat ESCAPE N'\');

  IF EXISTS (SELECT 1 FROM @ExcSch)
    DELETE t FROM #todo t
    WHERE EXISTS (SELECT 1 FROM @ExcSch s WHERE t.schema_name LIKE s.pat ESCAPE N'\');

  IF EXISTS (SELECT 1 FROM @ExcTab)
    DELETE t FROM #todo t
    WHERE EXISTS (SELECT 1 FROM @ExcTab s WHERE t.table_name LIKE s.pat ESCAPE N'\');

  -- OnlyModifiedStats logic
  IF @OnlyModifiedStats = 1
  BEGIN
    DELETE FROM #todo
    WHERE ISNULL(mod_pct, 0) < @ModifiedStatsThreshold;
  END

  -- Enqueue one message per stat
  DECLARE @dlg UNIQUEIDENTIFIER, @payload XML;

  DECLARE c CURSOR LOCAL FAST_FORWARD FOR
    SELECT schema_name, table_name, stat_name, rows_count FROM #todo;

  DECLARE @sch SYSNAME, @tab SYSNAME, @st SYSNAME, @rows BIGINT;

  OPEN c;
  FETCH NEXT FROM c INTO @sch, @tab, @st, @rows;

  DECLARE @mode NVARCHAR(16) = @SampleMode,
          @pct  INT          = @SampleSize,
          @thr  INT       = @AdaptiveThreshold;

  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @payload = N'<req>
  <db>' + @databaseName + N'</db>
  <schema>' + @sch + N'</schema>
  <table>' + @tab + N'</table>
  <stat>' + @st + N'</stat>
  <mode>' + @mode + N'</mode>
  <samplePct>' + CONVERT(NVARCHAR(10), @pct) + N'</samplePct>
  <adaptiveThres>' + CONVERT(NVARCHAR(20), @thr) + N'</adaptiveThres>
</req>';

    BEGIN DIALOG CONVERSATION @dlg
      FROM SERVICE [mx.UpdateStatsService]
      TO SERVICE   N'mx.UpdateStatsService'
      ON CONTRACT  [mx//UpdateStatsContract]
      WITH ENCRYPTION = OFF, LIFETIME = 600; -- 10 minutes

    SEND ON CONVERSATION @dlg
      MESSAGE TYPE [mx//UpdateStatsRequest] (@payload);

    END CONVERSATION @dlg;

    INSERT INTO mx.UpdateStatsLog(phase, db_name, schema_name, table_name, stats_name, msg)
    VALUES (N'ENQUEUE', @databaseName, @sch, @tab, @st, N'Sent');

    FETCH NEXT FROM c INTO @sch, @tab, @st, @rows;
  END

  CLOSE c; DEALLOCATE c;
END
GO
