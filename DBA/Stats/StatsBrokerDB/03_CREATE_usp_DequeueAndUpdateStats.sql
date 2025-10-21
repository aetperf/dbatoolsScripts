USE StatsBrokerDB;
GO

CREATE OR ALTER PROCEDURE mx.usp_DequeueAndUpdateStats
WITH EXECUTE AS OWNER   -- make sure OWNER can update stats on target DBs
AS
BEGIN
  SET NOCOUNT ON;
  DECLARE @conv UNIQUEIDENTIFIER,
          @mt   SYSNAME,
          @body XML;

  WHILE (1=1)
  BEGIN
    WAITFOR (
      RECEIVE TOP(1)
         @conv = conversation_handle,
         @mt   = message_type_name,
         @body = CAST(message_body AS XML)
      FROM mx.UpdateStatsQueue
    ), TIMEOUT 5000;

    IF @conv IS NULL
      BREAK;

    BEGIN TRY
      IF @mt = N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
      BEGIN
        END CONVERSATION @conv;
        CONTINUE;
      END;

      IF @mt <> N'mx//UpdateStatsRequest'
      BEGIN
        -- Unexpected; just end to avoid poison
        END CONVERSATION @conv;
        CONTINUE;
      END;

      DECLARE
        @TargetDb SYSNAME      = @body.value('(/req/db/text())[1]',       'SYSNAME'),
        @Sch      SYSNAME      = @body.value('(/req/schema/text())[1]',   'SYSNAME'),
        @Tab      SYSNAME      = @body.value('(/req/table/text())[1]',    'SYSNAME'),
        @Stat     SYSNAME      = @body.value('(/req/stat/text())[1]',     'SYSNAME'),
        @Mode     NVARCHAR(16) = @body.value('(/req/mode/text())[1]',     'NVARCHAR(16)'),
        @Pct      INT          = @body.value('(/req/samplePct/text())[1]','INT'),
        @Thres    INT          = @body.value('(/req/adaptiveThres/text())[1]','INT');

      DECLARE @sql NVARCHAR(MAX) =
N'USE ' + QUOTENAME(@TargetDb) + N';
DECLARE @u NVARCHAR(MAX) = N''UPDATE STATISTICS '
+ QUOTENAME(@Sch) + N'.' + QUOTENAME(@Tab) + N' ' + QUOTENAME(@Stat) + N' WITH ';
IF @Mode = N'FULLSCAN'
  SET @sql += N'FULLSCAN'';';
ELSE IF @Mode = N'SAMPLED'
  SET @sql += N'SAMPLE ' + CAST(@Pct AS NVARCHAR(10)) + N' PERCENT'';';
ELSE IF @Mode = N'ADAPTIVE'
BEGIN
  -- ADAPTIVE: lookup table rowcount, choose fullscan if <= threshold
  DECLARE @presql nvarchar(max);
  SET @presql += N'
  DECLARE @mode nvarchar(max);
DECLARE @rows BIGINT =
  (SELECT SUM(p.[rows]) FROM sys.partitions p
   JOIN sys.objects o ON o.object_id=p.object_id
   WHERE o.object_id = OBJECT_ID(N''' + QUOTENAME(@Sch) + N'.' + QUOTENAME(@Tab) + N''') AND p.index_id IN (0,1));
IF (@rows IS NULL OR @rows <= ' + CAST(@Thres AS NVARCHAR(20)) + N')
  SET @mode += N''FULLSCAN'';
ELSE
  SET @mode += N''SAMPLE ' + CAST(@Pct AS NVARCHAR(10)) + N' PERCENT'';';
END

     SET @sql = @presql + @sql

      
      SET @sql += N'SET @u+= @mode;EXEC (@u);';
      
      EXEC (@sql);

      INSERT INTO mx.UpdateStatsLog(phase,db_name,schema_name,table_name,stats_name,sample_mode,sample_size_pct,adaptive_threshold,command,msg)
      VALUES (N'EXECUTE', @TargetDb,@Sch,@Tab,@Stat,@Mode,@Pct,@Thres,@sql, N'OK');

      END CONVERSATION @conv;
    END TRY
    BEGIN CATCH
      INSERT INTO mx.UpdateStatsLog
      (
        phase, db_name, schema_name, table_name, stats_name,command, msg,
        error_number, error_severity, error_state, error_line, error_proc
      )
      VALUES
      (
        N'DEQUEUE',
        @body.value('(/req/db/text())[1]','SYSNAME'),
        @body.value('(/req/schema/text())[1]','SYSNAME'),
        @body.value('(/req/table/text())[1]','SYSNAME'),
        @body.value('(/req/stat/text())[1]','SYSNAME'),
        @sql,
        ERROR_MESSAGE(), ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_LINE(), ERROR_PROCEDURE()
      );

      -- End this conversation to prevent poison-loop
      IF @conv IS NOT NULL
        END CONVERSATION @conv;
    END CATCH;

    -- Reset locals
    SET @conv = NULL; SET @mt = NULL; SET @body = NULL;
  END
END
GO
PRINT 'Created mx.usp_DequeueAndUpdateStats procedure.';