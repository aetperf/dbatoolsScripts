USE StatsBrokerDB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'mx')
  EXEC('CREATE SCHEMA mx AUTHORIZATION dbo;');
GO

-- Simple CSV splitter (trimmed, ignores empties)
CREATE OR ALTER FUNCTION mx.SplitCsv(@csv NVARCHAR(MAX))
RETURNS TABLE WITH SCHEMABINDING AS
RETURN
WITH s AS (
  SELECT value = LTRIM(RTRIM([value]))
  FROM string_split(COALESCE(@csv, N''), N',')
)
SELECT value FROM s WHERE value <> N'';
GO

DROP TABLE IF EXISTS mx.UpdateStatsLog;
GO

-- Log table
CREATE TABLE mx.UpdateStatsLog
(
  log_id            BIGINT IDENTITY(1,1) PRIMARY KEY,
  log_utc           DATETIME2(3)   NOT NULL DEFAULT (SYSUTCDATETIME()),
  phase             NVARCHAR(30)   NOT NULL,  -- ENQUEUE|DEQUEUE|EXECUTE
  db_name           SYSNAME        NULL,
  schema_name       SYSNAME        NULL,
  table_name        SYSNAME        NULL,
  stats_name        SYSNAME        NULL,
  sample_mode       NVARCHAR(16)   NULL,
  sample_size_pct   INT            NULL,
  adaptive_threshold INT        NULL,
  command			NVARCHAR(max) NULL,
  msg               NVARCHAR(max) NULL,
  error_number      INT            NULL,
  error_severity    INT            NULL,
  error_state       INT            NULL,
  error_line        INT            NULL,
  error_proc        NVARCHAR(800)  NULL
);
GO
