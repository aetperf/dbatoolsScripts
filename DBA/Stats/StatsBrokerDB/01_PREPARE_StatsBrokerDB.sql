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

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[mx].[UpdateStatsLog]') AND type in (N'U'))
DROP TABLE [mx].[UpdateStatsLog]


CREATE TABLE [mx].[UpdateStatsLog](
	[log_id] [bigint] IDENTITY(1,1) NOT NULL,
	[log_utc] [datetime2](3) NOT NULL,
	[run_id] [uniqueidentifier] NULL,
	[phase] [nvarchar](30) NOT NULL,
	[db_name] [sysname] NULL,
	[schema_name] [sysname] NULL,
	[table_name] [sysname] NULL,
	[msg] [nvarchar](4000) NULL,
	[error_number] [int] NULL,
	[error_message] [nvarchar](4000) NULL,
	[sqlcommand] [nvarchar](max) NULL,
	[ElapsedMS] [bigint] NULL,
PRIMARY KEY CLUSTERED 
(
	[log_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [mx].[UpdateStatsLog] ADD  DEFAULT (sysutcdatetime()) FOR [log_utc]
GO

ALTER TABLE [mx].[UpdateStatsLog] ADD  CONSTRAINT [DF_UpdateStatsLog_ElapsedMS]  DEFAULT ((0)) FOR [ElapsedMS]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[mx].[ActiveChannel]') AND type in (N'U'))
DROP TABLE [mx].[ActiveChannel]
GO

CREATE TABLE [mx].[ActiveChannel](
	[channel_key] [nvarchar](512) NOT NULL,
	[work_id] [bigint] NOT NULL,
	[holder] [nvarchar](128) NOT NULL,
	[locked_at] [datetime2](3) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[channel_key] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [mx].[ActiveChannel] ADD  DEFAULT (sysutcdatetime()) FOR [locked_at]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[mx].[WorkQueue]') AND type in (N'U'))
DROP TABLE [mx].[WorkQueue]
GO

CREATE TABLE [mx].[WorkQueue](
	[work_id] [bigint] IDENTITY(1,1) NOT NULL,
	[run_id] [uniqueidentifier] NOT NULL,
	[db_name] [sysname] NOT NULL,
	[schema_name] [sysname] NOT NULL,
	[table_name] [sysname] NOT NULL,
	[sample_mode] [nvarchar](16) NOT NULL,
	[sample_pct] [int] NULL,
	[adaptive_thres] [bigint] NULL,
	[only_modified] [bit] NOT NULL,
	[mod_threshold] [int] NULL,
	[priority] [int] NOT NULL,
	[created_at] [datetime2](3) NOT NULL,
	[locked_at] [datetime2](3) NULL,
	[locked_by] [nvarchar](128) NULL,
	[done_at] [datetime2](3) NULL,
	[err_msg] [nvarchar](4000) NULL,
	[channel_key]  AS (CONVERT([nvarchar](512),(((quotename([db_name])+N'.')+quotename([schema_name]))+N'.')+quotename([table_name]))) PERSISTED,
	[status]  AS (case when [locked_at] IS NULL AND [done_at] IS NULL then 'Queued' when [locked_at] IS NOT NULL AND [done_at] IS NULL then 'Running' when [done_at] IS NOT NULL AND [err_msg] IS NOT NULL then 'Failed' when [done_at] IS NOT NULL then 'Done'  end) PERSISTED,
PRIMARY KEY CLUSTERED 
(
	[work_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [mx].[WorkQueue] ADD  DEFAULT (newid()) FOR [run_id]
GO

ALTER TABLE [mx].[WorkQueue] ADD  DEFAULT ((0)) FOR [only_modified]
GO

ALTER TABLE [mx].[WorkQueue] ADD  DEFAULT ((100)) FOR [priority]
GO

ALTER TABLE [mx].[WorkQueue] ADD  DEFAULT (sysutcdatetime()) FOR [created_at]
GO


