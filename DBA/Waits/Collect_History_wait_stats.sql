/***********************************************
    Create the historical table
***********************************************/

USE DBATOOLS;
GO

-- Create the history table if it does not exist
IF OBJECT_ID('dbo.WaitStatsHistory') IS NULL
BEGIN
    CREATE TABLE dbo.WaitStatsHistory
    (
         SqlServerStartTime DATETIME NOT NULL
        ,CollectionTime DATETIME NOT NULL
        ,TimeDiff_ms BIGINT NOT NULL
        ,WaitType NVARCHAR(60) NOT NULL
        ,WaitingTasksCountCumulative BIGINT NOT NULL
        ,WaitingTasksCountDiff BIGINT NOT NULL
        ,WaitTimeCumulative_ms BIGINT NOT NULL
        ,WaitTimeDiff_ms BIGINT NOT NULL
        ,MaxWaitTime_ms BIGINT NOT NULL
        ,SignalWaitTimeCumulative_ms BIGINT NOT NULL
        ,SignalWaitTimeDiff_ms BIGINT NOT NULL
        ,CONSTRAINT PK_WaitStatsHistory PRIMARY KEY CLUSTERED (CollectionTime, WaitType)
    )WITH (DATA_COMPRESSION = NONE);
END
GO

CREATE INDEX IX_MAX_SqlServerStartTime ON dbo.WaitStatsHistory(SqlServerStartTime);
CREATE INDEX IX_MAX_CollectionTime ON dbo.WaitStatsHistory(CollectionTime);

/***********************************************************
    Create Script for job creation and Schedule every 10min
************************************************************/

USE [msdb]
GO
DECLARE @jobId BINARY(16)
EXEC  msdb.dbo.sp_add_job @job_name=N'dbatools_collect_wait_stats', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
select @jobId
GO
EXEC msdb.dbo.sp_add_jobserver @job_name=N'dbatools_collect_wait_stats'
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_add_jobstep @job_name=N'dbatools_collect_wait_stats', @step_name=N'Collect Wait Stats', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_fail_action=2, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DECLARE
     @CurrentSqlServerStartTime DATETIME
    ,@PreviousSqlServerStartTime DATETIME
    ,@PreviousCollectionTime DATETIME;

SELECT @CurrentSqlServerStartTime = sqlserver_start_time FROM sys.dm_os_sys_info;

-- Get the last collection time
SELECT
     @PreviousSqlServerStartTime = MAX(SqlServerStartTime)
    ,@PreviousCollectionTime = MAX(CollectionTime)
FROM dbo.WaitStatsHistory;

IF @CurrentSqlServerStartTime <> ISNULL(@PreviousSqlServerStartTime,0)
BEGIN
    -- Insert starter values if SQL Server has been recently restarted
    INSERT INTO dbo.WaitStatsHistory
    SELECT
         @CurrentSqlServerStartTime
        ,GETDATE()
        ,cast(DATEDIFF(S,@CurrentSqlServerStartTime,GETDATE()) as bigint)*1000
        ,wait_type
        ,waiting_tasks_count
        ,0
        ,wait_time_ms
        ,0
        ,max_wait_time_ms
        ,signal_wait_time_ms
        ,0
    FROM sys.dm_os_wait_stats;
END
ELSE
BEGIN
    -- Get the current wait stats
    WITH CurrentWaitStats AS
    (
        SELECT GETDATE() AS ''CollectionTime'',* FROM sys.dm_os_wait_stats
    )
    -- Insert the diff values into the history table
    INSERT dbo.WaitStatsHistory
    SELECT
         @CurrentSqlServerStartTime
        ,cws.CollectionTime
        ,DATEDIFF(MS,@PreviousCollectionTime,cws.CollectionTime)
        ,cws.wait_type
        ,cws.waiting_tasks_count
        ,cws.waiting_tasks_count - COALESCE(hist.WaitingTasksCountCumulative,0)
        ,cws.wait_time_ms
        ,cws.wait_time_ms - COALESCE(hist.WaitTimeCumulative_ms,0)
        ,cws.max_wait_time_ms
        ,cws.signal_wait_time_ms
        ,cws.signal_wait_time_ms - COALESCE(hist.SignalWaitTimeCumulative_ms,0)
    FROM CurrentWaitStats cws LEFT OUTER JOIN dbo.WaitStatsHistory hist
        ON cws.wait_type = hist.WaitType
        AND hist.CollectionTime = @PreviousCollectionTime;
END

/************************************************************/
/* PURGE unused waittypeSUNDAY 1am	*/
/************************************************************/

IF(DATEPART(dw, GETDATE()-3)=7 AND DATEPART(HOUR, GETDATE())=1)BEGIN
    SELECT [WaitType]
    INTO [#WaitStatsHistory_unused_waittype]
    FROM [dbo].[WaitStatsHistory]
    GROUP BY [WaitType]
    HAVING SUM([WaitTimeDiff_ms])=0;

    DELETE p
    FROM [dbo].[WaitStatsHistory] p
         INNER JOIN [#WaitStatsHistory_unused_waittype] uw ON uw.[WaitType]=p.[WaitType];

    DROP TABLE [#WaitStatsHistory_unused_waittype];
END;
GO', 
		@database_name=N'DBATOOLS', 
		@flags=4
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_update_job @job_name=N'dbatools_collect_wait_stats', 
		@enabled=1, 
		@start_step_id=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'', 
		@notify_page_operator_name=N''
GO
USE [msdb]
GO
DECLARE @schedule_id int
EXEC msdb.dbo.sp_add_jobschedule @job_name=N'dbatools_collect_wait_stats', @name=N'Every_30minutes', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=30, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20180328, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, @schedule_id = @schedule_id OUTPUT
select @schedule_id
GO
