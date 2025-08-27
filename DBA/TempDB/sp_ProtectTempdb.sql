

IF OBJECT_ID('dbo.sp_ProtectTempdb') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_ProtectTempdb AS RETURN 0;');
GO

IF OBJECT_ID('dbo.sp_GetSessionPlan') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_GetSessionPlan AS RETURN 0;');
GO

/*Log Table*/
IF OBJECT_ID('dbo.ProtectTempdbLog', 'U') IS NULL
CREATE TABLE [dbo].[ProtectTempdbLog](
	[SessionId] [int] NULL,
    [DatabaseName] [sysname] NULL,
	[LoginName] [nvarchar](256) NULL,
	[ProgramName] [nvarchar](1000) NULL,
	[RunningUserSpaceMB] [numeric](10, 1) NULL,
	[RunningInternalSpaceMB] [numeric](10, 1) NULL,
	[ThresholdMB] [numeric](10, 1) NULL,
	[BatchText] [nvarchar](max) NULL,
	[StatementText] [nvarchar](max) NULL,
	[StatementPlan] [xml] NULL,
	[ExecutionDateTime] [datetime] NULL DEFAULT (getdate())
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO







ALTER   PROCEDURE [dbo].[sp_GetSessionPlan]
    @session_id      SMALLINT,
    @batch_text_out  NVARCHAR(MAX) OUTPUT,
    @stmt_text_out   NVARCHAR(MAX) OUTPUT,
    @plan_xml_out    XML OUTPUT,
    @actual          BIT = 1 -- 0 = estimated (compilé), 1 = actual (en cours si possible)
AS
BEGIN
    /*
      Need : VIEW SERVER STATE.
      Usage :
        DECLARE @batch NVARCHAR(MAX), @stmt NVARCHAR(MAX), @plan XML;
        EXEC dbo.sp_GetSessionPlan
             @session_id = 53,
             @batch_text_out = @batch OUTPUT,
             @stmt_text_out  = @stmt  OUTPUT,
             @plan_xml_out   = @plan  OUTPUT
        SELECT @batch AS BatchText, @stmt AS StatementText, @plan AS PlanXml;
    */
    SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	DECLARE @is_actualplan bit = @actual;

    ;WITH req AS
    (
        SELECT TOP (1)
            r.session_id,
            r.request_id,
            r.plan_handle,
            r.sql_handle,
            r.statement_start_offset,
            r.statement_end_offset
        FROM sys.dm_exec_requests AS r
        WHERE r.session_id = @session_id
        ORDER BY r.request_id
    )
    SELECT
        @batch_text_out = st.text,
        @stmt_text_out  =
            CASE
                WHEN req.statement_start_offset IS NULL THEN st.text
                ELSE SUBSTRING(
                        st.text,
                        (req.statement_start_offset / 2) + 1,
                        CASE req.statement_end_offset
                            WHEN -1 THEN (DATALENGTH(st.text) / 2) - (req.statement_start_offset / 2) + 1
                            ELSE (req.statement_end_offset - req.statement_start_offset) / 2 + 1
                        END
                     )
            END
    FROM req
    CROSS APPLY sys.dm_exec_sql_text(req.sql_handle) AS st;

    IF @batch_text_out IS NULL
    BEGIN
        RAISERROR('Aucune requête en cours pour session_id %d, ou permissions insuffisantes (VIEW SERVER STATE).', 16, 1, @session_id);
        RETURN;
    END

    -- Get Plan
        IF @actual = 1
    BEGIN
        -- Plan ACTUEL (avec stats runtime) pour la session en cours d’exécution
        SELECT @plan_xml_out = qsx.query_plan
        FROM sys.dm_exec_requests r
        CROSS APPLY sys.dm_exec_query_statistics_xml(r.session_id) AS qsx
        WHERE r.session_id = @session_id;

        -- Si indisponible (session idle, version non supportée, etc.), fallback estimé
        IF @plan_xml_out IS NULL
        BEGIN
            SELECT @plan_xml_out = qp.query_plan
            FROM sys.dm_exec_requests r
            CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) AS qp
            WHERE r.session_id = @session_id;
        END
    END
    ELSE
    BEGIN
        -- Plan ESTIMÉ (compilé)
        SELECT @plan_xml_out = qp.query_plan
        FROM sys.dm_exec_requests r
        CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) AS qp
        WHERE r.session_id = @session_id;
    END

    SELECT
        @session_id       AS session_id,
        @batch_text_out   AS batch_text,
        @stmt_text_out    AS statement_text,
        @plan_xml_out     AS execution_plan_xml,
        @is_actualplan    AS is_actualplan;
END
GO

ALTER   PROCEDURE [dbo].[sp_ProtectTempdb]
--Use @Help=1 to see the parameters definitions
    @UsageTempDb DECIMAL(18,2) = 0.5
    , @IncludeLogin VARCHAR(MAX) = NULL
    , @ExcludeLogin VARCHAR(MAX) = NULL
    , @IncludeProgramName VARCHAR(MAX) = NULL
    , @ExcludeProgramName VARCHAR(MAX) = NULL
    , @ThrowException BIT = 0
    , @WhatIf BIT = 0
    , @Help BIT = 0

AS
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @Version VARCHAR(10) = NULL
    , @VersionDate DATETIME = NULL

SELECT
    @Version = '1.1'
    , @VersionDate = '20250826';

DECLARE @MaxTempMB DECIMAL(18,2);

SELECT @MaxTempMB =
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM tempdb.sys.database_files
            WHERE max_size = -1 AND type = 0
        ) THEN -1
        ELSE SUM(max_size) / 128.0
    END
FROM tempdb.sys.database_files
WHERE type = 0;


/* @Help = 1 */
IF @Help = 1 BEGIN
    PRINT '
/*
    sp_KillSessionTempdb from Architecture&Performance GitHub  
    Version: 1.0 updated 06/25/2025

    This stored procedure analyzes tempdb usage and optionally terminates sessions 
    that are consuming excessive space, based on various filters and thresholds.

    Key Features:
    - Supports filtering by login name or program name (inclusion/exclusion).
    - Supports threshold-based targeting of sessions using tempdb space.
    - Allows previewing the impact via WhatIf mode before terminating sessions.
    - Optionally throws an exception when a session fails to be terminated.
    - Logs all terminated sessions into a permanent audit table (ProtectTempdbLog).

    Known limitations of this version:
    - This stored procedure is supported on SQL Server 2012 and newer.
    - The parameters @IncludeLogin, @ExcludeLogin, @IncludeProgramName, and @ExcludeProgramName
      are only functional on SQL Server 2016 and above.
      If used on SQL Server 2012–2014, these filters will be ignored.
    - You must have appropriate permissions (e.g., sysadmin) to kill sessions.
    - If tempdb files are set to unlimited growth and @UsageTempDb is less than 1,
      execution will be blocked to avoid illogical comparisons.

    Parameters:

    @UsageTempDb DECIMAL(18,2) = 0.5
        - Represents the threshold of tempdb space usage above which a session may be killed.
        - If between 0 and 1 (e.g., 0.5), it is interpreted as a percentage of total tempdb size.
        - If >= 1, it is interpreted as a fixed amount in megabytes (MB).
        - If tempdb has any file with unlimited growth, @UsageTempDb must be >= 1.

    @IncludeLogin VARCHAR(MAX) = NULL
        - Comma-separated list of login names to INCLUDE.
        - Only available on SQL Server 2016 and newer.

    @ExcludeLogin VARCHAR(MAX) = NULL
        - Comma-separated list of login names to EXCLUDE.
        - Only available on SQL Server 2016 and newer.

    @IncludeProgramName VARCHAR(MAX) = NULL
        - Comma-separated list of application names to INCLUDE.
        - Only available on SQL Server 2016 and newer.

    @ExcludeProgramName VARCHAR(MAX) = NULL
        - Comma-separated list of application names to EXCLUDE.
        - Only available on SQL Server 2016 and newer.

    @WhatIf BIT = 0
        - If set to 1, displays the sessions that would be killed without actually executing the KILL command.

    @ThrowException BIT = 0
        - If set to 1, throws an exception (THROW 50001) when a session fails to be killed.
        - Useful for automated monitoring and alerting systems that need to detect failures programmatically.
        - If set to 0 (default), the procedure will log the error using PRINT and continue processing.

    @Help BIT = 0
        - Displays this help documentation and exits.

    Logging:
    - All killed sessions are logged in the table dbo.ProtectTempdbLog, which includes:
        - SessionId           : ID of the terminated session.
        - LoginName           : Login name associated with the session.
        - ProgramName         : Name of the client application.
        - RunningUserSpaceMB  : Space (in MB) used by user objects.
        - ThresholdMB         : Configured threshold (in MB) that triggered the kill.
        - StatementText       : Command executed (e.g., KILL 53).
        - ExecutionDateTime   : Date and time when the session was terminated.

    Table Definition:

        CREATE TABLE [dbo].[ProtectTempdbLog](
        [SessionId] [int] NULL,
        [DatabaseName] [sysname] NULL,
        [LoginName] [nvarchar](256) NULL,
        [ProgramName] [nvarchar](1000) NULL,
        [RunningUserSpaceMB] [numeric](10, 1) NULL,
        [RunningInternalSpaceMB] [numeric](10, 1) NULL,
        [ThresholdMB] [numeric](10, 1) NULL,
        [BatchText] [nvarchar](max) NULL,
        [StatementText] [nvarchar](max) NULL,
        [StatementPlan] [xml] NULL,
        [ExecutionDateTime] [datetime] NULL DEFAULT (getdate())
    ;

    Usage Notes:
    - Use @WhatIf = 1 to preview results safely.
    - Use @ThrowException = 1 if you need to catch failures as actual SQL exceptions.
    - It is strongly recommended to monitor carefully before terminating sessions in a production environment.
    - Use filters wisely to avoid unintended session terminations.
    - Review contents of dbo.ProtectTempdbLog for audit and post-mortem analysis.

    This version is maintained by Architecture & Performance.
*/';

    RETURN;
    END;  



/* SQL Server version check */
DECLARE
    @SQL NVARCHAR(4000)
    , @SQLVersion NVARCHAR(128)
    , @SQLVersionMajor DECIMAL(10,2)
    , @SQLVersionMinor DECIMAL(10,2);

IF OBJECT_ID('tempdb..#SQLVersions') IS NOT NULL
    DROP TABLE #SQLVersions;

CREATE TABLE #SQLVersions (
    VersionName VARCHAR(10)
    , VersionNumber DECIMAL(10,2)
    );

INSERT #SQLVersions
VALUES
    ('2008', 10)
    , ('2008 R2', 10.5)
    , ('2012', 11)
    , ('2014', 12)
    , ('2016', 13)
    , ('2017', 14)
    , ('2019', 15)
    , ('2022', 16)
    , ('2025', 17);

/* SQL Server version */
SELECT @SQLVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));

SELECT
    @SQLVersionMajor = SUBSTRING(@SQLVersion, 1,CHARINDEX('.', @SQLVersion) + 1 )
    , @SQLVersionMinor = PARSENAME(CONVERT(varchar(32), @SQLVersion), 2);


/* check for unsupported version */
IF @SQLVersionMajor < 13 BEGIN
    PRINT '
/*
    *** Unsupported SQL Server Version ***

    sp_ProtectTempdb is supported only for execution on SQL Server 2016 and later.

    For more information about the limitations of sp_ProtectTempdb, execute
    using @Help = 1

    *** EXECUTION ABORTED ***
      
*/';
    RETURN 1;
    END;

/* check if TempDb size is unlimited then @UsageTempDb must be greater than 1 */
IF @MaxTempMB=-1 AND @UsageTempDb < 1 BEGIN
    PRINT '
/*
    *** @UsageTempDb MUST BE GREATER THAN 1 ***

    @UsageTempDb must be greater than 1 if @MaxTempMB equals -1 (Unlimited).

    For more information about the limitations of sp_ProtectTempdb, execute
    using @Help = 1

    *** EXECUTION ABORTED ***
          
*/';
    RETURN 1;
    END;

/* check if @UsageTempDb is smaller than @MaxTempMB */
IF @MaxTempMB <> -1 AND @UsageTempDb > @MaxTempMB BEGIN
    PRINT '
/*
    *** @UsageTempDb MUST BE SMALLER THAN @MaxTempMB ***

    @UsageTempDb must be smaller than @MaxTempMB.

    For more information about the limitations of sp_ProtectTempdb, execute
    using @Help = 1

    *** EXECUTION ABORTED ***
          
*/';
    RETURN 1;
    END;


/* check version if parameters @IncludeLogin,@ExcludeLogin,@IncludeProgramName,@ExcludeProgramName are used*/     
IF @SQLVersionMajor < 13 AND (@IncludeLogin IS NOT NULL OR @ExcludeLogin IS NOT NULL OR @IncludeProgramName IS NOT NULL OR @ExcludeProgramName IS NOT NULL) BEGIN
    PRINT '
/*
    *** @IncludeLogin,@ExcludeLogin,@IncludeProgramName,@ExcludeProgramName ARE NOT SUPPORTED ***

    @IncludeLogin,@ExcludeLogin,@IncludeProgramName,@ExcludeProgramName is supported only for execution on SQL Server 2016 and later.

    For more information about the limitations of sp_ProtectTempdb, execute
    using @Help = 1

    *** EXECUTION ABORTED ***
          
*/';
    RETURN 1;
    END;

/* Check Usage Percent value between 0 and 1 */
IF @UsageTempDb < 0  BEGIN
    PRINT '
/*
    *** @UsageTempDb MUST BE GREATER THAN 0 ***

    For more information of sp_ProtectTempdb, execute
    using @Help = 1

    *** EXECUTION ABORTED ***
        
*/';
    RETURN 1;
    END;


/* Set @UsageTempDbSize variable */
DECLARE @UsageTempDbSize BIGINT;
IF @UsageTempDb >= 0 AND @UsageTempDb <= 1
    BEGIN
        SET @UsageTempDbSize=@UsageTempDb * @MaxTempMB;
    END
ELSE
    BEGIN
        SET @UsageTempDbSize=@UsageTempDb;
    END;
           

/* TempdbStats */


DECLARE  @TempdbStats TABLE (
        TotalSpaceMB NUMERIC(10,1),
        UsedSpaceMB NUMERIC(10,1),
        FreespaceMB NUMERIC(10,1),
        UserObjectSpaceMB NUMERIC(10,1),
        InternalObjectSpaceMB NUMERIC(10,1),
        VersionStoreSpaceMB NUMERIC(10,1),
        LogFileSizeMB NUMERIC(10,1),
        LogSpaceUsedMB NUMERIC(10,1)
    );

INSERT INTO @TempdbStats
SELECT
    CONVERT(NUMERIC(10,1),(SUM(total_page_count)/128.)) AS TotalSpaceMB
    , CONVERT(NUMERIC(10,1),(SUM(allocated_extent_page_count)/128.)) AS UsedSpaceMB
    , CONVERT(NUMERIC(10,1),(SUM(unallocated_extent_page_count)/128.)) AS FreespaceMB
    , CONVERT(NUMERIC(10,1),(SUM(user_object_reserved_page_count)/128.)) AS UserObjectSpaceMB
    , CONVERT(NUMERIC(10,1),(SUM(internal_object_reserved_page_count)/128.)) AS InternalObjectSpaceMB
    , CONVERT(NUMERIC(10,1),(SUM(version_store_reserved_page_count)/128.)) AS VersionStoreSpaceMB
    , CONVERT(NUMERIC(10,1),(SELECT SUM(size)/128. FROM tempdb.sys.database_files WHERE type = 1)) AS LogFileSizeMB
    , (SELECT CONVERT(NUMERIC(10,1),(used_log_space_in_bytes/1048576.)) FROM tempdb.sys.dm_db_log_space_usage) AS LogSpaceUsedMB
FROM tempdb.sys.dm_db_file_space_usage

/* TempSessionStats */


DECLARE  @TempSessionStats TABLE (
		session_id SMALLINT,
        DatabaseName SYSNAME,
        LoginName NVARCHAR(128),
        ProgramName NVARCHAR(128),
        SessionSpaceMB NUMERIC(10,1),
        SessionUserSpaceMB NUMERIC(10,1),
        SessionInternalSpaceMB NUMERIC(10,1),
        RunningSpaceMB NUMERIC(10,1),
        RunningUserSpaceMB NUMERIC(10,1),
        RunningInternalSpaceMB NUMERIC(10,1)
    );

WITH SessionInfo AS (
    SELECT DISTINCT
        u.session_id, u.database_id
    FROM (
        SELECT session_id, ss1.database_id
        FROM tempdb.sys.dm_db_session_space_usage ss1 WITH (NOLOCK)
        WHERE session_id <> @@SPID
            AND (user_objects_alloc_page_count > 0 OR internal_objects_alloc_page_count > 0)
        UNION
        SELECT session_id,ts2.database_id
        FROM tempdb.sys.dm_db_task_space_usage ts2 WITH (NOLOCK)
        WHERE session_id <> @@SPID
            AND (user_objects_alloc_page_count > 0 OR internal_objects_alloc_page_count > 0)
    ) u
)
INSERT INTO @TempSessionStats
SELECT
    s.session_id,
    DB_NAME(s.database_id) AS DatabaseName,
    es.login_name AS LoginName,
    es.program_name AS ProgramName,
    CONVERT(NUMERIC(10,1), ssu.SessionNetAllocationMB) AS SessionSpaceMB,
    CONVERT(NUMERIC(10,1), ssu.SessionNetAllocationUserSpaceMB) AS SessionUserSpaceMB,
    CONVERT(NUMERIC(10,1), ssu.SessionNetAllocationInternalSpaceMB) AS SessionInternalSpaceMB,
    CONVERT(NUMERIC(10,1), tsu.RunningNetAllocationMB) AS RunningSpaceMB,
    CONVERT(NUMERIC(10,1), tsu.RunningNetAllocationUserSpaceMB) AS RunningUserSpaceMB,
    CONVERT(NUMERIC(10,1), tsu.RunningNetAllocationInternalSpaceMB) AS RunningInternalSpaceMB
FROM SessionInfo s
INNER JOIN sys.dm_exec_sessions es
    ON s.session_id = es.session_id
LEFT JOIN (
    SELECT
        session_id,
        (user_objects_alloc_page_count + internal_objects_alloc_page_count
         - internal_objects_dealloc_page_count - user_objects_dealloc_page_count ) / 128. AS SessionNetAllocationMB,
        (user_objects_alloc_page_count - user_objects_dealloc_page_count) / 128. AS SessionNetAllocationUserSpaceMB,
        (internal_objects_alloc_page_count - internal_objects_dealloc_page_count) / 128. AS SessionNetAllocationInternalSpaceMB
    FROM tempdb.sys.dm_db_session_space_usage
) ssu
    ON s.session_id = ssu.session_id
LEFT JOIN (
    SELECT
        session_id,
        SUM(user_objects_alloc_page_count + internal_objects_alloc_page_count
            - internal_objects_dealloc_page_count - user_objects_dealloc_page_count ) / 128. AS RunningNetAllocationMB,
        SUM(user_objects_alloc_page_count - user_objects_dealloc_page_count) / 128. AS RunningNetAllocationUserSpaceMB,
        SUM(internal_objects_alloc_page_count - internal_objects_dealloc_page_count) / 128. AS RunningNetAllocationInternalSpaceMB
    FROM tempdb.sys.dm_db_task_space_usage
    GROUP BY session_id
) tsu
    ON s.session_id = tsu.session_id
LEFT JOIN sys.dm_exec_connections c
    ON c.session_id = s.session_id
WHERE
    (CONVERT(NUMERIC(10,1), tsu.RunningNetAllocationUserSpaceMB) > @UsageTempDbSize) OR (CONVERT(NUMERIC(10,1), tsu.RunningNetAllocationInternalSpaceMB) > @UsageTempDbSize) AND
    (
        @IncludeLogin IS NULL
        OR es.login_name IN (
            SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@IncludeLogin, ',')
        )
    )
    AND (
        @ExcludeLogin IS NULL
        OR es.login_name NOT IN (
            SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@ExcludeLogin, ',')
        )
    )
    AND (
        @IncludeProgramName IS NULL
        OR es.program_name IN (
            SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@IncludeProgramName, ',')
        )
    )
    AND (
        @ExcludeProgramName IS NULL
        OR es.program_name NOT IN (
            SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@ExcludeProgramName, ',')
        )
    )
;



IF @WhatIf=1 BEGIN
    /* TempdbStats */
    SELECT * FROM @TempdbStats;
    /* TempSessionStats */
    SELECT * FROM @TempSessionStats ORDER BY SessionSpaceMB DESC;
    RETURN;
END;


/* Kill session Tempdb*/
DECLARE @SessionId SMALLINT;
DECLARE @DatabaseName SYSNAME;
DECLARE @LoginName NVARCHAR(256);
DECLARE @ProgramName NVARCHAR(1000);
DECLARE @RunningUserSpaceMB NUMERIC(10,1);
DECLARE @RunningInternalSpaceMB NUMERIC(10,1);
DECLARE @StatementText NVARCHAR(MAX);
DECLARE @BatchText NVARCHAR(MAX);
DECLARE @StatementPLAN XML;
DECLARE @KillCommand NVARCHAR(100);
DECLARE @ReturnCode INT = 0;
DECLARE @KillErrorMessage NVARCHAR(4000);

DECLARE SessionsToKill CURSOR LOCAL FAST_FORWARD FOR
SELECT session_id,DatabaseName,LoginName,ProgramName,RunningUserSpaceMB,RunningInternalSpaceMB
FROM @TempSessionStats
;

OPEN SessionsToKill;
FETCH NEXT FROM SessionsToKill INTO @SessionId, @DatabaseName, @LoginName, @ProgramName, @RunningUserSpaceMB,@RunningInternalSpaceMB;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @KillCommand = 'KILL ' + CAST(@SessionId AS NVARCHAR(10));
    PRINT 'Executing: ' + @KillCommand;
    BEGIN TRY
		
		--Get Sessions batch, statement and plan for logging
		EXEC dbo.sp_GetSessionPlan @session_id = @SessionId,
                                   @batch_text_out = @BatchText OUTPUT,
                                   @stmt_text_out  = @StatementText  OUTPUT,
                                   @plan_xml_out   = @StatementPLAN  OUTPUT;

        EXEC(@KillCommand);

        /* Insert into the log table*/
        INSERT INTO dbo.ProtectTempdbLog (
            SessionId, DatabaseName, LoginName, ProgramName, RunningUserSpaceMB,RunningInternalSpaceMB, ThresholdMB, BatchText, StatementText,StatementPlan
        ) VALUES (
            @SessionId, @DatabaseName,@LoginName, @ProgramName, @RunningUserSpaceMB,@RunningInternalSpaceMB, @UsageTempDbSize,@BatchText, @StatementText,@StatementPlan
        );


    END TRY
    BEGIN CATCH
        SET @KillErrorMessage = 'Error in killing the session ' + CAST(@SessionId AS NVARCHAR(10)) + ': ' + ERROR_MESSAGE();

        SET @ReturnCode = 2;

        IF @ThrowException = 1
            THROW 50001, @KillErrorMessage, 1;
       
    END CATCH;


    FETCH NEXT FROM SessionsToKill INTO @SessionId, @DatabaseName,  @LoginName, @ProgramName, @RunningUserSpaceMB,@RunningInternalSpaceMB ;
END;

CLOSE SessionsToKill;
DEALLOCATE SessionsToKill;

RETURN @ReturnCode;
GO

