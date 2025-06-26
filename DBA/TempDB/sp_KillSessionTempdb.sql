IF OBJECT_ID('dbo.sp_KillSessionTempdb') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_KillSessionTempdb AS RETURN 0;');
GO


ALTER PROCEDURE [dbo].[sp_KillSessionTempdb]
     @UsageTempDb DECIMAL(18,2) = 0.5
	, @IncludeLogin VARCHAR(MAX) = NULL
	, @ExcludeLogin VARCHAR(MAX) = NULL
	, @IncludeProgramName VARCHAR(MAX) = NULL
	, @ExcludeProgramName VARCHAR(MAX) = NULL
	, @WhatIf BIT = 0
	, @Help BIT = 0

WITH RECOMPILE
AS
SET NOCOUNT ON;

DECLARE 
    @Version VARCHAR(10) = NULL
	, @VersionDate DATETIME = NULL

SELECT
    @Version = '1.0'
    , @VersionDate = '20250625';

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

    @Help BIT = 0
        - Displays this help documentation and exits.

    Usage Notes:
    - Use @WhatIf = 1 to preview results safely.
    - It is strongly recommended to monitor carefully before terminating sessions in a production environment.
    - Use filters wisely to avoid unintended session terminations.


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
	, ('2022', 16);

/* SQL Server version */
SELECT @SQLVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));

SELECT 
	@SQLVersionMajor = SUBSTRING(@SQLVersion, 1,CHARINDEX('.', @SQLVersion) + 1 )
	, @SQLVersionMinor = PARSENAME(CONVERT(varchar(32), @SQLVersion), 2);


	/* check for unsupported version */	
IF @SQLVersionMajor < 11 BEGIN
	PRINT '
/*
    *** Unsupported SQL Server Version ***

    sp_KillSessionTempdb is supported only for execution on SQL Server 2012 and later.

	For more information about the limitations of sp_KillSessionTempdb, execute
    using @Help = 1

    *** EXECUTION ABORTED ***
    	   
*/';
	RETURN;
	END; 

/* check if TempDb size is unlimited then @UsageTempDb must be greater than 1 */
IF @MaxTempMB=-1 AND @UsageTempDb < 1 BEGIN
	PRINT '
/*
    *** @UsageTempDb MUST BE GREATER THAN 1 ***

    @UsageTempDb must be greater than 1 if @MaxTempMB equals -1 (Unlimited).

	For more information about the limitations of sp_KillSessionTempdb, execute
    using @Help = 1

    *** EXECUTION ABORTED ***
    	   
*/';
	RETURN;
	END;

/* check if @UsageTempDb is smaller than @MaxTempMB */
IF @MaxTempMB <> -1 AND @UsageTempDb > @MaxTempMB BEGIN
	PRINT '
/*
    *** @UsageTempDb MUST BE SMALLER THAN @MaxTempMB ***

    @UsageTempDb must be smaller than @MaxTempMB.

	For more information about the limitations of sp_KillSessionTempdb, execute
    using @Help = 1

    *** EXECUTION ABORTED ***
    	   
*/';
	RETURN;
	END;


/* check version if parameters @IncludeLogin,@ExcludeLogin,@IncludeProgramName,@ExcludeProgramName are used*/	
IF @SQLVersionMajor < 13 AND (@IncludeLogin IS NOT NULL OR @ExcludeLogin IS NOT NULL OR @IncludeProgramName IS NOT NULL OR @ExcludeProgramName IS NOT NULL) BEGIN
	PRINT '
/*
    *** @IncludeLogin,@ExcludeLogin,@IncludeProgramName,@ExcludeProgramName ARE NOT SUPPORTED ***

    @IncludeLogin,@ExcludeLogin,@IncludeProgramName,@ExcludeProgramName is supported only for execution on SQL Server 2016 and later.

	For more information about the limitations of sp_KillSessionTempdb, execute
    using @Help = 1

    *** EXECUTION ABORTED ***
    	   
*/';
	RETURN;
	END; 

/* Check Usage Percent value between 0 and 1 */
IF @UsageTempDb < 0  BEGIN
	PRINT '
/*
    *** @UsageTempDb MUST BE GREATER THAN 0 ***

	For more information of sp_KillSessionTempdb, execute
    using @Help = 1

    *** EXECUTION ABORTED ***
    	   
*/';
	RETURN;
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
IF OBJECT_ID('tempdb..#TempdbStats') IS NOT NULL
	DROP TABLE #TempdbStats;

CREATE TABLE #TempdbStats (
        TotalSpaceMB NUMERIC(10,1),
        UsedSpaceMB NUMERIC(10,1),
        FreespaceMB NUMERIC(10,1),
        UserObjectSpaceMB NUMERIC(10,1),
        InternalObjectSpaceMB NUMERIC(10,1),
        VersionStoreSpaceMB NUMERIC(10,1),
        LogFileSizeMB NUMERIC(10,1),
        LogSpaceUsedMB NUMERIC(10,1)
    );

INSERT INTO #TempdbStats	
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
IF OBJECT_ID('tempdb..#TempSessionStats') IS NOT NULL
	DROP TABLE #TempSessionStats;

CREATE TABLE #TempSessionStats (
        session_id INT,
        LoginName NVARCHAR(128),
        ProgramName NVARCHAR(128),
        SessionSpaceMB NUMERIC(10,1),
        SessionUserSpaceMB NUMERIC(10,1),
        SessionInternalSpaceMB NUMERIC(10,1),
        RunningSpaceMB NUMERIC(10,1),
        RunningUserSpaceMB NUMERIC(10,1),
        RunningInternalSpaceMB NUMERIC(10,1),
        StatementText NVARCHAR(MAX)
    );

;WITH SessionInfo AS (
    SELECT DISTINCT
        u.session_id
    FROM (
        SELECT session_id
        FROM tempdb.sys.dm_db_session_space_usage ss1 WITH (NOLOCK)
        WHERE session_id <> @@SPID
            AND (user_objects_alloc_page_count > 0 OR internal_objects_alloc_page_count > 0)
        UNION
        SELECT session_id
        FROM tempdb.sys.dm_db_task_space_usage ts2 WITH (NOLOCK)
        WHERE session_id <> @@SPID
            AND (user_objects_alloc_page_count > 0 OR internal_objects_alloc_page_count > 0)
    ) u
)
INSERT INTO #TempSessionStats
SELECT
    s.session_id,
    es.login_name AS LoginName,
    es.program_name AS ProgramName,
    CONVERT(NUMERIC(10,1), ssu.SessionNetAllocationMB) AS SessionSpaceMB,
    CONVERT(NUMERIC(10,1), ssu.SessionNetAllocationUserSpaceMB) AS SessionUserSpaceMB,
    CONVERT(NUMERIC(10,1), ssu.SessionNetAllocationInternalSpaceMB) AS SessionInternalSpaceMB,
    CONVERT(NUMERIC(10,1), tsu.RunningNetAllocationMB) AS RunningSpaceMB,
    CONVERT(NUMERIC(10,1), tsu.RunningNetAllocationUserSpaceMB) AS RunningUserSpaceMB,
    CONVERT(NUMERIC(10,1), tsu.RunningNetAllocationInternalSpaceMB) AS RunningInternalSpaceMB,
    t.[text] AS StatementText
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
OUTER APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) t
WHERE
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
    AND CONVERT(NUMERIC(10,1), ssu.SessionNetAllocationMB) > @UsageTempDbSize 
        
ORDER BY
    SessionSpaceMB DESC,
    RunningSpaceMB DESC;



IF @WhatIf=1 BEGIN
    /* TempdbStats */
    SELECT * FROM #TempdbStats;
    /* TempSessionStats */
    SELECT * FROM #TempSessionStats;
    RETURN;
END;


/* Kill session Tempdb */
DECLARE @SessionId INT;
DECLARE @KillCommand NVARCHAR(100);
DECLARE @SessionCount INT;

SELECT @SessionCount = COUNT(*) FROM #TempSessionStats;

IF @SessionCount = 0
BEGIN
    PRINT 'No session to kill. No process is consuming enough tempdb.';
END
ELSE
BEGIN
    DECLARE KillSessions CURSOR LOCAL FAST_FORWARD FOR
    SELECT session_id
    FROM #TempSessionStats;

    OPEN KillSessions;
    FETCH NEXT FROM KillSessions INTO @SessionId;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @KillCommand = 'KILL ' + CAST(@SessionId AS NVARCHAR(10));
        PRINT 'Executing : ' + @KillCommand;
        BEGIN TRY
            EXEC(@KillCommand);
        END TRY
        BEGIN CATCH
            PRINT 'Error while killing the session ' + CAST(@SessionId AS NVARCHAR(10)) + ' : ' + ERROR_MESSAGE();
        END CATCH

        FETCH NEXT FROM KillSessions INTO @SessionId;
    END;

    CLOSE KillSessions;
    DEALLOCATE KillSessions;
END





