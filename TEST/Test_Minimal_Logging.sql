--create schema [log];

--drop schema if exists [log];

USE FASTExportData
GO


DROP TABLE IF EXISTS [log].[TraceTable];
GO
DROP PROCEDURE IF EXISTS dbo.PrepareTest;
GO
DROP PROCEDURE IF EXISTS dbo.InsertData
GO
DROP PROCEDURE IF EXISTS dbo.RunBulkInsertTest;
GO
DROP PROCEDURE IF EXISTS dbo.RunAllBulkInsertTest;
GO

CREATE TABLE [log].[TraceTable]
(
    TestId INT PRIMARY KEY IDENTITY(1,1),
    TestName VARCHAR(255),
	SQLServerVersion VARCHAR(2000),
    TestParameters NVARCHAR(MAX),
    TestStart DATETIME,
    TestEnd DATETIME,
    TestDurationMilliseconds INT,
    TestCPUTimeMilliseconds INT,
    TestLogVolumeBytes BIGINT
);
GO

CREATE PROCEDURE dbo.PrepareTest
    @TestCase VARCHAR(30)
AS
BEGIN
    IF (@TestCase = 'CLASSIC CLUSTERED TABLE')
        CREATE CLUSTERED INDEX CI0 ON [dbo].[TEST_71_1M_12m_COPY](DT_PERIODE, ID_ARTICLE, ID_CLIENT);

    IF (@TestCase = 'COLUMNSTORE CLUSTERED TABLE')
        CREATE CLUSTERED COLUMNSTORE INDEX CCI0 ON [dbo].[TEST_71_1M_12m_COPY];
    
	IF (@TestCase = 'HEAP')
        PRINT 'No actions taken for this test case.';

	IF (@TestCase = 'SECONDARY INDEXES')
    BEGIN
        CREATE NONCLUSTERED INDEX IDX_DT_PERIODE ON [dbo].[TEST_71_1M_12m_COPY](DT_PERIODE);
        CREATE NONCLUSTERED INDEX IDX_ID_ARTICLE ON [dbo].[TEST_71_1M_12m_COPY](ID_ARTICLE);
        CREATE NONCLUSTERED INDEX IDX_ID_CLIENT ON [dbo].[TEST_71_1M_12m_COPY](ID_CLIENT);
        CREATE NONCLUSTERED INDEX IDX_ID_ARTICLE_ID_CLIENT ON [dbo].[TEST_71_1M_12m_COPY](ID_ARTICLE, ID_CLIENT);
    END

END;
GO


CREATE PROCEDURE dbo.InsertData 
    @TablockHint BIT, 
    @PreLoadStatement NVARCHAR(MAX),
	@logsize_used_in_bytes BIGINT OUTPUT 
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX);
	DECLARE @LogFilename NVARCHAR(1024);

	SELECT @LogFilename=name FROM sys.master_files WHERE database_id = DB_ID() AND type_desc = 'LOG';

	CHECKPOINT;
    DBCC SHRINKFILE(@LogFilename, 1);

	--WAITFOR DELAY '0:0:1';

    -- Execute PreLoadStatement if provided
    IF LEN(@PreLoadStatement) > 0
        EXEC sp_executesql @PreLoadStatement;


    BEGIN TRANSACTION;
		IF @TablockHint = 1
		  INSERT INTO [dbo].[TEST_71_1M_12m_COPY] WITH (TABLOCK) SELECT * FROM [dbo].[TEST_71_1M_12m];
		ELSE
		 INSERT INTO [dbo].[TEST_71_1M_12m_COPY] SELECT * FROM [dbo].[TEST_71_1M_12m];

		SELECT @logsize_used_in_bytes = used_log_space_in_bytes FROM sys.dm_db_log_space_usage;

    COMMIT TRANSACTION;
END;

GO




CREATE PROCEDURE dbo.RunBulkInsertTest
    @TargetTableType VARCHAR(30), 
    @TargetHasSecondaryIndexes BIT, 
    @TablockHint BIT, 
	@PreloadStatement NVARCHAR(2000)
AS
BEGIN
    DECLARE @StartTime DATETIME;
	DECLARE @EndTime DATETIME;
	DECLARE @CPUTime INT;
	DECLARE @logsize_used_in_bytes BIGINT;
	DECLARE @TestName VARCHAR(1000);
	DECLARE @TestCase NVARCHAR(255);
	DECLARE @Parameters VARCHAR(MAX);

	SET @StartTime=GETDATE();

	SET @Parameters = 
        'TargetTableType=' + @TargetTableType + ';' +
        'TargetHasSecondaryIndexes=' + CAST(@TargetHasSecondaryIndexes AS VARCHAR(5)) + ';' +
        'TablockHint=' + CAST(@TablockHint AS VARCHAR(5)) + ';' +
        'PreloadStatement=' + @PreloadStatement + ';';
    
    -- Now you can use @Parameters wherever needed
    PRINT @Parameters; 

    -- Drop and recreate table
    IF OBJECT_ID('dbo.TEST_71_1M_12m_COPY') IS NOT NULL
        DROP TABLE [dbo].[TEST_71_1M_12m_COPY];
    
	-- Create a copy empty table
    SELECT * INTO [dbo].[TEST_71_1M_12m_COPY] FROM [dbo].[TEST_71_1M_12m] WHERE 1=0;


    -- Prepare Test (HEAP vs CLASSIC CLUSTERED TABLE vs COLUMNSTORE CLUSTERED TABLE )
    EXEC dbo.PrepareTest @TestCase=@TargetTableType;

	-- Prepare Test (Has Secondary Index or not)
	IF (@TargetHasSecondaryIndexes=1)
	  EXEC dbo.PrepareTest @TestCase='SECONDARY INDEXES';

    -- Call the insert procedure
    EXEC dbo.InsertData @TablockHint=@TablockHint, @PreLoadStatement=@PreLoadStatement, @logsize_used_in_bytes=@logsize_used_in_bytes OUTPUT;

    -- End Time
    SET @EndTime = GETDATE();
    SET @CPUTime = (SELECT er.cpu_time FROM sys.dm_exec_requests er WHERE session_id = @@SPID);



    INSERT INTO [log].[TraceTable]
    (
        TestName,
		SQLServerVersion,
        TestParameters,
        TestStart,
        TestEnd,
        TestDurationMilliseconds,
        TestCPUTimeMilliseconds,
        TestLogVolumeBytes
    )
    VALUES
    (
        'Test : '+ @Parameters,
		@@VERSION,
        @Parameters,
        @StartTime,
        @EndTime,
        DATEDIFF(MILLISECOND, @StartTime, @EndTime),
        @CPUTime,
        @logsize_used_in_bytes
    );
END;

GO




CREATE PROCEDURE dbo.RunAllBulkInsertTest AS
BEGIN

-- Declare and populate table variables
DECLARE @TargetTypeValues TABLE (Value VARCHAR(30));
DECLARE @TargetHasSecondaryIndexesValues TABLE (Value BIT);
DECLARE @TablockHintValues TABLE (Value BIT);
DECLARE @PreloadStatementValues TABLE (Value NVARCHAR(2000));
DECLARE @LogFilename NVARCHAR(1024);

INSERT INTO @TargetTypeValues 
VALUES 
('HEAP'), 
('CLASSIC CLUSTERED TABLE'),
('COLUMNSTORE CLUSTERED TABLE');
INSERT INTO @TargetHasSecondaryIndexesValues VALUES (0), (1);
INSERT INTO @TablockHintValues VALUES (0), (1);
INSERT INTO @PreloadStatementValues VALUES ('DBCC TRACEON(715);'),('DBCC TRACEOFF(715);');

SELECT @LogFilename=name FROM sys.master_files WHERE database_id = DB_ID() AND type_desc = 'LOG';



-- Generate all combinations using CROSS JOIN and execute the procedure
SELECT 
    ti.Value AS TargetType, 
    thsi.Value AS TargetHasSecondaryIndexes, 
    tht.Value AS TablockHint, 
    ps.Value AS PreloadStatement
FROM 
    @TargetTypeValues ti
CROSS JOIN @TargetHasSecondaryIndexesValues thsi
CROSS JOIN @TablockHintValues tht
CROSS JOIN @PreloadStatementValues ps
ORDER BY 
    ti.Value, thsi.Value, tht.Value, ps.Value; -- Optional sorting

-- Loop through the results to call the procedure for each combination
DECLARE @TargetType VARCHAR(30), 
        @TargetHasSecondaryIndexes BIT, 
        @TablockHint BIT, 
        @PreloadStatement NVARCHAR(2000);

DECLARE comboCursor CURSOR FAST_FORWARD FOR 
SELECT 
     ti.Value AS TargetType, 
    thsi.Value AS TargetHasSecondaryIndexes, 
    tht.Value AS TablockHint, 
    ps.Value AS PreloadStatement
FROM 
    @TargetTypeValues ti
CROSS JOIN @TargetHasSecondaryIndexesValues thsi
CROSS JOIN @TablockHintValues tht
CROSS JOIN @PreloadStatementValues ps;

OPEN comboCursor;
FETCH NEXT FROM comboCursor INTO @TargetType, @TargetHasSecondaryIndexes, @TablockHint, @PreloadStatement;

WHILE @@FETCH_STATUS = 0
BEGIN
    
    EXEC dbo.RunBulkInsertTest @TargetTableType=@TargetType, @TargetHasSecondaryIndexes=@TargetHasSecondaryIndexes, @TablockHint=@TablockHint, @PreloadStatement=@PreloadStatement;
	CHECKPOINT;
    FETCH NEXT FROM comboCursor INTO @TargetType, @TargetHasSecondaryIndexes, @TablockHint, @PreloadStatement;
END;

CLOSE comboCursor;
DEALLOCATE comboCursor;

END;
GO




TRUNCATE TABLE [log].[TraceTable];
GO

EXEC dbo.RunAllBulkInsertTest;