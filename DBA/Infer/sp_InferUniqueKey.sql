CREATE OR ALTER PROCEDURE dbo.sp_inferUniqueKey
(
    @dbname              sysname,
    @schemaname          sysname,
    @tablename           sysname,
    @eligiblecolumnslist nvarchar(max) = NULL,  -- CSV of LIKE patterns, OR'ed
    @excludedcolumnslist nvarchar(max) = NULL,  -- CSV of NOT LIKE patterns, AND'ed
    @maxkeycolumns       int = 8,               -- max columns to consider
    @maxtests            int = 128,             -- max number of tests to run
    @samplepercent       int = 0,               -- 0=off; else 1..99 approximate page sample
    @validate            bit = 1,               -- if sampling is used, recheck on full data
    @storeResults        bit = 1,               -- if 1, store results in dbo.InferedUniqueKey table
    @debug               bit = 0                -- debug streaming & SQL printing
)

/*==============================================================================
  Infer a unique key (column combination) for a given table by testing combinations
  of columns. The procedure returns the first unique combination it finds, or null if not found
  within the given limits.
  The procedure can optionally use a page sample of the table to speed up the search,
  at the risk of false positives. If sampling is used, the found key can optionally be
  validated on the full data.
  
  Parameters:
    @dbname              sysname          : database name of the target table
    @schemaname          sysname          : schema name of the target table
    @tablename           sysname          : table name of the target table
    @eligiblecolumnslist nvarchar(max)    : optional CSV of LIKE patterns for column names to include (OR'ed)
    @excludedcolumnslist nvarchar(max)    : optional CSV of LIKE patterns for column names to exclude (AND'ed)
    @maxkeycolumns       int              : max number of columns in the inferred key (default 8)
    @maxtests            int              : max number of combinations to test (default 128)
    @samplepercent       int              : if between 1 and 99, use TABLESAMPLE SYSTEM (n PERCENT) to speed up (default 0=off)
    @validate            bit              : if sampling is used, recheck the found key on full data (default 1=yes)
    @debug               bit              : if 1, print debug messages and some SQL (default 0=off)
  
  Returns:
    A single row with the following columns:
      dbname              sysname        : as input
      schemaname          sysname        : as input
      tablename           sysname        : as input
      eligiblecolumnslist nvarchar(max)  : as input (or empty string)
      excludedcolumnslist nvarchar(max)  : as input (or empty string)
      uk_found           nvarchar(max)   : CSV of column names in the found unique key, or empty string if not found
  
  Example:
      EXEC dbo.sp_inferUniqueKey
      @dbname = N'tpch_test',
      @schemaname = N'dbo',
      @tablename = N'lineitem',
      @eligiblecolumnslist = N'%key,%number',
      @excludedcolumnslist = NULL,
      @maxkeycolumns = 5,
      @maxtests = 100,
      @samplepercent = 10,
      @validate = 1,
      @debug = 1;

================================================================================*/


AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime datetime2 = SYSUTCDATETIME();

    IF @dbname IS NULL OR @schemaname IS NULL OR @tablename IS NULL
    BEGIN
        RAISERROR('All of @dbname, @schemaname, @tablename are required.', 16, 1);
        RETURN;
    END;

    IF @maxkeycolumns IS NULL OR @maxkeycolumns < 1 SET @maxkeycolumns = 1;
    IF @maxtests      IS NULL OR @maxtests      < 1 SET @maxtests      = 1;
    IF @samplepercent IS NULL OR @samplepercent < 0 SET @samplepercent = 0;
    IF @samplepercent > 99 SET @samplepercent = 99;

    DECLARE @fullName nvarchar(776) =
        QUOTENAME(@dbname) + N'.' + QUOTENAME(@schemaname) + N'.' + QUOTENAME(@tablename);

    -- temp tables for patterns
    IF OBJECT_ID('tempdb..#eligible') IS NOT NULL DROP TABLE #eligible;
    IF OBJECT_ID('tempdb..#excluded') IS NOT NULL DROP TABLE #excluded;
    CREATE TABLE #eligible(pat nvarchar(4000) NOT NULL);
    CREATE TABLE #excluded(pat nvarchar(4000) NOT NULL);

    ;WITH s AS (
        SELECT LTRIM(RTRIM(value)) AS pat
        FROM STRING_SPLIT(ISNULL(@eligiblecolumnslist, N''), N',')
        WHERE value IS NOT NULL AND LTRIM(RTRIM(value)) <> N''
    )
    INSERT INTO #eligible(pat) SELECT pat FROM s;

    ;WITH s AS (
        SELECT LTRIM(RTRIM(value)) AS pat
        FROM STRING_SPLIT(ISNULL(@excludedcolumnslist, N''), N',')
        WHERE value IS NOT NULL AND LTRIM(RTRIM(value)) <> N''
    )
    INSERT INTO #excluded(pat) SELECT pat FROM s;

    -- candidate columns
    IF OBJECT_ID('tempdb..#candidates') IS NOT NULL DROP TABLE #candidates;
    CREATE TABLE #candidates(
        rn        int IDENTITY(1,1) PRIMARY KEY,
        colname   sysname NOT NULL,
        column_id int NOT NULL
    );

    DECLARE @metaSQL nvarchar(max) = N'
        INSERT INTO #candidates(colname, column_id)
        SELECT TOP (@topN) c.name, c.column_id
        FROM ' + QUOTENAME(@dbname) + N'.sys.tables AS t
        JOIN ' + QUOTENAME(@dbname) + N'.sys.schemas AS s ON s.schema_id = t.schema_id
        JOIN ' + QUOTENAME(@dbname) + N'.sys.columns AS c ON c.object_id = t.object_id
        JOIN ' + QUOTENAME(@dbname) + N'.sys.types   AS ty ON ty.user_type_id = c.user_type_id
        WHERE s.name = @pSchema
          AND t.name = @pTable
          AND ty.name NOT IN (N''text'', N''ntext'', N''image'', N''xml'', N''sql_variant'', N''geography'', N''geometry'', N''hierarchyid'')
          AND (
                NOT EXISTS (SELECT 1 FROM #eligible)
                OR EXISTS (SELECT 1 FROM #eligible e WHERE c.name COLLATE DATABASE_DEFAULT LIKE e.pat ESCAPE N''\'' )
              )
          AND (
                NOT EXISTS (SELECT 1 FROM #excluded)
                OR NOT EXISTS (SELECT 1 FROM #excluded x WHERE c.name COLLATE DATABASE_DEFAULT LIKE x.pat ESCAPE N''\'' )
              )
        ORDER BY c.column_id;';
    DECLARE @topN int = @maxkeycolumns;
    EXEC sp_executesql @metaSQL, N'@pSchema sysname, @pTable sysname, @topN int',
        @pSchema=@schemaname, @pTable=@tablename, @topN=@topN;

    IF NOT EXISTS (SELECT 1 FROM #candidates)
    BEGIN
        IF @debug = 1
        BEGIN
            DECLARE @dbgMsg1 nvarchar(400) = N'sp_inferUniqueKey: no candidates after filters.';
            RAISERROR('%s', 10, 1, @dbgMsg1) WITH NOWAIT;
        END;
        SELECT
            @dbname AS dbname, @schemaname AS schemaname, @tablename AS tablename,
            ISNULL(@eligiblecolumnslist, N'') AS eligiblecolumnslist,
            ISNULL(@excludedcolumnslist, N'') AS excludedcolumnslist,
            CAST(N'' AS nvarchar(max)) AS uk_found;
        RETURN;
    END;

    IF @debug = 1
    BEGIN
        DECLARE @dbgMsg2 nvarchar(400) = N'sp_inferUniqueKey: candidates (cap to @maxkeycolumns).';
        RAISERROR('%s', 10, 1, @dbgMsg2) WITH NOWAIT;
        SELECT rn, colname, column_id FROM #candidates ORDER BY rn;
    END;

    -- First Test is when using all columns as key we find duplicates or not
    -- If we find duplicates we can continue but we already know that no unique key exists so no @winner
    -- But we can store the number of duplicates found will all columns as the new ground for duplicates minimum
    -- If we find a smaller combination with the same number of duplicates we can stop searching earlier 

    DECLARE @isAllColsUnique bit;
    DECLARE @minimalDuplicates bigint = 0;

    DECLARE @allCols nvarchar(max);
    SELECT @allCols = STRING_AGG(QUOTENAME(colname), N',') WITHIN GROUP (ORDER BY rn)
    FROM #candidates;
    DECLARE @sqlAllCols nvarchar(max) = N'
        DECLARE @d BIGINT;
        SELECT @d = COUNT_BIG(*) FROM (
            SELECT ' + @allCols + N'
            FROM ' + @fullName + N'
            GROUP BY ' + @allCols + N'
            HAVING COUNT(*) > 1
        ) s;
        SELECT @d AS dup_count;';
    DECLARE @tAllCols TABLE(dup_count bigint);
    IF @debug = 1
    BEGIN
        DECLARE @dbgMsgAllCols nvarchar(200) = N'sp_inferUniqueKey: testing all columns as key first to check if unique key exists...';
        RAISERROR('%s', 10, 1, @dbgMsgAllCols) WITH NOWAIT;
        RAISERROR(N'SQL: %s', 10, 1, @sqlAllCols) WITH NOWAIT;
    END;
    INSERT @tAllCols EXEC sp_executesql @sqlAllCols;
    DECLARE @dupCountAllCols bigint;
    SELECT @dupCountAllCols = dup_count FROM @tAllCols;

    IF @dupCountAllCols = 0
        SET @isAllColsUnique = 1;
    ELSE
        BEGIN
            SET @minimalDuplicates = @dupCountAllCols;
            SET @isAllColsUnique = 0;
            IF @debug = 1
            BEGIN
                RAISERROR(N' -> All columns (%s) are NOT unique, duplicates found: %d', 10, 1, @allCols, @dupCountAllCols) WITH NOWAIT;
            END;
        END;

    
    

    -- optional one-time sample
    DECLARE @useSampling bit = CASE WHEN @samplepercent BETWEEN 1 AND 99 THEN 1 ELSE 0 END;
    DECLARE @sampleMaterialized bit = 1;
    DECLARE @colsAll nvarchar(max);

    SELECT @colsAll = STRING_AGG(QUOTENAME(colname), N',') WITHIN GROUP (ORDER BY rn)
    FROM #candidates;

    IF @useSampling = 1
    BEGIN
        -- we need a global temp table for the sample, because dynamic SQL cannot see local temp tables
        -- we also need a variable name for the global temp table, because multiple executions of this procedure
        -- could collide on the same ##sample table name
        
        -- Generate a random suffix for the global temp table name
        DECLARE @randomSuffix nvarchar(32) = CONVERT(nvarchar(32), ABS(CHECKSUM(NEWID())));
        DECLARE @globalSampleTableName sysname = N'##sample_' + @randomSuffix;
        DECLARE @dropGlobalSampleTableSQL nvarchar(max) =
            N'IF OBJECT_ID(''tempdb..' + @globalSampleTableName + N''') IS NOT NULL DROP TABLE ' + @globalSampleTableName + N';' ;
        EXEC (@dropGlobalSampleTableSQL);  -- clean up any leftover from previous failed runs

        DECLARE @mkSample nvarchar(max) =
            N'SELECT ' + @colsAll + N'
              INTO ' + @globalSampleTableName + N'
              FROM ' + @fullName + N' TABLESAMPLE SYSTEM (' + CAST(@samplepercent AS nvarchar(10)) + N' PERCENT);
              CREATE CLUSTERED COLUMNSTORE INDEX i0 ON ' + @globalSampleTableName + N';';

        BEGIN TRY
            IF @debug = 1
            BEGIN
                DECLARE @dbgMsg3 nvarchar(200) = N'sp_inferUniqueKey: creating '+ @globalSampleTableName + N' ...';
                RAISERROR('%s', 10, 1, @dbgMsg3) WITH NOWAIT;
                SELECT [sample_build_sql] = @mkSample;
            END;

            EXEC (@mkSample);
            IF @debug = 1
            BEGIN
                DECLARE @dysqlCheck nvarchar(max) = N'SELECT COUNT(*) FROM ' + @globalSampleTableName + N';';
                DECLARE @rc bigint = 0;
                EXEC sp_executesql @dysqlCheck, N'@rc bigint OUTPUT', @rc=@rc OUTPUT;                             
                RAISERROR(N' -> #sample ready (rows): %d', 10, 1,@rc) WITH NOWAIT;                
            END;
        END TRY
        BEGIN CATCH            
            DECLARE @errn int = ERROR_NUMBER();
            DECLARE @errm nvarchar(2048) = ERROR_MESSAGE();
            DECLARE @dbgMsg5 nvarchar(200) = N' -> Could not materialize'+ @globalSampleTableName;
            RAISERROR('%s', 10, 1, @dbgMsg5) WITH NOWAIT;
            RAISERROR('   error %d: %s', 10, 1, @errn, @errm) WITH NOWAIT;   
            RETURN 2;         
        END CATCH
    END

    -- generate combinations
    IF OBJECT_ID('tempdb..#tests') IS NOT NULL DROP TABLE #tests;
    CREATE TABLE #tests(
        test_id  int IDENTITY(1,1) PRIMARY KEY,
        k        int NOT NULL,
        cols_csv nvarchar(max) NOT NULL
    );

    ;WITH combos AS (
        SELECT k = 1, last_rn = c.rn, cols_csv = CAST(QUOTENAME(c.colname) AS nvarchar(max))
        FROM #candidates AS c
        UNION ALL
        SELECT k = combos.k + 1, last_rn = c2.rn,
               cols_csv = CAST(combos.cols_csv + N',' + QUOTENAME(c2.colname) AS nvarchar(max))
        FROM combos
        JOIN #candidates AS c2 ON c2.rn > combos.last_rn
        WHERE combos.k < @maxkeycolumns
    )
    INSERT INTO #tests(k, cols_csv)
    SELECT TOP (@maxtests) k, cols_csv
    FROM combos
    ORDER BY k, cols_csv
    OPTION (MAXRECURSION 32767);

    DECLARE @nTests int = (SELECT COUNT(*) FROM #tests);
    IF @debug = 1
    BEGIN
        DECLARE @dbgMsg6 nvarchar(200) = N'sp_inferUniqueKey: test combinations generated.';
        RAISERROR('%s', 10, 1, @dbgMsg6) WITH NOWAIT;
        SELECT test_id, k, cols_csv FROM #tests ORDER BY test_id;
    END;

    -- FROM fragments
    DECLARE @fromFull   nvarchar(max) = N' FROM ' + @fullName + N' ';
    DECLARE @fromSample nvarchar(max);

    IF @useSampling = 1 AND @sampleMaterialized = 1
        SET @fromSample = N' FROM '+ @globalSampleTableName + N' ';
    ELSE
        SET @fromSample = @fromFull;

    -- run tests
    DECLARE
        @i            int = 1,
        @cols         nvarchar(max),
        @k            int,
        @dupCount     bigint,
        @lastbestDupCount bigint = 2147483647,  -- max int
        @lastbestDupCountSample bigint = 2147483647,
        @sql          nvarchar(max),
        @winner       nvarchar(max) = N'',
        @bestSoFar    nvarchar(max) = N'',
        @bestSoFarSample    nvarchar(max) = N'',
        @dbgSampleSrc nvarchar(32);



    WHILE @i <= @nTests
    BEGIN
        SELECT @cols = cols_csv, @k = k FROM #tests WHERE test_id = @i;

        SET @sql = N'
            DECLARE @d BIGINT;
            SELECT @d = COUNT_BIG(*) FROM (
                SELECT ' + @cols + N'
                ' + @fromSample + N'
                GROUP BY ' + @cols + N'
                HAVING COUNT(*) > 1
            ) s;
            SELECT @d AS dup_count;';

        IF @debug = 1
        BEGIN
            DECLARE @dbgMsg7 nvarchar(400) = N'Test ' + CAST(@i AS nvarchar(10)) + N'/' + CAST(@nTests AS nvarchar(10)) + N' (k=' + CAST(@k AS nvarchar(10)) + N') [sample=' + CAST(@samplepercent AS nvarchar(2)) + N'%]: with keys ' + ISNULL(@cols, N'') + N' ...';
            RAISERROR('%s', 10, 1, @dbgMsg7) WITH NOWAIT;            
        END;

        BEGIN TRY
            DECLARE @t TABLE(dup_count bigint);
            INSERT @t EXEC sp_executesql @sql;
            SELECT @dupCount = dup_count FROM @t;

            IF @debug = 1
            BEGIN
                DECLARE @dupCountStr nvarchar(50) = CAST(@dupCount AS NVARCHAR(50));
                DECLARE @dbgMsg8 nvarchar(200) = N' -> Number of duplicates found for ('+ @cols + ') = ' + @dupCountStr;
                RAISERROR('%s', 10, 1, @dbgMsg8) WITH NOWAIT;
            END;
        END TRY
        BEGIN CATCH            
            DECLARE @errn2 int = ERROR_NUMBER();
            DECLARE @errm2 nvarchar(2048) = ERROR_MESSAGE();
            DECLARE @dbgMsg9 nvarchar(64) = N' -> ERROR on test('+ @cols + ') : ';
            RAISERROR('%s %d', 10, 1, @dbgMsg9, @i) WITH NOWAIT;
            RAISERROR('%d %s', 10, 1, @errn2, @errm2) WITH NOWAIT;            
            SET @i = @i + 1;
            CONTINUE;
        END CATCH

        -- keep best (of sample) so far
        IF @dupCount < @lastbestDupCountSample
        BEGIN
            SET @lastbestDupCountSample = @dupCount;
            SET @bestSoFarSample = @cols;
        END;


        IF @dupCount = 0
        BEGIN
            IF @useSampling = 1 AND @validate = 1
            BEGIN
                SET @sql = N'
                    DECLARE @d BIGINT;
                    SELECT @d = COUNT_BIG(*) FROM (
                        SELECT ' + @cols + N'
                        ' + @fromFull + N'
                        GROUP BY ' + @cols + N'
                        HAVING COUNT(*) > 1
                    ) s;
                    SELECT @d AS dup_count;';

                IF @debug = 1
                BEGIN
                    DECLARE @dbgMsg10 nvarchar(100) = N' -> validating on full data for ('+ @cols + ') ...';
                    RAISERROR('%s', 10, 1, @dbgMsg10) WITH NOWAIT;
                    SELECT [validation_sql]=@sql;
                END;

                BEGIN TRY
                    DECLARE @tv TABLE(dup_count bigint);
                    INSERT @tv EXEC sp_executesql @sql;
                    SELECT @dupCount = dup_count FROM @tv;

                    IF @dupCount = @minimalDuplicates
                    BEGIN
                        IF @isAllColsUnique = 1
                        BEGIN
                            -- all columns are unique, so no need to continue testing
                            SET @winner = @cols;
                            IF @debug = 1
                            BEGIN
                                DECLARE @dbgMsgAllCols2 nvarchar(200) = N' -> VALIDATED unique on full data (all columns).';
                                RAISERROR('%s', 10, 1, @dbgMsgAllCols2) WITH NOWAIT;
                            END;
                            BREAK;
                        END;
                        ELSE
                        BEGIN                       
                            SET @winner = null;
                            SET @bestSoFar = @cols;
                            IF @debug = 1
                            BEGIN
                                DECLARE @dbgMsg11 nvarchar(120) = N' -> EARLY STOPPED with best approx unique on full data (accepted) for ('+ @cols + ')';
                                RAISERROR('%s', 10, 1, @dbgMsg11) WITH NOWAIT;
                            END;
                            BREAK;
                        END
                    END
                    ELSE
                    BEGIN
                        -- store best so far on full data
                        IF @dupCount < @lastbestDupCount
                        BEGIN
                            SET @lastbestDupCount = @dupCount;
                            SET @bestSoFar = @cols;
                        END;
                        IF @debug = 1
                        BEGIN
                            DECLARE @dbgMsg12 nvarchar(120) = N' -> validation of ('+ @cols + ') failed (duplicates on full). Continue.';
                            RAISERROR('%s', 10, 1, @dbgMsg12) WITH NOWAIT;
                        END;
                    END
                END TRY
                BEGIN CATCH                    
                        DECLARE @errn3 int = ERROR_NUMBER();
                        DECLARE @errm3 nvarchar(2048) = ERROR_MESSAGE();
                        DECLARE @dbgMsg13 nvarchar(64) = N' -> ERROR during validation:';
                        RAISERROR('%s', 10, 1, @dbgMsg13) WITH NOWAIT;
                        RAISERROR('    %d %s', 10, 1, @errn3, @errm3) WITH NOWAIT;
                END CATCH
            END
            ELSE
            BEGIN
                SET @winner = @cols;
                IF @debug = 1
                BEGIN
                    DECLARE @dbgMsg14 nvarchar(120) = N' -> UNIQUE found on current scope (sample = ' + CAST(@samplepercent AS nvarchar(2)) + N'%) for ('+ @cols + ')';
                    RAISERROR('%s', 10, 1, @dbgMsg14) WITH NOWAIT;
                END;
                BREAK;
            END
        END



        SET @i = @i + 1;
    END

    IF @debug = 1
    BEGIN
        DECLARE @winnerSafe nvarchar(max) = ISNULL(@winner, N'');
        DECLARE @dbgMsg15 nvarchar(200) = N'sp_inferUniqueKey: result uk_found = ' + @winnerSafe;
        RAISERROR('%s', 10, 1, @dbgMsg15) WITH NOWAIT;
    END;

    -- clean up global temp table if any
    IF @useSampling = 1 AND @sampleMaterialized = 1
    BEGIN
        IF @debug = 1
        BEGIN
            DECLARE @dbgMsg16 nvarchar(200) = N'sp_inferUniqueKey: dropping '+ @globalSampleTableName + N' ...';
            RAISERROR('%s', 10, 1, @dbgMsg16) WITH NOWAIT;
        END;
        EXEC (@dropGlobalSampleTableSQL);
    END;

    DECLARE @EndTime datetime2 = SYSUTCDATETIME();
    DECLARE @durationSeconds int = DATEDIFF(SECOND, @StartTime, @EndTime);

    IF @storeResults = 1
    BEGIN
        INSERT INTO dbo.InferedUniqueKey
        (dbname, schemaname, tablename, eligiblecolumnslist, excludedcolumnslist, uk_found, best_unique_approximation, testdurationseconds)
        VALUES
        (@dbname, @schemaname, @tablename,
         ISNULL(@eligiblecolumnslist, N''),
         ISNULL(@excludedcolumnslist, N''),
         @winner,
         CASE WHEN @winner IS NOT NULL AND @winner <> N'' THEN NULL
              WHEN ISNULL(@bestSoFar,N'') <> N'' THEN @bestSoFar
              WHEN ISNULL(@bestSoFarSample,N'') <> N'' THEN @bestSoFarSample              
              ELSE NULL
         END,
         @durationSeconds);
    END;

    SELECT
        @dbname AS dbname,
        @schemaname AS schemaname,
        @tablename AS tablename,
        ISNULL(@eligiblecolumnslist, N'') AS eligiblecolumnslist,
        ISNULL(@excludedcolumnslist, N'') AS excludedcolumnslist,
        @winner AS uk_found,
        CASE WHEN @winner IS NOT NULL AND @winner <> N'' THEN NULL
              WHEN ISNULL(@bestSoFar,N'') <> N'' THEN @bestSoFar
              WHEN ISNULL(@bestSoFarSample,N'') <> N'' THEN @bestSoFarSample              
              ELSE NULL
         END AS best_unique_approximation,
         @durationSeconds AS testdurationseconds;
END
GO
