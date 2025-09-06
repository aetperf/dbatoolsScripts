CREATE OR ALTER PROCEDURE [dbo].[sp_CompareTables]
(
    @testname           NVARCHAR(255),            -- testname (found in T_COMPARE_CONFIG table)
    @include_columns    NVARCHAR(MAX) = NULL,     -- CSV : columns explicitly tested
    @exclude_columns    NVARCHAR(MAX) = NULL,     -- CSV : columns to exclude
    @cutoff             BIGINT        = 1000000,  -- cutoff for except temp results (set 0 will use full compare)
    @getsamplekeys      BIT           = 0,        -- no sampling for retrieving keys when diff
    @keydiffthreshold   BIGINT        = 0,        -- tolerated keydiff differences to pursue to col_diff
    @debug              BIT           = 0         -- prints dynamic SQL if 1
)
/*
Will compare 2 tables (or views) defined in the T_COMPARE_CONFIG table using an except technic 
A first test is made to compare rows count for source and target.
If the row counts are identical the program continue.
Then the keys are compared. If OK the program continue.
Then for each non-keys columns that sastisfy @include_columns and @exclude_params (or all if not defined), 
 the program compare key columns + the tested columns by except and store the diff count 

 The program return it's results and store them in the T_COMPARE_RESULTS table.

sample test :
INSERT INTO [dbo].[T_COMPARE_CONFIG] 
([testname] ,
 [sourcedatabase],[sourceschema],[sourcetable]
,[targetdatabase],[targetschema],[targettable]
,[keycolumns])
VALUES
('tpch10_orders'
 ,'tpch_copy','tpch_10','orders'
 ,'tpch_test','dbo','orders_15M'
 ,'o_orderkey')

Sample usage :
EXEC [dbo].[sp_CompareTables] @testname='tpch10_orders', @exclude_columns='o_comment,o_clerk'

Sample usage :
EXEC [dbo].[sp_CompareTables] @testname='tpch10_orders', @exclude_columns='o_comment,o_clerk', @getsamplekeys=1, @keydiffthreshold = 1


*/
AS
BEGIN
    SET NOCOUNT ON;

    -------------------------------------------------------------------------
    -- 0) Small helper to PRINT long NVARCHAR
    -------------------------------------------------------------------------
    DECLARE @p NVARCHAR(MAX);
    DECLARE @CRLF NCHAR(2) = NCHAR(13)+NCHAR(10);
    DECLARE @PrintLongSQL NVARCHAR(4000) =
       N'DECLARE @x NVARCHAR(MAX)=@s; WHILE LEN(@x)>0 BEGIN PRINT LEFT(@x,4000); SET @x=SUBSTRING(@x,4001,2147483647); END';

    -------------------------------------------------------------------------
    -- 1) Load config (now includes source/target databases)
    -------------------------------------------------------------------------
    DECLARE 
        @src_db SYSNAME, @src_schema SYSNAME, @src_table SYSNAME,
        @tgt_db SYSNAME, @tgt_schema SYSNAME, @tgt_table SYSNAME,
        @key_csv NVARCHAR(1000);

    SELECT
        @src_db     = NULLIF(LTRIM(RTRIM(c.sourcedatabase)),  ''),
        @src_schema = c.sourceschema,
        @src_table  = c.sourcetable,
        @tgt_db     = NULLIF(LTRIM(RTRIM(c.targetdatabase)),  ''),
        @tgt_schema = c.targetschema,
        @tgt_table  = c.targettable,
        @key_csv    = c.keycolumns
    FROM dbo.T_COMPARE_CONFIG AS c
    WHERE c.testname = @testname;

    IF @src_schema IS NULL
    BEGIN
        RAISERROR('Testname "%s" not found in T_COMPARE_CONFIG.', 16, 1, @testname);
        RETURN;
    END;

    -- Default DBs to current DB if null/empty
    SET @src_db = COALESCE(@src_db, DB_NAME());
    SET @tgt_db = COALESCE(@tgt_db, DB_NAME());

    -------------------------------------------------------------------------
    -- 2) Build 3-part names and validate existence
    -------------------------------------------------------------------------
    DECLARE 
        @src_3part NVARCHAR(500) = QUOTENAME(@src_db) + N'.' + QUOTENAME(@src_schema) + N'.' + QUOTENAME(@src_table),
        @tgt_3part NVARCHAR(500) = QUOTENAME(@tgt_db) + N'.' + QUOTENAME(@tgt_schema) + N'.' + QUOTENAME(@tgt_table);

    DECLARE @sql NVARCHAR(MAX), @src_id INT, @tgt_id INT;

    SET @sql = N'SELECT @id = OBJECT_ID(N''' + @src_3part + N''');';
    IF @debug=1 EXEC sp_executesql @PrintLongSQL, N'@s nvarchar(max)', @s=@sql;
    EXEC sp_executesql @sql, N'@id INT OUTPUT', @id=@src_id OUTPUT;

    SET @sql = N'SELECT @id = OBJECT_ID(N''' + @tgt_3part + N''');';
    IF @debug=1 EXEC sp_executesql @PrintLongSQL, N'@s nvarchar(max)', @s=@sql;
    EXEC sp_executesql @sql, N'@id INT OUTPUT', @id=@tgt_id OUTPUT;

    IF @src_id IS NULL OR @tgt_id IS NULL
    BEGIN
        RAISERROR('Source or target not found: %s / %s', 16, 1, @src_3part, @tgt_3part);
        RETURN;
    END;

    -------------------------------------------------------------------------
    -- 3) Parse CSV params
    -------------------------------------------------------------------------
    DECLARE @Keys TABLE (col SYSNAME PRIMARY KEY);
    INSERT INTO @Keys(col)
    SELECT LTRIM(RTRIM(value))
    FROM STRING_SPLIT(@key_csv, ',')
    WHERE LTRIM(RTRIM(value)) <> '';

    IF NOT EXISTS(SELECT 1 FROM @Keys)
    BEGIN
        RAISERROR('No key columns defined for %s.', 16, 1, @testname);
        RETURN;
    END;

    DECLARE @Include TABLE (col SYSNAME PRIMARY KEY);
    IF @include_columns IS NOT NULL AND LTRIM(RTRIM(@include_columns)) <> ''
        INSERT INTO @Include(col)
        SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@include_columns, ',')
        WHERE LTRIM(RTRIM(value)) <> '';

    DECLARE @Exclude TABLE (col SYSNAME PRIMARY KEY);
    IF @exclude_columns IS NOT NULL AND LTRIM(RTRIM(@exclude_columns)) <> ''
        INSERT INTO @Exclude(col)
        SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@exclude_columns, ',')
        WHERE LTRIM(RTRIM(value)) <> '';

    -------------------------------------------------------------------------
    -- 4) Load metadata cross-DB into temp tables
    -------------------------------------------------------------------------
    DROP TABLE IF EXISTS #src_cols; DROP TABLE IF EXISTS #tgt_cols;
    CREATE TABLE #src_cols(name SYSNAME NOT NULL, is_computed BIT NOT NULL, system_type_id INT NOT NULL, column_id INT NOT NULL);
    CREATE TABLE #tgt_cols(name SYSNAME NOT NULL, is_computed BIT NOT NULL, system_type_id INT NOT NULL, column_id INT NOT NULL);

    SET @sql = N'
        SELECT c.name, c.is_computed, c.system_type_id, c.column_id
        FROM ' + QUOTENAME(@src_db) + N'.sys.columns c
        WHERE c.object_id = OBJECT_ID(N''' + @src_3part + N''');';
    IF @debug=1 EXEC sp_executesql @PrintLongSQL, N'@s nvarchar(max)', @s=@sql;
    INSERT INTO #src_cols(name,is_computed,system_type_id,column_id)
    EXEC sys.sp_executesql @sql;

    SET @sql = N'
        SELECT c.name, c.is_computed, c.system_type_id, c.column_id
        FROM ' + QUOTENAME(@tgt_db) + N'.sys.columns c
        WHERE c.object_id = OBJECT_ID(N''' + @tgt_3part + N''');';
    IF @debug=1 EXEC sp_executesql @PrintLongSQL, N'@s nvarchar(max)', @s=@sql;
    INSERT INTO #tgt_cols(name,is_computed,system_type_id,column_id)
    EXEC sys.sp_executesql @sql;

    -- Ensure all key columns exist on both sides
    IF EXISTS (
        SELECT 1 FROM @Keys k
        WHERE NOT EXISTS (SELECT 1 FROM #src_cols s WHERE s.name = k.col)
           OR NOT EXISTS (SELECT 1 FROM #tgt_cols t WHERE t.name = k.col)
    )
    BEGIN
        RAISERROR('At least one key column is missing on source or target.', 16, 1);
        RETURN;
    END;

    -------------------------------------------------------------------------
    -- 5) Build key list (COLLATE for char-like types)
    -------------------------------------------------------------------------
    DECLARE @key_list NVARCHAR(MAX) = N'';
    SELECT @key_list = STRING_AGG(
        QUOTENAME(k.col) + CASE WHEN s.system_type_id IN (167,175,231,239) THEN ' COLLATE Latin1_General_100_BIN2_UTF8' ELSE '' END
        , ', ')
    FROM @Keys k
    JOIN #src_cols s ON s.name = k.col;

    -------------------------------------------------------------------------
    -- 6) Test 1: COUNT(*) and stop if different
    -------------------------------------------------------------------------
    DECLARE @src_count BIGINT, @tgt_count BIGINT, @diff BIGINT;
    DECLARE @testrunid VARCHAR(128) = NEWID();
    DECLARE @testdate datetime = GETDATE()

    SET @sql = N'SELECT @s = COUNT(*) FROM ' + @src_3part + N';';
    IF @debug=1 EXEC sp_executesql @PrintLongSQL, N'@s nvarchar(max)', @s=@sql;
    EXEC sp_executesql @sql, N'@s BIGINT OUTPUT', @s=@src_count OUTPUT;

    SET @sql = N'SELECT @t = COUNT(*) FROM ' + @tgt_3part + N';';
    IF @debug=1 EXEC sp_executesql @PrintLongSQL, N'@s nvarchar(max)', @s=@sql;
    EXEC sp_executesql @sql, N'@t BIGINT OUTPUT', @t=@tgt_count OUTPUT;

    SET @diff = ABS(@src_count - @tgt_count);

    INSERT INTO dbo.T_COMPARE_RESULTS
        (testname,testrunid,testdate,sourcedatabase,sourceschema,sourcetable,targetdatabase,targetschema,targettable,keycolumns,columnstested,diffcount,iscutted)
    VALUES
        (@testname, @testrunid, @testdate, @src_db, @src_schema, @src_table, @tgt_db, @tgt_schema, @tgt_table, @key_csv, N'count(*)', @diff,0);

    IF @diff <> 0
    BEGIN
        GOTO DISPLAYRESULTS;  -- counts differ -> display results and stop
        RAISERROR('Count rows are different for source and target. Stop at row count control step', 10, 1);        
    END

    -------------------------------------------------------------------------
    -- 7) Test 2: key set consistency (EXCEPT src -> tgt). Stop if any diff.
    -------------------------------------------------------------------------
    DECLARE @key_diff BIGINT = 0;
    DECLARE @cutoffquery NVARCHAR(max); 
    DECLARE @samplekeys VARCHAR(4000);
    DECLARE @is_cutted bit = 1;

    IF @cutoff = 0
        SET @cutoffquery = N'SELECT * FROM d';
    ELSE
        SET @cutoffquery = N'SELECT TOP ('+CAST(@cutoff as nvarchar(20))+') * FROM d';

    IF @getsamplekeys = 0
        BEGIN
            SET @sql = N'
                WITH d AS (
                    SELECT ' + @key_list + N'
                    FROM ' + @src_3part + N'
                    EXCEPT
                    SELECT ' + @key_list + N'
                    FROM ' + @tgt_3part + N'
                ),
                cutoffquery as ('+@cutoffquery+')
                SELECT @d = COUNT(*) , @samplekeys=NULL                       
                FROM cutoffquery
                option(hash join);';
            END
        ELSE
        BEGIN
        SET @sql = N'
            WITH d AS (
                SELECT ''only in source'' origin,' + @key_list + N'
                FROM ' + @src_3part + N'
                EXCEPT
                SELECT ''only in source'' origin,' + @key_list + N'
                FROM ' + @tgt_3part + N'
                UNION ALL
                SELECT ''only in target'' origin,' + @key_list + N'
                FROM ' + @tgt_3part + N'
                EXCEPT
                SELECT ''only in target'' origin,' + @key_list + N'
                FROM ' + @src_3part + N'
            ),
            cutoffquery as ('+@cutoffquery+')
            SELECT * INTO #cutoffquery from cutoffquery;

            WITH
            samplekey AS (
            SELECT (SELECT top 10 * FROM #cutoffquery FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS jsonsamplekeys            
            )
            SELECT @key_diff = COUNT_BIG(*), @samplekeys=MAX(jsample.jsonsamplekeys)                       
            FROM #cutoffquery cross apply (select jsonsamplekeys from samplekey) jsample;';
        END
    IF @debug=1 EXEC sp_executesql @PrintLongSQL, N'@s nvarchar(max)', @s=@sql;

    EXEC sp_executesql @sql, N'@key_diff BIGINT OUTPUT, @samplekeys VARCHAR(4000) OUTPUT', @key_diff=@key_diff OUTPUT,@samplekeys=@samplekeys OUTPUT;

    
    IF ((@cutoff=0) OR (@key_diff<@cutoff))
        SET @is_cutted=0;

    INSERT INTO dbo.T_COMPARE_RESULTS
        (testname,testrunid,testdate,sourcedatabase,sourceschema,sourcetable,targetdatabase,targetschema,targettable,keycolumns,columnstested,diffcount,samplekeysetwhere,iscutted)
    VALUES
        (@testname, @testrunid, @testdate, @src_db, @src_schema, @src_table, @tgt_db, @tgt_schema, @tgt_table, @key_csv, N'check(pk)', @key_diff,@samplekeys,@is_cutted);


    IF (@key_diff > @keydiffthreshold)
    BEGIN
        DECLARE @key_diff_string VARCHAR(21), @keydiffthreshold_string VARCHAR(21);
        SELECT @key_diff_string=CAST(@key_diff AS varchar(21)), @keydiffthreshold_string=CAST(@keydiffthreshold AS varchar(21));

        RAISERROR('Key sets differ for source and target : %s rows with differences. Stop at key control step due to keydiff threshold set at %s rows', 10,1, @key_diff_string, @keydiffthreshold_string);
        GOTO DISPLAYRESULTS;  -- key discrepancies -> display results and stop
    END;

    -------------------------------------------------------------------------
    -- 8) Common comparable columns (non-key, non text/ntext/image, non-computed)
    -------------------------------------------------------------------------
    DROP TABLE IF EXISTS #Cols;
    ;WITH common_cols AS (
        SELECT s.name,
               CASE WHEN s.system_type_id IN (167,175,231,239) THEN 1 ELSE 0 END AS is_char
			   ,s.column_id
        FROM #src_cols s
        JOIN #tgt_cols t ON t.name = s.name
        WHERE s.is_computed = 0 AND t.is_computed = 0
          AND s.system_type_id NOT IN (34,35,99)  -- image,text,ntext
          AND t.system_type_id NOT IN (34,35,99)
    )
    SELECT name, is_char, column_id
    INTO #Cols
    FROM common_cols
    WHERE name NOT IN (SELECT col FROM @Keys)
      AND (@include_columns IS NULL OR EXISTS(SELECT 1 FROM @Include i WHERE i.col = common_cols.name))
      AND NOT EXISTS(SELECT 1 FROM @Exclude e WHERE e.col = common_cols.name);

    IF @include_columns IS NOT NULL AND NOT EXISTS (SELECT 1 FROM #Cols)
    BEGIN
        RAISERROR('None of @include_columns match comparable columns.', 16, 1);
        RETURN;
    END;

    -------------------------------------------------------------------------
    -- 9) Per-column diffs: single EXCEPT (src -> tgt) per column
    -------------------------------------------------------------------------
    DECLARE @col SYSNAME, @is_char BIT, @col_count INT, @col_expr NVARCHAR(400), @col_diff BIGINT, @col_distinct BIGINT,@loopcounter INT=0,@stopwatch datetime, @elaspedtimems int;    

    


    DECLARE col_cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT name, is_char FROM #Cols ORDER BY column_id;

	
	SELECT @col_count=count(*) from #Cols;

    OPEN col_cur;
    FETCH NEXT FROM col_cur INTO @col, @is_char;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @col_expr = QUOTENAME(@col) + CASE WHEN @is_char = 1 THEN ' COLLATE Latin1_General_100_BIN2_UTF8' ELSE '' END + N' AS testcol';
        SET @loopcounter+=1;

		SET @stopwatch=GETDATE();		

        IF @getsamplekeys = 0
        BEGIN
            SET @sql = N'
                WITH d AS (
                    SELECT ' + @key_list + N', ' + @col_expr + N'
                    FROM ' + @src_3part + N'
                    EXCEPT
                    SELECT ' + @key_list + N', ' + @col_expr + N'
                    FROM ' + @tgt_3part + N'
                ),
                cutoffquery as ('+@cutoffquery+')
                SELECT @d = COUNT_BIG(*), @dd = COUNT_BIG(DISTINCT testcol) , @samplekeys=NULL                       
                FROM cutoffquery
                option(hash join);';
            END
        ELSE
        BEGIN
        SET @sql = N'
            WITH d AS (
                SELECT ' + @key_list + N', ' + @col_expr + N'
                FROM ' + @src_3part + N'
                EXCEPT
                SELECT ' + @key_list + N', ' + @col_expr + N'
                FROM ' + @tgt_3part + N'
            ),
            cutoffquery as ('+@cutoffquery+')
            SELECT * INTO #cutoffquery from cutoffquery;

            WITH
            samplekey AS (
            SELECT (SELECT top 10 '+@key_list+' FROM #cutoffquery FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS jsonsamplekeys            
            )
            SELECT @d = COUNT_BIG(*), @dd = COUNT_BIG(DISTINCT testcol) , @samplekeys=MAX(jsample.jsonsamplekeys)                       
            FROM #cutoffquery cross apply (select jsonsamplekeys from samplekey) jsample;';
        END

        IF @debug=1 EXEC sp_executesql @PrintLongSQL, N'@s nvarchar(max)', @s=@sql;

        SET @col_diff = 0;
        EXEC sp_executesql @sql, N'@d BIGINT OUTPUT, @dd BIGINT OUTPUT, @samplekeys VARCHAR(4000) OUTPUT', @d=@col_diff OUTPUT, @dd=@col_distinct OUTPUT,@samplekeys=@samplekeys OUTPUT;

        IF ((@cutoff=0) OR (@col_diff<@cutoff))
          SET @is_cutted=0;

        INSERT INTO dbo.T_COMPARE_RESULTS
            (testname,testrunid,testdate,sourcedatabase,sourceschema,sourcetable,targetdatabase, targetschema,targettable,keycolumns,columnstested,diffcount, diffdistinct,samplekeysetwhere, iscutted)
        VALUES
            (@testname, @testrunid, @testdate, @src_db, @src_schema, @src_table, @tgt_db, @tgt_schema, @tgt_table, @key_csv, @col, @col_diff, @col_distinct,@samplekeys,@is_cutted);

		SELECT @elaspedtimems = DATEDIFF(ms,@stopwatch,GETDATE());

		RAISERROR('{"Testname":"%s", "runid":"%s", "column":"%s", "elaspedtimems":%d, "step":%d, "totalsteps":%d}' , 10, 1, @testname, @testrunid, @col,@elaspedtimems, @loopcounter, @col_count) WITH NOWAIT;

        FETCH NEXT FROM col_cur INTO @col, @is_char;
    END

    CLOSE col_cur;
    DEALLOCATE col_cur;

    DROP TABLE IF EXISTS #Cols;
    DROP TABLE IF EXISTS #src_cols;
    DROP TABLE IF EXISTS #tgt_cols;

DISPLAYRESULTS:

    SELECT * FROM dbo.T_COMPARE_RESULTS where testrunid=@testrunid;

END
