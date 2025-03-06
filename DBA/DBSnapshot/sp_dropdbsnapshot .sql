CREATE OR ALTER PROCEDURE sp_dropdbsnapshot 
    @dbsnapshotname     sysname,                    -- Snapshot Name
    @execute            BIT = 0,                    -- 1 => Execute drop snapshot
    @force              BIT = 0,                    -- 1 => kill existing sessions on the snapshot db if any
    @debug              BIT = 1,                    -- 1 => debug messages are printed
    @continueOnError    BIT = 1                     -- 1 => continue if any error. the procedure will return 0 even if error are encountered
WITH EXECUTE AS OWNER
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;

	DECLARE @sql            NVARCHAR(MAX);
	DECLARE @ErrMsg			NVARCHAR(MAX);
	DECLARE @session_id     INT;

     
    -----------------------------------------------------------------------
    -- 1. Check if the snapshot exists
    -----------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1 
        FROM sys.databases 
        WHERE QUOTENAME(name) = QUOTENAME(@dbsnapshotname) 
	AND source_database_id is not null -- to be sure the snahoshotname given in parameter is a snapshot and not a real database
    )
    BEGIN
		IF @continueOnError=1
			BEGIN
				RAISERROR('The snapshot %s does not exist', 1, 1, @dbsnapshotname) WITH NOWAIT;
				RETURN 0
			END
		ELSE
			BEGIN
				RAISERROR('The snapshot %s does not exist', 16, 1, @dbsnapshotname) WITH NOWAIT;
				RETURN 1
			END
    END
	
    -----------------------------------------------------------------------
    -- 2. Kill sessions on the snapshot database if any
    -----------------------------------------------------------------------
    IF @force = 1 
    BEGIN
        
        DECLARE session_cursor CURSOR FOR
        SELECT session_id
        FROM sys.dm_exec_sessions
        WHERE database_id = DB_ID(@dbsnapshotname);

        OPEN session_cursor;
        FETCH NEXT FROM session_cursor INTO @session_id;

        WHILE @@FETCH_STATUS = 0
        BEGIN            
            SET @sql = 'KILL ' + CAST(@session_id AS NVARCHAR(10));
            IF @execute=1
				EXEC sp_executesql @sql;
            IF @debug=1
				RAISERROR('kill session %d', 1, 1, @session_id) WITH NOWAIT;
            FETCH NEXT FROM session_cursor INTO @session_id;
        END
        CLOSE session_cursor;
        DEALLOCATE session_cursor;
    END

    -----------------------------------------------------------------------
    -- 3. Drop the snapshot database
    -----------------------------------------------------------------------
    
        BEGIN TRY			
			SET @sql = 'DROP DATABASE '+ QUOTENAME(@dbsnapshotname) +';';
			IF @debug=1
				PRINT @sql;
            IF @execute=1
			BEGIN
				EXEC sp_executesql @sql;
				RAISERROR('Drop snapshot %s completed', 10, 1,@dbsnapshotname);
			END
			RETURN 0;
        END TRY
        BEGIN CATCH
			SET @ErrMsg = ERROR_MESSAGE();            
            IF @continueOnError = 0 
            BEGIN
                RAISERROR('Error when dropping the snapshot %s : %s', 10, 1,@dbsnapshotname,@ErrMsg);
                RETURN 0;
            END
			ELSE 
			BEGIN
				RAISERROR('Error when dropping the snapshot  %s : %s', 16, 1, @dbsnapshotname,@ErrMsg) WITH NOWAIT;
				RETURN 1;
			END
		END CATCH
END
