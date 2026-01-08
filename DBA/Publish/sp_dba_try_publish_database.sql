-- A store procedure that will check if there is any active session in the origin database and if not, it will :
-- 1) Check if the target database exists and if so, it will
-- 2) check if there is any active session on the target database and if so will loop and wait 5 seconds until there is no active session.
-- 3) drop inactive sessions on the target database   
-- 3) drop the target database 
-- 4) rename the origin database to the target database.


-- Usage: exec sp_dba_try_publish_database 'old_db_name', 'target_db_name'
-- Usage exec sp_dba_try_publish_database @dbname_origin = 'old_db_name', @dbname_target = 'new_db_name', @mode = 'force'

CREATE OR ALTER PROCEDURE sp_dba_try_publish_database
    @dbname_origin sysname,
    @dbname_target sysname,
    @mode varchar(10) = 'normal',
    @timeout int = 60 -- timeout in minutes
AS
BEGIN
    DECLARE @sql nvarchar(max)
    DECLARE @start_time datetime2;
    DECLARE @duration_min int;

    -- Check if the origin database exists
    IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = @dbname_origin)
    BEGIN
        RAISERROR('The origin database %s does not exist.', 16, 1, @dbname_origin) WITH NOWAIT;
        RETURN
    END

    -- Check if the target database exists
    IF EXISTS (SELECT name FROM sys.databases WHERE name = @dbname_target)
    BEGIN
        IF @mode <> 'force'
        BEGIN
            -- Wait and Check if there is any active session on the target database
            WHILE EXISTS (SELECT 1 FROM sys.dm_exec_sessions WHERE database_id = DB_ID(@dbname_target) and status not in ('sleeping'))
            BEGIN
                RAISERROR('There are still active sessions on the target database %s. Waiting for 5 seconds.', 1, 1, @dbname_target) WITH NOWAIT;
                WAITFOR DELAY '00:00:05'
                SET @duration_min = DATEDIFF(MINUTE, @start_time, GETDATE());
                IF @duration_min > @timeout
                BEGIN
                    RAISERROR('Timeout: There are still active sessions on the target database %s.', 16, 1, @dbname_target) WITH NOWAIT;
                    RETURN;
                END
            END

            exec sp_dba_publish_database @dbname_origin = @dbname_origin, @dbname_target = @dbname_target;
        END
        ELSE
        BEGIN
            RAISERROR('Force mode', 1, 1, @dbname_target) WITH NOWAIT;
            exec sp_dba_publish_database @dbname_origin = @dbname_origin, @dbname_target = @dbname_target;
        END
    END
    ELSE
    BEGIN
        RAISERROR('%s target not exist in the instance. Publish directly', 1, 1, @dbname_target) WITH NOWAIT;
        exec sp_dba_publish_database @dbname_origin = @dbname_origin, @dbname_target = @dbname_target;
    END
END



