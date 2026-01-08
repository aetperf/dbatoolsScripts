
-- 1) drop inactive sessions on the target database   
-- 2) drop the target database 
-- 3) rename the origin database to the target database.


-- Usage: exec sp_dba_publish_database 'old_db_name', 'target_db_name'
-- Usage exec sp_dba_publish_database @dbname_origin = 'old_db_name', @dbname_target = 'new_db_name', @mode = 'force'

CREATE OR ALTER PROCEDURE sp_dba_publish_database
    @dbname_origin sysname,
    @dbname_target sysname,
    @timeout int = 60 -- timeout in minutes
AS
BEGIN
    DECLARE @sql nvarchar(max)
    DECLARE @start_time datetime2;
    DECLARE @duration_min int;

    -- Check if the origin database exists
    IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = @dbname_origin)
    BEGIN
        RAISERROR('Error : The origin database %s does not exist.', 16, 1, @dbname_origin) WITH NOWAIT;
        RETURN
    END

    -- Check if the target database is not the same as the origin database
    IF @dbname_origin = @dbname_target
    BEGIN
        RAISERROR('Error : The target database %s is the same as the origin database.', 16, 1, @dbname_target) WITH NOWAIT;
        RETURN
    END

    -- Check if the target database exists
    IF EXISTS (SELECT name FROM sys.databases WHERE name = @dbname_target)
    BEGIN
        
        -- Drop sessions on the target database by passing the database name in single user mode   
        SET @sql = 'ALTER DATABASE ' + QUOTENAME(@dbname_target) + ' SET SINGLE_USER WITH ROLLBACK IMMEDIATE';
        RAISERROR('Dropping all sessions on the target database %s.', 1, 1, @dbname_target) WITH NOWAIT;
        RAISERROR(@sql, 1, 1) WITH NOWAIT;
        EXEC sp_executesql @sql;

        -- Drop the target database
        SET @sql = 'DROP DATABASE ' + QUOTENAME(@dbname_target);
        RAISERROR('Dropping the target database %s.', 1, 1, @dbname_target) WITH NOWAIT;
        RAISERROR(@sql, 1, 1) WITH NOWAIT;
        EXEC sp_executesql @sql;

        -- Drop sessions on the origin database by passing the database name in single user mode   
        SET @sql = 'ALTER DATABASE ' + QUOTENAME(@dbname_origin) + ' SET SINGLE_USER WITH ROLLBACK IMMEDIATE';
        RAISERROR('Dropping all sessions on the origin database %s.', 1, 1, @dbname_origin) WITH NOWAIT;
        RAISERROR(@sql, 1, 1) WITH NOWAIT;
        EXEC sp_executesql @sql;

        -- Rename the origin database to the target database
        SET @sql = 'ALTER DATABASE ' + QUOTENAME(@dbname_origin) + ' MODIFY NAME = ' + QUOTENAME(@dbname_target);
        RAISERROR('Renaming the database %s to  %s.', 1, 1, @dbname_origin, @dbname_target) WITH NOWAIT;
        RAISERROR(@sql, 1, 1) WITH NOWAIT;
        EXEC sp_executesql @sql;
        
        -- set the target database to multi user mode
        SET @sql = 'ALTER DATABASE ' + QUOTENAME(@dbname_target) + ' SET MULTI_USER';
        RAISERROR('Setting the target database %s to multi user mode.', 1, 1, @dbname_target) WITH NOWAIT;
        RAISERROR(@sql, 1, 1) WITH NOWAIT;
        EXEC sp_executesql @sql;
    END
    ELSE
    BEGIN
        -- Drop sessions on the origin database by passing the database name in single user mode   
        SET @sql = 'ALTER DATABASE ' + QUOTENAME(@dbname_origin) + ' SET SINGLE_USER WITH ROLLBACK IMMEDIATE';
        RAISERROR('Dropping all sessions on the origin database %s.', 1, 1, @dbname_origin) WITH NOWAIT;
        RAISERROR(@sql, 1, 1) WITH NOWAIT;
        EXEC sp_executesql @sql;

        -- Rename the origin database to the target database
        SET @sql = 'ALTER DATABASE ' + QUOTENAME(@dbname_origin) + ' MODIFY NAME = ' + QUOTENAME(@dbname_target);
        RAISERROR('Renaming the database %s to  %s.', 1, 1, @dbname_origin, @dbname_target) WITH NOWAIT;
        RAISERROR(@sql, 1, 1) WITH NOWAIT;
        EXEC sp_executesql @sql;

        -- set the target database to multi user mode
        SET @sql = 'ALTER DATABASE ' + QUOTENAME(@dbname_target) + ' SET MULTI_USER';
        RAISERROR('Setting the target database %s to multi user mode.', 1, 1, @dbname_target) WITH NOWAIT;
        RAISERROR(@sql, 1, 1) WITH NOWAIT;
        EXEC sp_executesql @sql;
    END
END



