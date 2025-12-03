USE [DBATOOLS]
GO

/****** Object:  StoredProcedure [security].[AlignSecurityPermissions]    Script Date: 13/11/2025 11:42:55 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [security].[AlignSecurityPermissions]
    @IgnoreRoles NVARCHAR(MAX) = NULL,       -- List of role patterns to ignore
    @IncludeLogins NVARCHAR(MAX) = NULL,     -- List of login patterns to include (if NULL, all are included)
    @ExcludeLogins NVARCHAR(MAX) = NULL,     -- List of login patterns to exclude
    @Execute CHAR(1) = 'N',                  -- 'Y' or 'N'
    @DatabaseGroupName NVARCHAR(MAX)         -- List of database group patterns
AS
BEGIN
    SET NOCOUNT ON;

    -- Create the log table if it does not exist
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AlignSecurityPermissionsLogs' AND schema_id = SCHEMA_ID('security'))
    BEGIN
        CREATE TABLE [security].[AlignSecurityPermissionsLogs] (
            LogId INT IDENTITY(1,1) PRIMARY KEY,
            RunId UNIQUEIDENTIFIER NOT NULL,
            ExecuteDate DATETIME NOT NULL,
            DatabaseName NVARCHAR(255),
            LoginName NVARCHAR(255),
            RoleName NVARCHAR(255),
            AlignCommandStatus INT NOT NULL, -- -1: Execute=N, 0: OK, 1: Error
            SQLStatement NVARCHAR(MAX),
            UndoSQLStatement NVARCHAR(MAX),
            ErrorMessage NVARCHAR(MAX)
        );
    END

    DECLARE @RunId UNIQUEIDENTIFIER = NEWID();
    DECLARE @ExecuteDate DATETIME = GETDATE();
    DECLARE @ErrorMessage NVARCHAR(MAX);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @DatabaseName NVARCHAR(255);
    DECLARE @LoginName NVARCHAR(255);
    DECLARE @RoleName NVARCHAR(255);
    DECLARE @AlignCommandStatus INT;
    DECLARE @CurrentSQLStatement NVARCHAR(MAX);
    DECLARE @CurrentUndoSQLStatement NVARCHAR(MAX);

    -- Check the @Execute parameter
    IF @Execute NOT IN ('Y', 'N')
    BEGIN
        RAISERROR('The @Execute parameter must be ''Y'' or ''N''.', 16, 1);
        RETURN;
    END

    -- Create a temporary table to store actions to be performed
    CREATE TABLE #ActionsToAlign (
        DatabaseName NVARCHAR(255),
        LoginName NVARCHAR(255),
        RoleName NVARCHAR(255),
        SQLStatement NVARCHAR(MAX),
        UndoSQLStatement NVARCHAR(MAX)
    );

    -- Fill the temporary table with actions to be performed,
    -- filtering on database groups, ignored roles, and logins to include/exclude
    INSERT INTO #ActionsToAlign (DatabaseName, LoginName, RoleName, SQLStatement, UndoSQLStatement)
    SELECT DISTINCT
        v.DatabaseName,
        v.LoginName,
        v.RoleName,
        v.SQLStatement,
        v.UndoSQLStatement
    FROM [security].[VW_CheckSecurityComparison] v
    JOIN [security].[DatabaseGroup] d ON v.DatabaseName = d.DatabaseName
    WHERE
        v.SQLStatement IS NOT NULL -- Only take rows where an action is needed
        -- Filter ignored roles
        AND (
            @IgnoreRoles IS NULL
            OR NOT EXISTS (
                SELECT 1
                FROM STRING_SPLIT(@IgnoreRoles, ',') AS s
                WHERE v.RoleName LIKE REPLACE(s.value, '''', '''''')
            )
        )
        -- Filter database groups
        AND EXISTS (
            SELECT 1
            FROM STRING_SPLIT(@DatabaseGroupName, ',') AS s
            WHERE d.DatabaseGroupName LIKE REPLACE(s.value, '''', '''''')
        )
        -- Filter logins to include (if @IncludeLogins is provided)
        AND (
            @IncludeLogins IS NULL
            OR EXISTS (
                SELECT 1
                FROM STRING_SPLIT(@IncludeLogins, ',') AS s
                WHERE v.LoginName LIKE REPLACE(s.value, '''', '''''')
            )
        )
        -- Filter logins to exclude (if @ExcludeLogins is provided)
        AND (
            @ExcludeLogins IS NULL
            OR NOT EXISTS (
                SELECT 1
                FROM STRING_SPLIT(@ExcludeLogins, ',') AS s
                WHERE v.LoginName LIKE REPLACE(s.value, '''', '''''')
            )
        );

    -- For each action, log and execute if @Execute = 'Y'
    DECLARE ActionCursor CURSOR FOR
    SELECT DatabaseName, LoginName, RoleName, SQLStatement, UndoSQLStatement
    FROM #ActionsToAlign;

    OPEN ActionCursor;
    FETCH NEXT FROM ActionCursor INTO @DatabaseName, @LoginName, @RoleName, @CurrentSQLStatement, @CurrentUndoSQLStatement;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @AlignCommandStatus = CASE WHEN @Execute = 'N' THEN -1 ELSE 0 END;
        SET @ErrorMessage = NULL;

        -- Log the action (even if not executed)
        INSERT INTO [security].[AlignSecurityPermissionsLogs] (
            RunId, ExecuteDate, DatabaseName, LoginName, RoleName,
            AlignCommandStatus, SQLStatement, UndoSQLStatement, ErrorMessage
        )
        VALUES (
            @RunId, @ExecuteDate, @DatabaseName, @LoginName, @RoleName,
            @AlignCommandStatus, @CurrentSQLStatement, @CurrentUndoSQLStatement, @ErrorMessage
        );

        -- Execute if @Execute = 'Y'
        IF @Execute = 'Y'
        BEGIN
            BEGIN TRY
                EXEC sp_executesql @CurrentSQLStatement;
            END TRY
            BEGIN CATCH
                SET @AlignCommandStatus = 1;
                SET @ErrorMessage = ERROR_MESSAGE();
                -- Update the log with the error
                UPDATE [security].[AlignSecurityPermissionsLogs]
                SET AlignCommandStatus = @AlignCommandStatus, ErrorMessage = @ErrorMessage
                WHERE LogId = SCOPE_IDENTITY();
            END CATCH
        END

        FETCH NEXT FROM ActionCursor INTO @DatabaseName, @LoginName, @RoleName, @CurrentSQLStatement, @CurrentUndoSQLStatement;
    END

    CLOSE ActionCursor;
    DEALLOCATE ActionCursor;

    -- Return the RunId to allow tracking of logs
    SELECT @RunId AS RunId;
END
GO