CREATE OR ALTER PROCEDURE [security].sp_ApplyPermissionsForGroupMembers
    @GroupName NVARCHAR(100) = NULL,
    @Execute BIT = 1,
    @Debug BIT = 0
AS
BEGIN


    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @GroupID INT;
    DECLARE @UserLogin NVARCHAR(255);
    DECLARE @DatabaseName NVARCHAR(128);
    DECLARE @DBRole NVARCHAR(100);
    DECLARE @SQLDYN NVARCHAR(MAX);
    DECLARE @SQLDYN_TEMPLATE NVARCHAR(MAX);

    SET @SQLDYN_TEMPLATE = N'USE [<dbname>];
    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = ''<user>'')
    BEGIN
        CREATE USER [<user>] FOR LOGIN [<user>];
    END
    ALTER ROLE <role> ADD MEMBER [<user>];';

    DECLARE @SQLCMD NVARCHAR(MAX);
    DECLARE @ERRORMSG NVARCHAR(MAX);

    -- Récupération de l'ID du groupe   
    SELECT @GroupID = GroupID FROM [security].Groups WHERE GroupName = @GroupName;

    -- Vérification de l'existence du groupe
    IF (@GroupID IS NULL)
    BEGIN
        RAISERROR('Le groupe %s n''existe pas.', 16, 1, @GroupName);
        RETURN;
    END

    -- Récupéartion des membres du groupe, des bases de données et des rôles associés
    DECLARE security_cursor CURSOR FOR
    SELECT u.UserLogin, gdr.DatabaseName, gdr.DBRole
    FROM 
    [security].GroupDatabaseDBRoles gdr
    INNER JOIN [security].GroupMembers gm ON gdr.GroupID = gm.GroupID
    INNER JOIN [security].Groups g ON gdr.GroupID = g.GroupID
    INNER JOIN [security].Users u ON gm.UserID = u.UserID
    WHERE g.GroupID = @GroupID;

    OPEN security_cursor;
    FETCH NEXT FROM security_cursor INTO @UserLogin, @DatabaseName, @DBRole;

    WHILE @@FETCH_STATUS = 0

    BEGIN
        --utilisation de SQL dynamique via templating pour entrée dans la database USE @DatabaseName
        -- creation du user s'il n'existe pas déja dans la database
        -- et application des permissions GRANT @DBRole TO @UserLogin
        SET @SQLCMD = 
        REPLACE(
            REPLACE(
                REPLACE(@SQLDYN_TEMPLATE, 
                 '<dbname>', @DatabaseName),
                 '<user>', @UserLogin), 
                 '<role>', @DBRole);

        IF @Debug = 1
            PRINT @SQLCMD;

        IF @Execute = 1
        BEGIN
            BEGIN TRY
                EXEC sp_executesql @SQLCMD;
                RAISERROR('Role %s pour %s sur %s.', 10, 1, @DBRole, @UserLogin, @DatabaseName);
            END TRY
            BEGIN CATCH
                SET @ERRORMSG = ERROR_MESSAGE();
                RAISERROR('Erreur lors de l''application du role %s pour %s sur %s.', 12, 1, @DBRole, @UserLogin, @DatabaseName);
                RAISERROR(@ERRORMSG, 16, 1);
            END CATCH
        END
    
        FETCH NEXT FROM security_cursor INTO @UserLogin, @DatabaseName, @DBRole;
    END

    CLOSE security_cursor;
    DEALLOCATE security_cursor;

END



