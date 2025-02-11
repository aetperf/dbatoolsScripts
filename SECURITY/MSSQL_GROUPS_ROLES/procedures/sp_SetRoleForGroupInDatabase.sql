CREATE OR ALTER PROCEDURE [security].sp_SetRoleForGroupInDatabase
    @GroupName NVARCHAR(100),
    @DatabaseName NVARCHAR(128),
    @Profile NVARCHAR(10)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @SuperGroupID INT;
    DECLARE @GroupID INT;
   

    -- Vérification du profil

    IF @Profile NOT IN ('Reader', 'Writer', 'Admin')
    BEGIN
        RAISERROR('Le profil spécifié est invalide. Valeurs acceptées : Reader, Writer, Admin.', 16, 1);
        RETURN;
    END
  

    -- Vérification de l'existence de la database
    IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @DatabaseName)
    BEGIN
        RAISERROR('La base de données %s n''existe pas.', 16, 1, @DatabaseName);
        RETURN;
    END

    -- Récupération du GroupID 
    SELECT @GroupID = GroupID FROM [security].Groups WHERE GroupName = @GroupName;
  
    -- Vérification de l'existence des groupes
    IF (@GroupID IS NULL)
    BEGIN
        RAISERROR('Le groupe %s n''existe pas.', 16, 1, @GroupName);
        RETURN;
    END

    -- utilisation d'une table variable contenant les dbroles en fonction des profiles
    DECLARE @ProfileDbRoles AS TABLE (ProfileName NVARCHAR(10), DBRole NVARCHAR(100));
    INSERT INTO @ProfileDbRoles (ProfileName, DBRole)
    VALUES ('Reader', 'db_datareader'),
           ('Writer', 'db_datawriter'),
           ('Writer', 'db_ddladmin'),
           ('Admin', 'db_owner');

    -- Insertion des roles dans la table GroupDatabaseDBRoles en fonction du profile
    INSERT INTO [security].GroupDatabaseDBRoles (GroupID, DatabaseName, DBRole)
    SELECT @GroupID, @DatabaseName, DBRole
    FROM @ProfileDbRoles
    WHERE ProfileName = @Profile
    AND NOT EXISTS (
        SELECT 1
        FROM [security].GroupDatabaseDBRoles gdr INNER JOIN @ProfileDbRoles vp ON (gdr.DBRole = vp.DBRole)
        WHERE GroupID = @GroupID
        AND DatabaseName = @DatabaseName
        AND vp.ProfileName = @Profile
    );



    
END;
