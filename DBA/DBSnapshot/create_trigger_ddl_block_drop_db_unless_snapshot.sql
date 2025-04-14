USE master;
GO

-- S'il existe déjà un trigger portant ce nom, on le supprime pour le recréer
IF EXISTS (
    SELECT name 
    FROM sys.server_triggers 
    WHERE name = 'ddl_block_drop_db_unless_snapshot'
)
BEGIN
    DROP TRIGGER ddl_block_drop_db_unless_snapshot ON ALL SERVER;
END;
GO

CREATE TRIGGER ddl_block_drop_db_unless_snapshot
ON ALL SERVER
FOR DROP_DATABASE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EventData XML = EVENTDATA();
    DECLARE @DbName SYSNAME = @EventData.value('(/EVENT_INSTANCE/DatabaseName)[1]', 'SYSNAME');

    -- Vérifie si l'utilisateur courant est sysadmin 
    IF IS_SRVROLEMEMBER('sysadmin') <> 1
    BEGIN
        -- On va récupérer l'ID de la base source.
        -- Si la base n'est pas un snapshot, source_database_id sera NULL.
        IF (
            SELECT source_database_id 
            FROM sys.databases 
            WHERE name = @DbName
        ) IS NULL
        BEGIN
            RAISERROR('Vous n''êtes pas autorisé à supprimer cette base de données (non snapshot).', 16, 1);
            ROLLBACK;
        END
    END
END;
GO
