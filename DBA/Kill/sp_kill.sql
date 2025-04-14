CREATE PROCEDURE dbo.sp_kill
    @sessionId INT
AS
BEGIN
    SET NOCOUNT ON;

    ---------------------------------------------------------------------
    -- 1) Vérifier que la session existe encore
    ---------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1
        FROM sys.dm_exec_sessions
        WHERE session_id = @sessionId
    )
    BEGIN
        RAISERROR('La session %d n''existe pas ou n''est plus active.', 16, 1, @sessionId);
        RETURN;
    END;

    ---------------------------------------------------------------------
    -- 2) Récupérer les infos de la session
    ---------------------------------------------------------------------
    DECLARE @LoginName NVARCHAR(128),
            @DBName    SYSNAME;

    SELECT
        @LoginName = s.login_name,
        @DBName    = DB_NAME(s.database_id)
    FROM sys.dm_exec_sessions s
    WHERE s.session_id = @sessionId;

    ---------------------------------------------------------------------
    -- 3) Appliquer la logique de vérification
    --    a) même login ?
    --    b) ou membre db_owner sur la base visée ?
    ---------------------------------------------------------------------
    DECLARE @IsAuthorized BIT = 0; 

    -- Vérif a) même login
    IF @LoginName = SUSER_SNAME()
    BEGIN
        SET @IsAuthorized = 1;
    END
    ELSE
    BEGIN
        -- Vérif b) membre du rôle db_owner dans la base @DBName
        DECLARE @IsDbOwner INT = 0;
        DECLARE @sql NVARCHAR(MAX) = N'
            USE ' + QUOTENAME(@DBName) + ';
            SELECT @retour = IS_MEMBER(''db_owner'');
        ';

        BEGIN TRY
            EXEC sp_executesql
                @sql,
                N'@retour INT OUTPUT',
                @retour = @IsDbOwner OUTPUT;
        END TRY
        BEGIN CATCH
            DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
            RAISERROR('Impossible de vérifier le rôle db_owner : %s', 16, 1, @ErrMsg);
            RETURN;
        END CATCH;

        IF @IsDbOwner = 1
        BEGIN
            SET @IsAuthorized = 1;
        END
    END

    ---------------------------------------------------------------------
    -- 4) S'il n'est PAS autorisé, on lève une erreur et on arrête
    ---------------------------------------------------------------------
    IF @IsAuthorized = 0
    BEGIN
        RAISERROR(
            'Vous n''avez pas les droits pour tuer la session %d (Base: %s, Login: %s).',
            16, 1,
            @sessionId,
            @DBName,
            @LoginName
        );
        RETURN;
    END

    ---------------------------------------------------------------------
    -- 5) Sinon, on exécute la procédure interne qui fait le KILL
    ---------------------------------------------------------------------
    EXEC sp_kill_internal @sessionId = @sessionId;
END;
GO


