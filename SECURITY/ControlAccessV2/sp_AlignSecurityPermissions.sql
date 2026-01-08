USE [DBATOOLS]

GO

/****** Object:  StoredProcedure [security].[AlignSecurityPermissions]    Script Date: 08/12/2025 10:11:49 ******/

SET ANSI_NULLS ON

GO

SET QUOTED_IDENTIFIER ON

GO

 

CREATE   PROCEDURE [security].[sp_AlignSecurityPermissions]

    @IgnoreRoles NVARCHAR(MAX) = NULL,       -- Liste de patterns de rôles à ignorer

    @IncludeLogins NVARCHAR(MAX) = NULL,    -- Liste de patterns de logins à inclure (si NULL, tous sont inclus)

    @ExcludeLogins NVARCHAR(MAX) = NULL,    -- Liste de patterns de logins à exclure

              @IncludeDatabases NVARCHAR(MAX) = NULL,    -- Liste de patterns de bases à inclure (si NULL, tous sont inclus)

    @ExcludeDatabases NVARCHAR(MAX) = NULL,    -- Liste de patterns de base à exclure

    @Execute CHAR(1) = 'N',                  -- 'Y' ou 'N'

    @GrantorName NVARCHAR(MAX) = NULL ,       -- Liste de patterns sur Grantor

              @DatabaseGroupName NVARCHAR(MAX) = NULL  --Liste de patterns de group de base (si NULL, toutes les bases même celle qui ne sont pas dans un groupe)

AS

BEGIN

    SET NOCOUNT ON;

 

    -- Création de la table de logs si elle n'existe pas

    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AlignSecurityPermissionsLogs' AND schema_id = SCHEMA_ID('security'))

    BEGIN

        CREATE TABLE [security].[AlignSecurityPermissionsLogs] (

            LogId INT IDENTITY(1,1) PRIMARY KEY,

            RunId UNIQUEIDENTIFIER NOT NULL,

            ExecuteDate DATETIME NOT NULL,

            DatabaseName NVARCHAR(255),

            LoginName NVARCHAR(255),

            RoleName NVARCHAR(255),

            AlignCommandStatus INT NOT NULL, -- -1: Execute=N, 0: OK, 1: Erreur

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

 

    -- Vérification du paramètre @Execute

    IF @Execute NOT IN ('Y', 'N')

    BEGIN

        RAISERROR('Le paramètre @Execute doit être ''Y'' ou ''N''.', 16, 1);

        RETURN;

    END

 

    -- Création d'une table temporaire pour stocker les actions à effectuer

    CREATE TABLE #ActionsToAlign (

        DatabaseName NVARCHAR(255),

        LoginName NVARCHAR(255),

        RoleName NVARCHAR(255),

        SQLStatement NVARCHAR(MAX),

        UndoSQLStatement NVARCHAR(MAX)

    );

 

    -- Remplissage de la table temporaire avec les actions à effectuer,

    -- en filtrant sur les groupes de bases, les rôles ignorés, et les logins à inclure/exclure

    INSERT INTO #ActionsToAlign (DatabaseName, LoginName, RoleName, SQLStatement, UndoSQLStatement)

    SELECT DISTINCT

        v.DatabaseName,

        v.LoginName,

        v.RoleName,

        v.SQLStatement,

        v.UndoSQLStatement

    FROM [security].[VW_CheckSecurityComparison] v

              LEFT JOIN [security].[DatabaseGroup] d ON v.DatabaseName=d.DatabaseName

    WHERE

        v.SQLStatement IS NOT NULL -- On ne prend que les lignes où une action est nécessaire

        -- Filtrage des rôles ignorés

        AND (

            @IgnoreRoles IS NULL

            OR NOT EXISTS (

                SELECT 1

                FROM STRING_SPLIT(@IgnoreRoles, ',') AS s

                WHERE v.RoleName LIKE REPLACE(s.value, '''', '''''')

            )

        )

        -- Filtrage des grantors

        AND (

                                          @GrantorName IS NULL

                                          OR v.Grantor IS NULL

                                          OR EXISTS(

                                                         SELECT 1

                                                         FROM STRING_SPLIT(@GrantorName, ',') AS s

                                                         WHERE v.Grantor LIKE REPLACE(s.value, '''', '''''')

                                          )

        )

                            -- Filtrage des groupes de bases

        AND (

                                          @DatabaseGroupName IS NULL

                                          OR EXISTS(

                                                         SELECT 1

                                                         FROM STRING_SPLIT(@DatabaseGroupName, ',') AS s

                                                         WHERE d.DatabaseGroupName LIKE REPLACE(s.value, '''', '''''')

                                          )

        )

                            -- Filtrage des bases à inclure (si @IncludeDatabases est fourni)

        AND (

            @IncludeDatabases IS NULL

            OR EXISTS (

                SELECT 1

                FROM STRING_SPLIT(@IncludeDatabases, ',') AS s

                WHERE v.DatabaseName LIKE REPLACE(s.value, '''', '''''')

            )

        )

        -- Filtrage des bases à exclure (si @ExcludeDatabases est fourni)

        AND (

            @ExcludeDatabases IS NULL

            OR NOT EXISTS (

                SELECT 1

                FROM STRING_SPLIT(@ExcludeDatabases, ',') AS s

                WHERE v.DatabaseName LIKE REPLACE(s.value, '''', '''''')

            )

        )

        -- Filtrage des logins à inclure (si @IncludeLogins est fourni)

        AND (

            @IncludeLogins IS NULL

            OR EXISTS (

                SELECT 1

                FROM STRING_SPLIT(@IncludeLogins, ',') AS s

                WHERE v.LoginName LIKE REPLACE(s.value, '''', '''''')

            )

        )

        -- Filtrage des logins à exclure (si @ExcludeLogins est fourni)

        AND (

            @ExcludeLogins IS NULL

            OR NOT EXISTS (

                SELECT 1

                FROM STRING_SPLIT(@ExcludeLogins, ',') AS s

                WHERE v.LoginName LIKE REPLACE(s.value, '''', '''''')

            )

        );

 

    -- Pour chaque action, on loggue et on exécute si @Execute = 'Y'

    DECLARE ActionCursor CURSOR FOR

    SELECT DatabaseName, LoginName, RoleName, SQLStatement, UndoSQLStatement

    FROM #ActionsToAlign;

 

    OPEN ActionCursor;

    FETCH NEXT FROM ActionCursor INTO @DatabaseName, @LoginName, @RoleName, @CurrentSQLStatement, @CurrentUndoSQLStatement;

 

    WHILE @@FETCH_STATUS = 0

    BEGIN

        SET @AlignCommandStatus = CASE WHEN @Execute = 'N' THEN -1 ELSE 0 END;

        SET @ErrorMessage = NULL;

 

        -- Log de l'action (même si on n'exécute pas)

        INSERT INTO [security].[AlignSecurityPermissionsLogs] (

            RunId, ExecuteDate, DatabaseName, LoginName, RoleName,

            AlignCommandStatus, SQLStatement, UndoSQLStatement, ErrorMessage

        )

        VALUES (

            @RunId, @ExecuteDate, @DatabaseName, @LoginName, @RoleName,

            @AlignCommandStatus, @CurrentSQLStatement, @CurrentUndoSQLStatement, @ErrorMessage

        );

 

        -- Exécution si @Execute = 'Y'

       

        BEGIN

            BEGIN TRY

                                                         PRINT @CurrentSQLStatement;

                                                         IF @Execute = 'Y'

                                                                       EXEC sp_executesql @CurrentSQLStatement;

            END TRY

            BEGIN CATCH

                SET @AlignCommandStatus = 1;

                SET @ErrorMessage = ERROR_MESSAGE();

                -- Mise à jour du log avec l'erreur

                UPDATE [security].[AlignSecurityPermissionsLogs]

                SET AlignCommandStatus = @AlignCommandStatus, ErrorMessage = @ErrorMessage

                WHERE LogId = SCOPE_IDENTITY();

            END CATCH

         END

 

        FETCH NEXT FROM ActionCursor INTO @DatabaseName, @LoginName, @RoleName, @CurrentSQLStatement, @CurrentUndoSQLStatement;

    END

 

    CLOSE ActionCursor;

    DEALLOCATE ActionCursor;

 

    -- Retourne le RunId pour permettre de suivre les logs

    SELECT @RunId AS RunId;

END