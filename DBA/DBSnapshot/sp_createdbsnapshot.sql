CREATE OR ALTER PROCEDURE sp_createdbsnapshot
    @dborigin          SYSNAME,               -- Base d'origine (auparavant @dbname / @snapshotof)
    @suffix            VARCHAR(50),           -- Pour former le nom du snapshot (ex: DB_Production_Snap2025)
    @snapshotdirectory NVARCHAR(255) = NULL,  -- Chemin de destination (NULL => répertoire source)
    @execute           BIT = 0,               -- 1 => exécute la création du snapshot
    @debug             BIT = 1,               -- 1 => affiche des messages de débogage
    @continueOnError   BIT = 0                -- 1 => continue malgré les erreurs (erreurs « légères »)
WITH EXECUTE AS OWNER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @err          INT            = 0;
    DECLARE @snapshotname SYSNAME;
    DECLARE @sql          NVARCHAR(MAX)  = N'';
    DECLARE @filesClause  NVARCHAR(MAX)  = N'';
	DECLARE @ErrorMsg	  NVARCHAR(MAX);

    -----------------------------------------------------------------------
    -- 1. Vérifier que la base d'origine existe
    -----------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1
        FROM sys.databases
        WHERE name = @dborigin
    )
    BEGIN
        IF @continueOnError = 1
        BEGIN
            IF @debug = 1
                RAISERROR('La base de données source %s n''existe pas.', 1, 1, @dborigin) WITH NOWAIT;
            SET @err = 0;
        END
        ELSE
        BEGIN
            IF @debug = 1
                RAISERROR('La base de données source %s n''existe pas.', 16, 1, @dborigin) WITH NOWAIT;
            RETURN; -- On arrête ici si on ne continue pas sur erreur
        END
    END

    -----------------------------------------------------------------------
    -- 2. Générer le nom du snapshot (@dborigin + '_' + @suffix)
    -----------------------------------------------------------------------
    SET @snapshotname = @dborigin + '_' + @suffix;

    -----------------------------------------------------------------------
    -- 3. Vérifier que ce snapshot n'existe pas déjà
    -----------------------------------------------------------------------
    IF EXISTS (
        SELECT 1
        FROM sys.databases
        WHERE name = @snapshotname
    )
    BEGIN
        IF @continueOnError = 1
        BEGIN
            IF @debug = 1
                RAISERROR('Le snapshot %s existe déjà.', 1, 1, @snapshotname) WITH NOWAIT;
            SET @err = 0;
        END
        ELSE
        BEGIN
            IF @debug = 1
                RAISERROR('Le snapshot %s existe déjà.', 16, 1, @snapshotname) WITH NOWAIT;
            RETURN; -- On arrête ici si on ne continue pas sur erreur
        END
    END

    -----------------------------------------------------------------------
    -- 4. Récupération des fichiers de données (type_desc = 'ROWS') de la DB d'origine
    --    Pour chaque fichier, on va soit reprendre son répertoire source,
    --    soit utiliser le chemin passé en paramètre.
    -----------------------------------------------------------------------
    ;WITH DbFiles AS
    (
        SELECT
            mf.[name]         AS LogicalName,
            mf.[physical_name],
            /* Extraire le chemin du répertoire avant la dernière '\' */
            DirectoryPath = LEFT(mf.[physical_name],
                                 LEN(mf.[physical_name]) 
                                 - CHARINDEX('\', REVERSE(mf.[physical_name]))),
            ROW_NUMBER() OVER (ORDER BY mf.[file_id]) AS rn
        FROM sys.master_files mf
        WHERE mf.database_id = DB_ID(@dborigin)
          AND mf.type_desc = 'ROWS'  -- Exclut les fichiers LOG
    )
    SELECT 
        @filesClause = 
            IIF(rn>1,COALESCE(@filesClause + ',', ''),'')
            + CHAR(13) + CHAR(10)
            + '('
            + 'NAME = ''' + LogicalName + ''', '
            + 'FILENAME = '''
                + CASE 
                    WHEN @snapshotdirectory IS NULL
                        THEN DirectoryPath           -- On reprend le répertoire d'origine du fichier
                        ELSE @snapshotdirectory      -- On utilise le répertoire spécifié
                  END
                + '\' + LogicalName + '.ss'''
            + ')'
    FROM DbFiles
    ORDER BY rn;

    -- Si aucun fichier de données n'est trouvé
    IF @filesClause IS NULL OR @filesClause = ''
    BEGIN
        IF @continueOnError = 1
        BEGIN
            IF @debug = 1
                RAISERROR('Aucun fichier de données trouvé pour la base %s (type_desc = ''ROWS'').', 1, 1, @dborigin) WITH NOWAIT;
            SET @err = 0;
        END
        ELSE
        BEGIN
            IF @debug = 1
                RAISERROR('Aucun fichier de données trouvé pour la base %s (type_desc = ''ROWS'').', 16, 1, @dborigin) WITH NOWAIT;
            RETURN;
        END
    END

    -----------------------------------------------------------------------
    -- 5. Construire la commande CREATE DATABASE <snapshot> AS SNAPSHOT OF <dborigin>
    -----------------------------------------------------------------------
    IF @err = 0
    BEGIN
        BEGIN TRY
            SET @sql = N'CREATE DATABASE ' 
                       + QUOTENAME(@snapshotname)
                       + N' ON '
                       + @filesClause
                       + N' AS SNAPSHOT OF '
                       + QUOTENAME(@dborigin);

            IF @debug = 1
            BEGIN
                RAISERROR('Création du snapshot %s. Commande : %s', 1, 1, @snapshotname, @sql) WITH NOWAIT;
            END

            -----------------------------------------------------------------------
            -- 6. Exécuter la commande si @execute = 1
            -----------------------------------------------------------------------
            IF @execute = 1
            BEGIN
                EXEC sys.sp_executesql @sql;
                IF @debug = 1
                    RAISERROR('Snapshot %s créé avec succès.', 1, 1, @snapshotname) WITH NOWAIT;
            END
        END TRY
        BEGIN CATCH
			SET @ErrorMsg = ERROR_MESSAGE();
            IF @continueOnError = 0
            BEGIN
                IF @debug = 1
                    RAISERROR('Erreur lors de la création du snapshot %s : %s', 16, 1, @snapshotname,@ErrorMsg );
                RETURN;
            END
            ELSE
            BEGIN
                IF @debug = 1
                    RAISERROR('Erreur lors de la création du snapshot %s : %s', 1, 1, @snapshotname, @ErrorMsg) WITH NOWAIT;
                RETURN;
            END
        END CATCH
    END
END
GO
