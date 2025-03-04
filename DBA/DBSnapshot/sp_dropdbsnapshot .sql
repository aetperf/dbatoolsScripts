CREATE OR ALTER PROCEDURE sp_dropdbsnapshot 
    @dbsnapshotname sysname, 
    @execute bit = 0, 
    @force bit = 0, 
    @debug bit = 1, 
    @continueOnError bit = 1
AS
BEGIN
    DECLARE @err INT = 0;
	DECLARE @sql VARCHAR(MAX);

    -- Vérifier si le snapshot existe
    IF NOT EXISTS (
        SELECT 1 
        FROM sys.databases 
        WHERE name = @dbsnapshotname
    )
    BEGIN
		IF @continueOnError=1
			BEGIN
				IF @debug=1
					RAISERROR('La snapshot %s n''existe pas', 1, 1, @dbsnapshotname) WITH NOWAIT;
				SET @err = 0;
			END
		ELSE
			BEGIN
				IF @debug=1
					RAISERROR('La snapshot %s n''existe pas', 16, 1, @dbsnapshotname) WITH NOWAIT;
				SET @err = 1;
				RETURN
			END
    END

    -- Si on demande de forcer, mettre la base en mode SINGLE_USER et déconnecter les utilisateurs
    /*IF @force = 1 AND @err = 0
    BEGIN
        BEGIN TRY
			SET @sql = 'USE MASTER GO ALTER DATABASE '+QUOTENAME(@dbsnapshotname)+' SET SINGLE_USER GO;'
			IF @debug = 1
				RAISERROR('Passage de la snapshot en SINGLE_USER : %s', 1, 1, @sql) WITH NOWAIT;
            EXEC sp_executesql @sql;
        END TRY
        BEGIN CATCH
            SET @err = 1;
            IF @continueOnError = 0 
            BEGIN
                RAISERROR('Erreur lors du passage en mode SINGLE_USER', 16, 1) WITH NOWAIT;
                RETURN;
            END
			ELSE
			BEGIN
				RAISERROR('Erreur lors du passage en mode SINGLE_USER', 1, 1) WITH NOWAIT;
			END
        END CATCH
    END*/

    -- Si tout est ok, supprimer le snapshot
    IF @err = 0
    BEGIN
        BEGIN TRY
			IF @debug = 1
				RAISERROR('Suppression du database snapshot %s', 1, 1, @dbsnapshotname) WITH NOWAIT;
            EXEC('DROP DATABASE ' + @dbsnapshotname);
        END TRY
        BEGIN CATCH
            SET @err = 1;
           
				RAISERROR('Erreur lors de la suppression du database snapshot %s', 1, 1, @dbsnapshotname) WITH NOWAIT;
            IF @continueOnError = 0 
            BEGIN
                RAISERROR('Erreur lors de la suppression du snapshot', 16, 1);
                RETURN;
            END
			ELSE 
			BEGIN
				IF @debug=1
					RAISERROR('Erreur lors de la suppression du database snapshot %s', 1, 1, @dbsnapshotname) WITH NOWAIT;	
			END
		END CATCH
    END

    -- Retourner 0 si on continue malgré l'erreur ou pas d'erreur
    IF @err = 0 OR @continueOnError = 1
    BEGIN
        PRINT 'Opération réussie.';
        RETURN 0;
    END
    ELSE
    BEGIN
        PRINT 'Opération échouée.';
        RETURN 1;
    END
END
GO
