CREATE OR ALTER PROCEDURE sp_dropdbsnapshot 
    @dbsnapshotname sysname, 
    @execute bit = 0, 
    @force bit = 0, 
    @debug bit = 1, 
    @continueOnError bit = 1
WITH EXECUTE AS OWNER
AS
BEGIN
    DECLARE @err INT = 0;
	DECLARE @sql VARCHAR(MAX);
	DECLARE @session_id INT;

    -- Vérifier si le snapshot existe
    IF NOT EXISTS (
        SELECT 1 
        FROM sys.databases 
        WHERE QUOTENAME(name) = QUOTENAME(@dbsnapshotname) 
	AND source_database_id is not null -- Pour etre sur que la base passée en paramètre est bien un snapshot et pas une base réelle
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
    IF @force = 1 AND @err = 0
    BEGIN
        --BEGIN TRY
            -- Trouver toutes les sessions utilisant le snapshot
            DECLARE session_cursor CURSOR FOR
            SELECT session_id
            FROM sys.dm_exec_sessions
            WHERE database_id = DB_ID(@dbsnapshotname);

            OPEN session_cursor;
            FETCH NEXT FROM session_cursor INTO @session_id;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                -- Tuer la session
                SET @sql = 'KILL ' + CAST(@session_id AS NVARCHAR(10));
                EXEC sp_executesql @sql;
                IF @debug=1
					RAISERROR('Suppression de la session %s, SQL : %s', 1, 1, @session_id,@sql) WITH NOWAIT;
                FETCH NEXT FROM session_cursor INTO @session_id;
            END

            CLOSE session_cursor;
            DEALLOCATE session_cursor;
        /*END TRY
        BEGIN CATCH
            SET @err = 1;
            IF @continueOnError = 0 
            BEGIN
				IF @debug=1
					RAISERROR('Erreur lors de la tentative de tuer les sessions', 16, 1) WITH NOWAIT;
                RETURN;
            END
			ELSE 
			BEGIN
				IF @debug = 1
					 RAISERROR('Erreur lors de la tentative de suppression des sessions', 1, 1) WITH NOWAIT;
			END
        END CATCH*/
    END

    -- Si tout est ok, on supprime le snapshot
    IF @err = 0
    BEGIN
        BEGIN TRY
			IF @debug = 1
				RAISERROR('Suppression du database snapshot %s', 1, 1, @dbsnapshotname) WITH NOWAIT;
            EXEC('DROP DATABASE ' + QUOTENAME(@dbsnapshotname));
        END TRY
        BEGIN CATCH
            SET @err = 1;
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
	/*
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
    END*/
END
GO
