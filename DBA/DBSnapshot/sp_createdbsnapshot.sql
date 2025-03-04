CREATE OR ALTER PROCEDURE sp_createdbsnapshot
    @dbname sysname,
    @suffix varchar(50),
    @filename NVARCHAR(255) = 'C:\Program Files\Microsoft SQL Server\MSSQL16.DBA01\MSSQL\Data',  -- Paramètre pour spécifier le chemin et le nom du fichier
    @snapshotof sysname,  -- Paramètre pour spécifier la base de données source du snapshot
    @execute bit = 0,
    @debug bit = 1,
    @continueOnError bit = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @err INT = 0;
    DECLARE @snapshotname sysname;
    DECLARE @sql NVARCHAR(MAX);

    -- Vérification de l'existence de la base de données source (@snapshotof)
    IF EXISTS (
        SELECT 1 
        FROM sys.databases 
        WHERE name = @snapshotof
    )
    BEGIN
		IF @continueOnError=1
			BEGIN
				IF @debug=1
					RAISERROR('La base de données source pour le snapshot n''existe pas', 1, 1) WITH NOWAIT;
				SET @err = 0;
			END
		ELSE
			BEGIN
				IF @debug=1
					RAISERROR('La base de données source pour le snapshot n''existe pas', 16, 1) WITH NOWAIT;
				SET @err = 1;
				RETURN
			END
    END

	-- Générer le nom du snapshot
    SET @snapshotname = @dbname + '_' + @suffix;

    -- Vérification de l'existence de la base de données cible (@dbname)
    IF EXISTS (
        SELECT 1 
        FROM sys.databases 
        WHERE name = @snapshotname
    )
    BEGIN
		IF @continueOnError=1
			BEGIN
				IF @debug=1
					RAISERROR('La snapshot %s existe déjà', 1, 1,@snapshotname) WITH NOWAIT;
				SET @err = 0;
			END
		ELSE
			BEGIN
				IF @debug=1
					RAISERROR('La snapshot %s existe déjà', 16, 1,@snapshotname) WITH NOWAIT;
				SET @err = 1;
				RETURN
			END
    END

 
    -- Si tout est OK, créer le snapshot
    IF @err = 0
    BEGIN
        BEGIN TRY
            -- Utilisation du paramètre @filename pour spécifier le chemin du fichier
            SET @sql = 'CREATE DATABASE ' + @snapshotname + ' ON (NAME = ''' + @snapshotof + ''', FILENAME = ''' + @filename + '\' + @snapshotname + '.ss'') AS SNAPSHOT OF ' + @snapshotof;
            IF @debug=1
				RAISERROR('Creation de la snapshot %s, SQL : %s', 1, 1,@snapshotname,@sql) WITH NOWAIT;
            
            -- Si @execute est à 1, on exécute la commande SQL pour créer le snapshot
            IF @execute = 1
            BEGIN
                EXEC sp_executesql @sql;
				IF @debug=1
					RAISERROR('Snapshot %s créée', 1, 1,@snapshotname) WITH NOWAIT;	
            END
        END TRY
        BEGIN CATCH
            IF @continueOnError = 0 
            BEGIN
				IF @debug=1
					RAISERROR('Erreur lors de la création du snapshot %s', 16, 1, @snapshotname);
                RETURN;
            END
			ELSE
			BEGIN
				IF @debug=1
					RAISERROR('Erreur lors de la création du snapshot %s', 1, 1,@snapshotname) WITH NOWAIT;
				RETURN
			END
        END CATCH
    END

   
END
GO
