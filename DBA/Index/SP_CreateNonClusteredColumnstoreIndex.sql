CREATE OR ALTER PROCEDURE [dbo].[GenerateColumnstoreIndex]
    @schemaName sysname,					-- Nom du schéma de la table
    @tableName sysname,						-- Nom de la table
	@indexName NVARCHAR(150),				--Nom de l'index	
	@orderClause NVARCHAR(1000) = '',		--(Optionnel) Liste de colonnes pour trier le columnstore index sur les colonnes de la liste (Example : [col1],[col2]
	@fileGroup NVARCHAR(100) = 'Primary',	--(Optionnel) Nom du groupe de fichier où sera créé l'index
	@force BIT = 0,							--(Optionnel) Si option activé, suppresion et reconstruction de l'index
	@execute BIT = 1,						--(Optionnel) Paramètre d'execution de l'ordre SQL par défaut 1
    @debug BIT = 1,							--(Optionnel) Paramètre de débogage avec valeur par défaut de 1
	@errorCode INT OUTPUT
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX) = '';
    DECLARE @sqlDropIX NVARCHAR(MAX) = '';
    DECLARE @errorMessage NVARCHAR(MAX) = '';
    DECLARE @mssqlVersion tinyint;

    BEGIN TRY
        SELECT 
            @sql = 'CREATE NONCLUSTERED COLUMNSTORE INDEX '+QUOTENAME(@indexName)+' ON ' 
            + QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME) + ' (' 
            + STRING_AGG(QUOTENAME(c.COLUMN_NAME), ', ') WITHIN GROUP (ORDER BY c.ORDINAL_POSITION)  
            + ')'
        FROM 
            INFORMATION_SCHEMA.COLUMNS c
        INNER JOIN 
            INFORMATION_SCHEMA.TABLES t 
            ON c.TABLE_NAME = t.TABLE_NAME
        INNER JOIN 
            sys.columns sc
            ON sc.object_id = OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME)
            AND sc.name = c.COLUMN_NAME
        WHERE 
            t.TABLE_TYPE = 'BASE TABLE'
            AND c.TABLE_SCHEMA = @schemaName
            AND c.TABLE_NAME = @tableName
            AND c.DATA_TYPE NOT IN ('image', 'ntext', 'text')
            AND (c.CHARACTER_MAXIMUM_LENGTH != -1 OR c.CHARACTER_MAXIMUM_LENGTH IS NULL)
            AND sc.is_computed = 0 -- Exclure les colonnes calculées
        GROUP BY 
            c.TABLE_SCHEMA, c.TABLE_NAME

        SELECT @mssqlVersion = CAST(SERVERPROPERTY('ProductMajorVersion') AS int);

        -- Ajout de la clause ORDER si la version est supérieure à 16 (SQL Server 2022)
        IF @mssqlVersion > 16
        BEGIN
            IF @orderClause <> ''
                SET @sql = @sql + ' ORDER (' + @orderClause +')';
        END

        -- Ajout du FILEGROUP
        SET @sql = @sql + ' ON ' + QUOTENAME(@fileGroup)+';';

        -- Suppression de l'index si @force = 1
        IF @force = 1
        BEGIN
            SET @sqlDropIX = 'DROP INDEX IF EXISTS '+QUOTENAME(@indexName)+' ON '+QUOTENAME(@schemaName)+'.'+QUOTENAME(@tableName)+';';
            IF @debug = 1 OR (@debug = 0 AND @execute = 0)
            BEGIN
                RAISERROR('Dropping index if exists %s on %s.%s', 1, 1, @indexName, @schemaName, @tableName) WITH NOWAIT;
                RAISERROR('SQL Statement : %s', 1, 1, @sqlDropIX) WITH NOWAIT;
            END
            IF @execute = 1
                EXEC sp_executesql @sqlDropIX;
        END

        -- Création de l'index
        IF @debug = 1
        BEGIN
            RAISERROR('Creating index on %s.%s', 1, 1, @schemaName, @tableName) WITH NOWAIT;
            RAISERROR('SQL Statement : %s', 1, 1, @sql) WITH NOWAIT;
        END
        IF @execute = 1
            EXEC sp_executesql @sql;

        SET @errorCode=0; 
    END TRY
    BEGIN CATCH
        SET @errorMessage = ERROR_MESSAGE();
        RAISERROR('Error during the index creation: %s. Error Message: %s', 16, 1, @indexName, @errorMessage) WITH NOWAIT;

        SET @errorCode=1; 
    END CATCH
END;
