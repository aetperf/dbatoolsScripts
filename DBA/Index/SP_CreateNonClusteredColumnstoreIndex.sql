CREATE OR ALTER PROCEDURE [dbo].[CreateColumnstoreIndex]
    @SchemaName sysname,					-- Nom du schéma de la table
    @TableName sysname,						-- Nom de la table
	@IndexName NVARCHAR(150),				--Nom de l'index	
	@OrderClause NVARCHAR(1000) = '',		--(Optionnel) Liste de colonnes pour trier le columnstore index sur les colonnes de la liste (Example : [col1],[col2]
	@FileGroup NVARCHAR(100) = 'Primary',	--(Optionnel) Nom du groupe de fichier où sera créé l'index
	@Force BIT = 0,							--(Optionnel) Si option activé, suppresion et reconstruction de l'index
	@Execute BIT = 1,						--(Optionnel) Paramètre d'execution de l'ordre SQL par défaut 1
    @Debug BIT = 1,							--(Optionnel) Paramètre de débogage avec valeur par défaut de 1
	@ErrorCode INT OUTPUT
AS
BEGIN
    DECLARE @Sql NVARCHAR(MAX) = '';
    DECLARE @SqlDropIX NVARCHAR(MAX) = '';
    DECLARE @ErrorMessage NVARCHAR(MAX) = '';
    DECLARE @MssqlVersion tinyint;

    BEGIN TRY
        SELECT 
            @Sql = 'CREATE NONCLUSTERED COLUMNSTORE INDEX '+QUOTENAME(@IndexName)+' ON ' 
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
            AND c.TABLE_SCHEMA = @SchemaName
            AND c.TABLE_NAME = @TableName
            AND c.DATA_TYPE NOT IN ('image', 'ntext', 'text')
            AND (c.CHARACTER_MAXIMUM_LENGTH != -1 OR c.CHARACTER_MAXIMUM_LENGTH IS NULL)
            AND sc.is_computed = 0 -- Exclure les colonnes calculées
        GROUP BY 
            c.TABLE_SCHEMA, c.TABLE_NAME

        SELECT @MssqlVersion = CAST(SERVERPROPERTY('ProductMajorVersion') AS int);

        -- Ajout de la clause ORDER si la version est supérieure à 16 (SQL Server 2022)
        IF @MssqlVersion > 16
        BEGIN
            IF @OrderClause <> ''
                SET @Sql = @Sql + ' ORDER (' + @OrderClause +')';
        END

        -- Ajout du FILEGROUP
        SET @Sql = @Sql + ' ON ' + QUOTENAME(@FileGroup)+';';

        -- Suppression de l'index si @force = 1
        IF @Force = 1
        BEGIN
            SET @SqlDropIX = 'DROP INDEX IF EXISTS '+QUOTENAME(@IndexName)+' ON '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TableName)+';';
            IF @Debug = 1 OR (@Debug = 0 AND @Execute = 0)
            BEGIN
                RAISERROR('Dropping index if exists %s on %s.%s', 1, 1, @IndexName, @SchemaName, @TableName) WITH NOWAIT;
                RAISERROR('SQL Statement : %s', 1, 1, @SqlDropIX) WITH NOWAIT;
            END
            IF @Execute = 1
                EXEC sp_executesql @SqlDropIX;
        END

        -- Création de l'index
        IF @Debug = 1
        BEGIN
            RAISERROR('Creating index on %s.%s', 1, 1, @SchemaName, @TableName) WITH NOWAIT;
            RAISERROR('SQL Statement : %s', 1, 1, @Sql) WITH NOWAIT;
        END
        IF @Execute = 1
            EXEC sp_executesql @Sql;

        SET @ErrorCode=0; 
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        RAISERROR('Error during the index creation: %s. Error Message: %s', 16, 1, @IndexName, @ErrorMessage) WITH NOWAIT;

        SET @ErrorCode=1; 
    END CATCH
END;
