USE [DBATOOLS]
GO
/****** Object:  StoredProcedure [dbo].[GenerateColumnstoreIndex]    Script Date: 05/02/2025 16:53:32 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [dbo].[GenerateColumnstoreIndex]
    @schemaName sysname,					-- Nom du schéma de la table
    @tableName sysname,						-- Nom de la table
	@orderClause NVARCHAR(1000) = '',		--(Optionnel) Liste de colonnes pour trier le columnstore index sur les colonnes de la liste (Example : [col1],[col2]
	@fileGroup NVARCHAR(100) = 'Primary',	--(Optionnel) Nom du groupe de fichier où sera créé l'index
	@force BIT = 0,							--(Optionnel) Si option activé, suppresion et reconstruction de l'index
	@execute BIT = 1,						--(Optionnel) Paramètre d'execution de l'ordre SQL par défaut 1
    @debug BIT = 1							--(Optionnel) Paramètre de débogage avec valeur par défaut de 1
	
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX) = '';
	DECLARE @sqlDropIX NVARCHAR(MAX) = '';
    DECLARE @errorMessage NVARCHAR(MAX) = '';
    DECLARE @mssqlVersion tinyint;  

    SELECT 
        @sql = 'CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_I0 ON ' 
                + QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME) + ' (' 
                + STRING_AGG(QUOTENAME(c.COLUMN_NAME), ', ') WITHIN GROUP (ORDER BY c.ORDINAL_POSITION)  
                + ')' 
    FROM 
		INFORMATION_SCHEMA.COLUMNS c
    INNER JOIN 
        INFORMATION_SCHEMA.TABLES t 
        ON c.TABLE_NAME = t.TABLE_NAME
    WHERE 
        t.TABLE_TYPE = 'BASE TABLE'
        AND c.TABLE_SCHEMA = @schemaName
        AND c.TABLE_NAME = @tableName
        AND c.DATA_TYPE NOT IN ('image','ntext','text')
        AND (c.CHARACTER_MAXIMUM_LENGTH != -1 OR c.CHARACTER_MAXIMUM_LENGTH IS NULL)
    GROUP BY 
        c.TABLE_SCHEMA, c.TABLE_NAME

	--Récupère la vrsion majeur de l'instance
	SELECT @mssqlVersion=CAST(SERVERPROPERTY('ProductMajorVersion') AS int)

	--Ajoute la clause ORDER si la version majeur du serveur est supèrieur à 16 donc SQL Server 2022
	IF @mssqlVersion > 16
		BEGIN
			-- Ajoute la clause ORDER si spécifiée
			IF @orderClause <> ''
			BEGIN
				SET @sql = @sql + ' ORDER (' + @orderClause +')';
			END
		END

    -- Ajoute le FILEGROUP, par défaut 'PRIMARY'
    SET @sql = @sql + ' ON ' + QUOTENAME(@fileGroup)+';';
    
	IF @force = 1
		BEGIN
			SET @sqlDropIX='DROP INDEX IF EXISTS NCCI_I0 ON '+QUOTENAME(@schemaName)+'.'+QUOTENAME(@tableName)+';'
			IF @debug = 1 OR (@debug=0 AND @execute=0)
				BEGIN
					RAISERROR('Dropping index if exists NCCI_I0 on %s.%s', 1, 1, @schemaName, @tableName) WITH NOWAIT;
					RAISERROR('SQL Statement : %s', 1, 1, @sqlDropIX) WITH NOWAIT;
				END
			IF @execute = 1
				BEGIN
					EXEC sp_executesql @sqlDropIX;
				END
		END
		
		BEGIN TRY
			IF @debug = 1
				BEGIN
					RAISERROR('Creating index on %s.%s', 1, 1, @schemaName, @tableName) WITH NOWAIT;
					RAISERROR('SQL Statement : %s', 1, 1, @sql) WITH NOWAIT;
				END
			IF @execute = 1
				EXEC sp_executesql @sql;
		END TRY
		BEGIN CATCH				
					SET @errorMessage = ERROR_MESSAGE();
					RAISERROR('Error during the index creation NCCI_I0 on %s.%s; SQL Statement : %s Error Message : %s', 16, 1, @schemaName, @tableName, @sql, @errorMessage) WITH NOWAIT;
					
		END CATCH                                 
END;
