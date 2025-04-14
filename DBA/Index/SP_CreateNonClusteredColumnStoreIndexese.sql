CREATE OR ALTER PROCEDURE dbo.CreateColumnstoreIndexes
	@SchemaNameLike NVARCHAR(100)='%',		-- Instruction SQL dans le like pour le nom du schéma par défaut '%'
    @TableNameLike NVARCHAR(100)='%',		-- Instruction SQL dans le like pour le nom de la table par défaut '%'
	@IndexName NVARCHAR(100)='<TableName>_NCCI_I0',				--Nom de l'index
	@MinRow BIGINT=-1,						--Nombre minimum de ligne pour créer le non clustered columnstore index par défaut -1
	@MaxRow BIGINT=1000000000000,			--Nombre maximum de ligne pour créer le non clustered columnstore index par défaut -1
	@OrderClause NVARCHAR(1000) = '',		--(Optionnel) Liste de colonnes pour trier le columnstore index sur les colonnes de la liste (Example : [col1],[col2]
	@FileGroup NVARCHAR(100) = 'Primary',	--(Optionnel) Nom du groupe de fichier où sera créé l'index
	@Force BIT = 0,							--(Optionnel) Si option activé, suppresion et reconstruction de l'index
	@Execute BIT = 1,						--(Optionnel) Paramétre d'execution de l'ordre SQL par défaut 1
    @Debug BIT = 1,							--(Optionnel) Paramétre de debug par défaut 1
	@Continue BIT = 1						--(Optionnel) Paramètre pour continuer l'execution si l'on rencontre une erreur avec valeur par défaut de 1

AS
BEGIN
    DECLARE 
	@SchemaNameParam NVARCHAR(128), 
	@TableNameParam NVARCHAR(200), 
    @IndexNameWithTableName NVARCHAR(150),
	@RowCount BIGINT,
	@errorCode INT;

    CREATE TABLE #TableCursor (
        Schema_Name sysname,
        Table_Name sysname,
        Row_Count BIGINT
    );
    
    INSERT INTO #TableCursor (Schema_Name, Table_Name, Row_Count)
    SELECT 
        s.name AS Schema_Name,
        t.name AS Table_Name, 
        SUM(p.rows) AS Row_Count
    FROM 
        sys.tables t
    INNER JOIN 
        sys.schemas s ON t.schema_id = s.schema_id
    INNER JOIN 
        sys.partitions p ON t.object_id = p.object_id
    WHERE 
        t.is_ms_shipped = 0  -- Exclure les tables système
        AND s.name LIKE @SchemaNameLike  -- Filtrer par schéma (si nécessaire, remplace '%' par le schéma)
        AND t.name LIKE @TableNameLike  -- Filtrer par nom de table (si besoin, remplace '%' par le pattern)
        AND p.index_id=1
        AND p.data_compression_desc!='COLUMNSTORE'
    GROUP BY 
        s.name, t.name
    HAVING
        SUM(p.rows) >= @MinRow AND SUM(p.rows) <= @MaxRow
    ORDER BY 
        Schema_Name, Table_Name;

	DECLARE TableCursor CURSOR FOR
	SELECT 
        Schema_Name, Table_Name, Row_Count
    FROM 
        #TableCursor

   
    OPEN TableCursor;

    FETCH NEXT FROM TableCursor INTO @SchemaNameParam, @TableNameParam,@RowCount;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @IndexNameWithTableName = REPLACE(@IndexName, '<TableName>', @TableNameParam);

        PRINT 'Schema: ' + @SchemaNameParam + ', Table: ' + @TableNameParam + ', Row Count: ' + CAST(@RowCount AS NVARCHAR(10));

		EXEC dbo.CreateColumnstoreIndex 
		@SchemaName=@SchemaNameParam, 
		@TableName=@TableNameParam, 
		@IndexName=@IndexNameWithTableName,
		@OrderClause=@OrderClause, 
		@FileGroup=@FileGroup, 
		@Force=@Force, 
		@Execute=@Execute, 
		@Debug=@Debug,
		@ErrorCode=@ErrorCode OUTPUT

		IF @Continue=0 and @ErrorCode=1
			BEGIN
				CLOSE TableCursor;
				DEALLOCATE TableCursor;
				DROP TABLE #TableCursor;
				RETURN;
			END
			

        FETCH NEXT FROM TableCursor INTO @SchemaNameParam, @TableNameParam,@RowCount;
    END

    CLOSE TableCursor;
    DEALLOCATE TableCursor;
	DROP TABLE #TableCursor;
END;
