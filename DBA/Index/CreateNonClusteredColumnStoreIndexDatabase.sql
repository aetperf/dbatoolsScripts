

ALTER PROCEDURE dbo.GenerateDatabaseColumnstoreIndex
	@schemaNameLike NVARCHAR(100)='%',		-- Instruction SQL dans le like pour le nom du sch�ma par d�faut '%'
    @tableNameLike NVARCHAR(100)='%',		-- Instruction SQL dans le like pour le nom de la table par d�faut '%'
	@indexName NVARCHAR(100)='NCCI_I0',				--Nom de l'index
	@MinRow BIGINT=-1,						--Nombre minimum de ligne pour cr�er le non clustered columnstore index par d�faut -1
	@MaxRow BIGINT=1000000000000,				--Nombre maximum de ligne pour cr�er le non clustered columnstore index par d�faut -1
	@orderClause NVARCHAR(1000) = '',		--(Optionnel) Liste de colonnes pour trier le columnstore index sur les colonnes de la liste (Example : [col1],[col2]
	@fileGroup NVARCHAR(100) = 'Primary',	--(Optionnel) Nom du groupe de fichier o� sera cr�� l'index
	@force BIT = 0,							--(Optionnel) Si option activ�, suppresion et reconstruction de l'index
	@execute BIT = 1,						--(Optionnel) Param�tre d'execution de l'ordre SQL par d�faut 1
    @debug BIT = 1							--(Optionnel) Param�tre de debug par d�faut 1
AS
BEGIN
    DECLARE 
	@SchemaNameParam NVARCHAR(128), 
	@TableNameParam NVARCHAR(200), 
	@RowCount BIGINT;
    
    DECLARE TableCursor CURSOR FOR
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
        t.is_ms_shipped = 0  -- Exclure les tables syst�me
        AND s.name LIKE @schemaNameLike  -- Filtrer par sch�ma (si n�cessaire, remplace '%' par le sch�ma)
        AND t.name LIKE @tableNameLike  -- Filtrer par nom de table (si besoin, remplace '%' par le pattern)
    GROUP BY 
        s.name, t.name
    HAVING
        SUM(p.rows) >= @MinRow AND SUM(p.rows) <= @MaxRow
    ORDER BY 
        Schema_Name, Table_Name;

   
    OPEN TableCursor;

    FETCH NEXT FROM TableCursor INTO @SchemaNameParam, @TableNameParam,@RowCount;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT 'Schema: ' + @SchemaNameParam + ', Table: ' + @TableNameParam + ', Row Count: ' + CAST(@RowCount AS NVARCHAR(10));

		EXEC dbo.GenerateColumnstoreIndex @schemaName=@SchemaNameParam, @tableName=@TableNameParam, @indexName=@indexName,
		@orderClause=@orderClause, @fileGroup=@fileGroup, @force=@force, @execute=@execute, @debug=@debug

        FETCH NEXT FROM TableCursor INTO @SchemaNameParam, @TableNameParam,@RowCount;
    END

    CLOSE TableCursor;
    DEALLOCATE TableCursor;
END;
