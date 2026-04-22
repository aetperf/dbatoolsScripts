CREATE OR ALTER PROCEDURE dbo.dba_realign_columnscollation_with_dbcollation
(
      @Execute         bit           = 0
    , @Debug           bit           = 0
    , @IncludeTables   nvarchar(max) = NULL   -- ex: 'dbo.Customer, sales.%, %.Audit%'
    , @ExcludeTables   nvarchar(max) = NULL   -- ex: 'dbo.Log%, %.tmp_%'
    , @IncludeColumns  nvarchar(max) = NULL   -- ex: 'Name, Code, %.Description, dbo.Customer.Name'
    , @ExcludeColumns  nvarchar(max) = NULL   -- ex: '%_backup, Legacy%'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;

    DECLARE @DatabaseCollation sysname = CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS sysname);

    BEGIN TRY

        IF @Debug = 1
        BEGIN
            PRINT 'Database         : ' + CONVERT(varchar(256), DB_NAME());
            PRINT 'DB collation     : ' + CONVERT(varchar(256), @DatabaseCollation);
            PRINT '@Execute         : ' + CONVERT(varchar(10), @Execute);
            PRINT '@Debug           : ' + CONVERT(varchar(10), @Debug);
            PRINT '@IncludeTables   : ' + ISNULL(CONVERT(varchar(max), @IncludeTables), '<NULL>');
            PRINT '@ExcludeTables   : ' + ISNULL(CONVERT(varchar(max), @ExcludeTables), '<NULL>');
            PRINT '@IncludeColumns  : ' + ISNULL(CONVERT(varchar(max), @IncludeColumns), '<NULL>');
            PRINT '@ExcludeColumns  : ' + ISNULL(CONVERT(varchar(max), @ExcludeColumns), '<NULL>');
        END;

        ---------------------------------------------------------------------
        -- Temp tables with DATABASE_DEFAULT collation on textual columns
        ---------------------------------------------------------------------
        CREATE TABLE #IncludeTables
        (
            pattern nvarchar(4000) COLLATE DATABASE_DEFAULT NOT NULL
        );

        CREATE TABLE #ExcludeTables
        (
            pattern nvarchar(4000) COLLATE DATABASE_DEFAULT NOT NULL
        );

        CREATE TABLE #IncludeColumns
        (
            pattern nvarchar(4000) COLLATE DATABASE_DEFAULT NOT NULL
        );

        CREATE TABLE #ExcludeColumns
        (
            pattern nvarchar(4000) COLLATE DATABASE_DEFAULT NOT NULL
        );

        CREATE TABLE #Candidates
        (
              candidate_id           int IDENTITY(1,1) PRIMARY KEY
            , object_id              int NOT NULL
            , column_id              int NOT NULL
            , schema_name            sysname        COLLATE DATABASE_DEFAULT NOT NULL
            , table_name             sysname        COLLATE DATABASE_DEFAULT NOT NULL
            , column_name            sysname        COLLATE DATABASE_DEFAULT NOT NULL
            , full_table_name        nvarchar(517)  COLLATE DATABASE_DEFAULT NOT NULL
            , full_column_name       nvarchar(900)  COLLATE DATABASE_DEFAULT NOT NULL
            , type_name              sysname        COLLATE DATABASE_DEFAULT NOT NULL
            , max_length             smallint NOT NULL
            , precision_value        tinyint NOT NULL
            , scale_value            tinyint NOT NULL
            , is_nullable            bit NOT NULL
            , is_computed            bit NOT NULL
            , is_identity            bit NOT NULL
            , is_rowguidcol          bit NOT NULL
            , current_collation      sysname        COLLATE DATABASE_DEFAULT NULL
            , target_collation       sysname        COLLATE DATABASE_DEFAULT NOT NULL
            , column_definition      nvarchar(max)  COLLATE DATABASE_DEFAULT NULL
            , alter_sql              nvarchar(max)  COLLATE DATABASE_DEFAULT NULL
            , has_dependencies       bit NOT NULL DEFAULT(0)
            , dependency_summary     nvarchar(max)  COLLATE DATABASE_DEFAULT NULL
            , warning_message        nvarchar(max)  COLLATE DATABASE_DEFAULT NULL
            , status                 varchar(30)    COLLATE DATABASE_DEFAULT NOT NULL DEFAULT('PENDING')
            , error_message          nvarchar(max)  COLLATE DATABASE_DEFAULT NULL
        );

        CREATE TABLE #DependencyWarnings
        (
              warning_id             int IDENTITY(1,1) PRIMARY KEY
            , object_id              int NOT NULL
            , column_id              int NOT NULL
            , schema_name            sysname        COLLATE DATABASE_DEFAULT NOT NULL
            , table_name             sysname        COLLATE DATABASE_DEFAULT NOT NULL
            , column_name            sysname        COLLATE DATABASE_DEFAULT NOT NULL
            , dependency_type        varchar(50)    COLLATE DATABASE_DEFAULT NOT NULL
            , dependency_name        sysname        COLLATE DATABASE_DEFAULT NULL
            , details                nvarchar(max)  COLLATE DATABASE_DEFAULT NULL
            , drop_script            nvarchar(max)  COLLATE DATABASE_DEFAULT NULL
            , create_script          nvarchar(max)  COLLATE DATABASE_DEFAULT NULL
        );

        CREATE TABLE #Results
        (
              result_id              int IDENTITY(1,1) PRIMARY KEY
            , schema_name            sysname        COLLATE DATABASE_DEFAULT NOT NULL
            , table_name             sysname        COLLATE DATABASE_DEFAULT NOT NULL
            , column_name            sysname        COLLATE DATABASE_DEFAULT NOT NULL
            , status                 varchar(30)    COLLATE DATABASE_DEFAULT NOT NULL
            , reason                 nvarchar(max)  COLLATE DATABASE_DEFAULT NULL
            , executed_sql           nvarchar(max)  COLLATE DATABASE_DEFAULT NULL
        );

        ---------------------------------------------------------------------
        -- Parse filters
        ---------------------------------------------------------------------
        INSERT INTO #IncludeTables(pattern)
        SELECT LTRIM(RTRIM([value])) COLLATE DATABASE_DEFAULT
        FROM string_split(COALESCE(@IncludeTables, N''), N',')
        WHERE LTRIM(RTRIM([value])) <> N'';

        INSERT INTO #ExcludeTables(pattern)
        SELECT LTRIM(RTRIM([value])) COLLATE DATABASE_DEFAULT
        FROM string_split(COALESCE(@ExcludeTables, N''), N',')
        WHERE LTRIM(RTRIM([value])) <> N'';

        INSERT INTO #IncludeColumns(pattern)
        SELECT LTRIM(RTRIM([value])) COLLATE DATABASE_DEFAULT
        FROM string_split(COALESCE(@IncludeColumns, N''), N',')
        WHERE LTRIM(RTRIM([value])) <> N'';

        INSERT INTO #ExcludeColumns(pattern)
        SELECT LTRIM(RTRIM([value])) COLLATE DATABASE_DEFAULT
        FROM string_split(COALESCE(@ExcludeColumns, N''), N',')
        WHERE LTRIM(RTRIM([value])) <> N'';

        ---------------------------------------------------------------------
        -- Identify candidate columns
        ---------------------------------------------------------------------
        ;WITH base AS
        (
            SELECT
                  c.object_id
                , c.column_id
                , CONVERT(sysname, s.name) COLLATE DATABASE_DEFAULT AS schema_name
                , CONVERT(sysname, t.name) COLLATE DATABASE_DEFAULT AS table_name
                , CONVERT(sysname, c.name) COLLATE DATABASE_DEFAULT AS column_name
                , CONVERT(nvarchar(517), QUOTENAME(s.name) + N'.' + QUOTENAME(t.name)) COLLATE DATABASE_DEFAULT AS full_table_name
                , CONVERT(nvarchar(900), QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N'.' + QUOTENAME(c.name)) COLLATE DATABASE_DEFAULT AS full_column_name
                , CONVERT(sysname, ty.name) COLLATE DATABASE_DEFAULT AS type_name
                , c.max_length
                , c.precision AS precision_value
                , c.scale AS scale_value
                , c.is_nullable
                , c.is_computed
                , c.is_identity
                , c.is_rowguidcol
                , CONVERT(sysname, c.collation_name) COLLATE DATABASE_DEFAULT AS current_collation
            FROM sys.columns c
            JOIN sys.tables  t ON t.object_id = c.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.types   ty ON ty.user_type_id = c.user_type_id
            WHERE
                    t.is_ms_shipped = 0
                AND c.collation_name IS NOT NULL
                AND c.collation_name <> @DatabaseCollation
                AND ty.name IN (N'char', N'varchar', N'nchar', N'nvarchar', N'text', N'ntext')
        )
        INSERT INTO #Candidates
        (
              object_id, column_id, schema_name, table_name, column_name
            , full_table_name, full_column_name
            , type_name, max_length, precision_value, scale_value
            , is_nullable, is_computed, is_identity, is_rowguidcol
            , current_collation, target_collation
        )
        SELECT
              b.object_id, b.column_id, b.schema_name, b.table_name, b.column_name
            , b.full_table_name, b.full_column_name
            , b.type_name, b.max_length, b.precision_value, b.scale_value
            , b.is_nullable, b.is_computed, b.is_identity, b.is_rowguidcol
            , b.current_collation, CONVERT(sysname, @DatabaseCollation) COLLATE DATABASE_DEFAULT
        FROM base b
        WHERE
            (
                NOT EXISTS (SELECT 1 FROM #IncludeTables)
                OR EXISTS
                (
                    SELECT 1
                    FROM #IncludeTables it
                    WHERE b.table_name COLLATE DATABASE_DEFAULT LIKE it.pattern COLLATE DATABASE_DEFAULT
                       OR (b.schema_name + N'.' + b.table_name) COLLATE DATABASE_DEFAULT LIKE it.pattern COLLATE DATABASE_DEFAULT
                )
            )
            AND NOT EXISTS
            (
                SELECT 1
                FROM #ExcludeTables et
                WHERE b.table_name COLLATE DATABASE_DEFAULT LIKE et.pattern COLLATE DATABASE_DEFAULT
                   OR (b.schema_name + N'.' + b.table_name) COLLATE DATABASE_DEFAULT LIKE et.pattern COLLATE DATABASE_DEFAULT
            )
            AND
            (
                NOT EXISTS (SELECT 1 FROM #IncludeColumns)
                OR EXISTS
                (
                    SELECT 1
                    FROM #IncludeColumns ic
                    WHERE b.column_name COLLATE DATABASE_DEFAULT LIKE ic.pattern COLLATE DATABASE_DEFAULT
                       OR (b.table_name + N'.' + b.column_name) COLLATE DATABASE_DEFAULT LIKE ic.pattern COLLATE DATABASE_DEFAULT
                       OR (b.schema_name + N'.' + b.table_name + N'.' + b.column_name) COLLATE DATABASE_DEFAULT LIKE ic.pattern COLLATE DATABASE_DEFAULT
                )
            )
            AND NOT EXISTS
            (
                SELECT 1
                FROM #ExcludeColumns ec
                WHERE b.column_name COLLATE DATABASE_DEFAULT LIKE ec.pattern COLLATE DATABASE_DEFAULT
                   OR (b.table_name + N'.' + b.column_name) COLLATE DATABASE_DEFAULT LIKE ec.pattern COLLATE DATABASE_DEFAULT
                   OR (b.schema_name + N'.' + b.table_name + N'.' + b.column_name) COLLATE DATABASE_DEFAULT LIKE ec.pattern COLLATE DATABASE_DEFAULT
            );

        ---------------------------------------------------------------------
        -- Build ALTER statement
        ---------------------------------------------------------------------
        UPDATE c
        SET
            column_definition =
                CASE
                    WHEN c.type_name IN (N'varchar', N'char')
                        THEN CONVERT(nvarchar(max),
                             c.type_name + N'(' + CASE WHEN c.max_length = -1 THEN N'MAX' ELSE CONVERT(varchar(10), c.max_length) END + N')'
                        ) COLLATE DATABASE_DEFAULT
                    WHEN c.type_name IN (N'nvarchar', N'nchar')
                        THEN CONVERT(nvarchar(max),
                             c.type_name + N'(' + CASE WHEN c.max_length = -1 THEN N'MAX' ELSE CONVERT(varchar(10), c.max_length / 2) END + N')'
                        ) COLLATE DATABASE_DEFAULT
                    WHEN c.type_name IN (N'text', N'ntext')
                        THEN CONVERT(nvarchar(max), c.type_name) COLLATE DATABASE_DEFAULT
                    ELSE CONVERT(nvarchar(max), c.type_name) COLLATE DATABASE_DEFAULT
                END,
            alter_sql =
                (
                    N'ALTER TABLE ' + QUOTENAME(c.schema_name) + N'.' + QUOTENAME(c.table_name)
                    + N' ALTER COLUMN ' + QUOTENAME(c.column_name) + N' '
                    + CASE
                        WHEN c.type_name IN (N'varchar', N'char')
                            THEN CONVERT(nvarchar(max),
                                 c.type_name + N'(' + CASE WHEN c.max_length = -1 THEN N'MAX' ELSE CONVERT(varchar(10), c.max_length) END + N')'
                            ) COLLATE DATABASE_DEFAULT
                        WHEN c.type_name IN (N'nvarchar', N'nchar')
                            THEN CONVERT(nvarchar(max),
                                 c.type_name + N'(' + CASE WHEN c.max_length = -1 THEN N'MAX' ELSE CONVERT(varchar(10), c.max_length / 2) END + N')'
                            ) COLLATE DATABASE_DEFAULT
                        WHEN c.type_name IN (N'text', N'ntext')
                            THEN CONVERT(nvarchar(max), c.type_name) COLLATE DATABASE_DEFAULT
                        ELSE CONVERT(nvarchar(max), c.type_name) COLLATE DATABASE_DEFAULT
                      END
                    + N' COLLATE ' + CONVERT(nvarchar(256), c.target_collation) COLLATE DATABASE_DEFAULT
                    + CASE WHEN c.is_nullable = 1 THEN N' NULL' ELSE N' NOT NULL' END
                    + N';'
                ) COLLATE DATABASE_DEFAULT
        FROM #Candidates c;

        ---------------------------------------------------------------------
        -- Unsupported cases
        ---------------------------------------------------------------------
        UPDATE c
        SET
              status = 'SKIPPED_UNSUPPORTED'
            , error_message =
                CASE
                    WHEN c.is_computed = 1 THEN N'Computed column: ALTER COLUMN not supported.'
                    WHEN c.is_identity = 1 THEN N'Identity column: Automatic treatment not supported.'
                    WHEN c.type_name IN (N'text', N'ntext') THEN N'Type text/ntext obsolet: migrate to varchar/nvarchar(max) before.'
                    ELSE N'Unsupported Case.'
                END COLLATE DATABASE_DEFAULT
        FROM #Candidates c
        WHERE c.is_computed = 1
           OR c.is_identity = 1
           OR c.type_name IN (N'text', N'ntext');

        ---------------------------------------------------------------------
        -- Dependencies : indexes
        ---------------------------------------------------------------------
        INSERT INTO #DependencyWarnings
        (
              object_id, column_id, schema_name, table_name, column_name
            , dependency_type, dependency_name, details, drop_script, create_script
        )
        SELECT DISTINCT
              c.object_id
            , c.column_id
            , c.schema_name
            , c.table_name
            , c.column_name
            , CONVERT(varchar(50),
                CASE
                    WHEN i.is_primary_key = 1 THEN 'PRIMARY KEY INDEX'
                    WHEN i.is_unique_constraint = 1 THEN 'UNIQUE CONSTRAINT INDEX'
                    ELSE 'INDEX'
                END
              ) COLLATE DATABASE_DEFAULT
            , CONVERT(sysname, i.name) COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max), N'Column used in an index.') COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max),
                CASE
                    WHEN i.is_primary_key = 1 OR i.is_unique_constraint = 1 THEN
                        N'-- DROP NOT GENERATED HERE: use the associated constraint'
                    ELSE
                        N'DROP INDEX ' + QUOTENAME(i.name) + N' ON ' + QUOTENAME(c.schema_name) + N'.' + QUOTENAME(c.table_name) + N';'
                END
              ) COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max),
                CASE
                    WHEN i.is_primary_key = 1 OR i.is_unique_constraint = 1 THEN
                        N'-- CREATE NOT GENERATED HERE: use the associated constraint'
                    ELSE
                        N'CREATE '
                        + CASE WHEN i.is_unique = 1 THEN N'UNIQUE ' ELSE N'' END
                        + CONVERT(nvarchar(200), i.type_desc) COLLATE DATABASE_DEFAULT
                        + N' INDEX ' + QUOTENAME(i.name)
                        + N' ON ' + QUOTENAME(c.schema_name) + N'.' + QUOTENAME(c.table_name)
                        + N' ('
                        + STUFF
                          (
                              (
                                  SELECT
                                      N', ' + QUOTENAME(c2.name)
                                      + CASE WHEN ic2.is_descending_key = 1 THEN N' DESC' ELSE N' ASC' END
                                  FROM sys.index_columns ic2
                                  JOIN sys.columns c2
                                    ON c2.object_id = ic2.object_id
                                   AND c2.column_id = ic2.column_id
                                  WHERE ic2.object_id = i.object_id
                                    AND ic2.index_id   = i.index_id
                                    AND ic2.key_ordinal > 0
                                  ORDER BY ic2.key_ordinal
                                  FOR XML PATH(''), TYPE
                              ).value('.', 'nvarchar(max)')
                              , 1, 2, N''
                          )
                        + N')'
                        + CASE
                            WHEN EXISTS
                            (
                                SELECT 1
                                FROM sys.index_columns ic3
                                WHERE ic3.object_id = i.object_id
                                  AND ic3.index_id   = i.index_id
                                  AND ic3.is_included_column = 1
                            )
                            THEN
                                N' INCLUDE ('
                                + STUFF
                                  (
                                      (
                                          SELECT
                                              N', ' + QUOTENAME(c3.name)
                                          FROM sys.index_columns ic3
                                          JOIN sys.columns c3
                                            ON c3.object_id = ic3.object_id
                                           AND c3.column_id = ic3.column_id
                                          WHERE ic3.object_id = i.object_id
                                            AND ic3.index_id   = i.index_id
                                            AND ic3.is_included_column = 1
                                          ORDER BY c3.column_id
                                          FOR XML PATH(''), TYPE
                                      ).value('.', 'nvarchar(max)')
                                      , 1, 2, N''
                                  )
                                + N')'
                            ELSE N''
                          END
                        + CASE WHEN i.filter_definition IS NOT NULL THEN N' WHERE ' + CONVERT(nvarchar(max), i.filter_definition) COLLATE DATABASE_DEFAULT ELSE N'' END
                        + N';'
                END
              ) COLLATE DATABASE_DEFAULT
        FROM #Candidates c
        JOIN sys.index_columns ic
          ON ic.object_id = c.object_id
         AND ic.column_id = c.column_id
        JOIN sys.indexes i
          ON i.object_id = ic.object_id
         AND i.index_id  = ic.index_id
        WHERE i.is_hypothetical = 0
          AND c.status = 'PENDING';

        ---------------------------------------------------------------------
        -- Dependencies : PK / UQ
        ---------------------------------------------------------------------
        INSERT INTO #DependencyWarnings
        (
              object_id, column_id, schema_name, table_name, column_name
            , dependency_type, dependency_name, details, drop_script, create_script
        )
        SELECT DISTINCT
              c.object_id
            , c.column_id
            , c.schema_name
            , c.table_name
            , c.column_name
            , CONVERT(varchar(50),
                CASE WHEN kc.[type] = 'PK' THEN 'PRIMARY KEY' ELSE 'UNIQUE CONSTRAINT' END
              ) COLLATE DATABASE_DEFAULT
            , CONVERT(sysname, kc.name) COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max), N'Column used in a key constraint.') COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max),
                N'ALTER TABLE ' + QUOTENAME(c.schema_name) + N'.' + QUOTENAME(c.table_name)
                + N' DROP CONSTRAINT ' + QUOTENAME(kc.name) + N';'
              ) COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max),
                N'ALTER TABLE ' + QUOTENAME(c.schema_name) + N'.' + QUOTENAME(c.table_name)
                + N' ADD CONSTRAINT ' + QUOTENAME(kc.name) + N' '
                + CASE WHEN kc.[type] = 'PK' THEN N'PRIMARY KEY ' ELSE N'UNIQUE ' END
                + CONVERT(nvarchar(200), i.type_desc) COLLATE DATABASE_DEFAULT
                + N' ('
                + STUFF
                  (
                      (
                          SELECT
                              N', ' + QUOTENAME(c2.name)
                              + CASE WHEN ic2.is_descending_key = 1 THEN N' DESC' ELSE N' ASC' END
                          FROM sys.index_columns ic2
                          JOIN sys.columns c2
                            ON c2.object_id = ic2.object_id
                           AND c2.column_id = ic2.column_id
                          WHERE ic2.object_id = i.object_id
                            AND ic2.index_id   = i.index_id
                            AND ic2.key_ordinal > 0
                          ORDER BY ic2.key_ordinal
                          FOR XML PATH(''), TYPE
                      ).value('.', 'nvarchar(max)')
                      , 1, 2, N''
                  )
                + N');'
              ) COLLATE DATABASE_DEFAULT
        FROM #Candidates c
        JOIN sys.index_columns icx
          ON icx.object_id = c.object_id
         AND icx.column_id = c.column_id
        JOIN sys.indexes i
          ON i.object_id = icx.object_id
         AND i.index_id  = icx.index_id
        JOIN sys.key_constraints kc
          ON kc.parent_object_id = i.object_id
         AND kc.unique_index_id  = i.index_id
        WHERE c.status = 'PENDING';

        ---------------------------------------------------------------------
        -- Dependencies : FK
        ---------------------------------------------------------------------
        INSERT INTO #DependencyWarnings
        (
              object_id, column_id, schema_name, table_name, column_name
            , dependency_type, dependency_name, details, drop_script, create_script
        )
        SELECT DISTINCT
              c.object_id
            , c.column_id
            , c.schema_name
            , c.table_name
            , c.column_name
            , CONVERT(varchar(50), 'FOREIGN KEY') COLLATE DATABASE_DEFAULT
            , CONVERT(sysname, fk.name) COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max), N'Column used in a foreign key.') COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max),
                N'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(fk.parent_object_id)) + N'.' + QUOTENAME(OBJECT_NAME(fk.parent_object_id))
                + N' DROP CONSTRAINT ' + QUOTENAME(fk.name) + N';'
              ) COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max),
                N'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(fk.parent_object_id)) + N'.' + QUOTENAME(OBJECT_NAME(fk.parent_object_id))
                + N' ADD CONSTRAINT ' + QUOTENAME(fk.name)
                + N' FOREIGN KEY ('
                + STUFF
                  (
                      (
                          SELECT N', ' + QUOTENAME(pc.name)
                          FROM sys.foreign_key_columns fkc2
                          JOIN sys.columns pc
                            ON pc.object_id = fkc2.parent_object_id
                           AND pc.column_id = fkc2.parent_column_id
                          WHERE fkc2.constraint_object_id = fk.object_id
                          ORDER BY fkc2.constraint_column_id
                          FOR XML PATH(''), TYPE
                      ).value('.', 'nvarchar(max)')
                      , 1, 2, N''
                  )
                + N') REFERENCES '
                + QUOTENAME(OBJECT_SCHEMA_NAME(fk.referenced_object_id)) + N'.' + QUOTENAME(OBJECT_NAME(fk.referenced_object_id))
                + N' ('
                + STUFF
                  (
                      (
                          SELECT N', ' + QUOTENAME(rc.name)
                          FROM sys.foreign_key_columns fkc3
                          JOIN sys.columns rc
                            ON rc.object_id = fkc3.referenced_object_id
                           AND rc.column_id = fkc3.referenced_column_id
                          WHERE fkc3.constraint_object_id = fk.object_id
                          ORDER BY fkc3.constraint_column_id
                          FOR XML PATH(''), TYPE
                      ).value('.', 'nvarchar(max)')
                      , 1, 2, N''
                  )
                + N')'
                + CASE fk.delete_referential_action
                    WHEN 1 THEN N' ON DELETE CASCADE'
                    WHEN 2 THEN N' ON DELETE SET NULL'
                    WHEN 3 THEN N' ON DELETE SET DEFAULT'
                    ELSE N''
                  END
                + CASE fk.update_referential_action
                    WHEN 1 THEN N' ON UPDATE CASCADE'
                    WHEN 2 THEN N' ON UPDATE SET NULL'
                    WHEN 3 THEN N' ON UPDATE SET DEFAULT'
                    ELSE N''
                  END
                + N';'
              ) COLLATE DATABASE_DEFAULT
        FROM #Candidates c
        JOIN sys.foreign_key_columns fkc
          ON (fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id)
          OR (fkc.referenced_object_id = c.object_id AND fkc.referenced_column_id = c.column_id)
        JOIN sys.foreign_keys fk
          ON fk.object_id = fkc.constraint_object_id
        WHERE c.status = 'PENDING';

        ---------------------------------------------------------------------
        -- Dependencies : DEFAULT
        ---------------------------------------------------------------------
        INSERT INTO #DependencyWarnings
        (
              object_id, column_id, schema_name, table_name, column_name
            , dependency_type, dependency_name, details, drop_script, create_script
        )
        SELECT DISTINCT
              c.object_id
            , c.column_id
            , c.schema_name
            , c.table_name
            , c.column_name
            , CONVERT(varchar(50), 'DEFAULT CONSTRAINT') COLLATE DATABASE_DEFAULT
            , CONVERT(sysname, dc.name) COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max), N'Column have a default constraint.') COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max),
                N'ALTER TABLE ' + QUOTENAME(c.schema_name) + N'.' + QUOTENAME(c.table_name)
                + N' DROP CONSTRAINT ' + QUOTENAME(dc.name) + N';'
              ) COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max),
                N'ALTER TABLE ' + QUOTENAME(c.schema_name) + N'.' + QUOTENAME(c.table_name)
                + N' ADD CONSTRAINT ' + QUOTENAME(dc.name)
                + N' DEFAULT ' + CONVERT(nvarchar(max), dc.definition) COLLATE DATABASE_DEFAULT
                + N' FOR ' + QUOTENAME(c.column_name) + N';'
              ) COLLATE DATABASE_DEFAULT
        FROM #Candidates c
        JOIN sys.default_constraints dc
          ON dc.parent_object_id = c.object_id
         AND dc.parent_column_id = c.column_id
        WHERE c.status = 'PENDING';

        ---------------------------------------------------------------------
        -- Dependencies : CHECK
        -- Note: fallback simple by parent_object_id + referenced_minor_id
        ---------------------------------------------------------------------
        INSERT INTO #DependencyWarnings
        (
              object_id, column_id, schema_name, table_name, column_name
            , dependency_type, dependency_name, details, drop_script, create_script
        )
        SELECT DISTINCT
              c.object_id
            , c.column_id
            , c.schema_name
            , c.table_name
            , c.column_name
            , CONVERT(varchar(50), 'CHECK CONSTRAINT') COLLATE DATABASE_DEFAULT
            , CONVERT(sysname, cc.name) COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max), N'Column referenced by a CHECK constraint.') COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max),
                N'ALTER TABLE ' + QUOTENAME(c.schema_name) + N'.' + QUOTENAME(c.table_name)
                + N' DROP CONSTRAINT ' + QUOTENAME(cc.name) + N';'
              ) COLLATE DATABASE_DEFAULT
            , CONVERT(nvarchar(max),
                N'ALTER TABLE ' + QUOTENAME(c.schema_name) + N'.' + QUOTENAME(c.table_name)
                + N' WITH CHECK ADD CONSTRAINT ' + QUOTENAME(cc.name)
                + N' CHECK ' + CONVERT(nvarchar(max), cc.definition) COLLATE DATABASE_DEFAULT + N';'
              ) COLLATE DATABASE_DEFAULT
        FROM #Candidates c
        JOIN sys.check_constraints cc
          ON cc.parent_object_id = c.object_id
        JOIN sys.sql_expression_dependencies sed
          ON sed.referencing_id = cc.object_id
         AND sed.referenced_id = c.object_id
         AND sed.referenced_minor_id = c.column_id
        WHERE c.status = 'PENDING';

        ---------------------------------------------------------------------
        -- Mark candidates with dependencies
        ---------------------------------------------------------------------
        UPDATE c
        SET
              has_dependencies = 1
            , dependency_summary =
                STUFF
                (
                    (
                        SELECT DISTINCT
                            (
                                N'; '
                                + CONVERT(nvarchar(200), w.dependency_type) COLLATE DATABASE_DEFAULT
                                + N' ['
                                + ISNULL(CONVERT(nvarchar(256), w.dependency_name) COLLATE DATABASE_DEFAULT, N'?')
                                + N']'
                            )
                        FROM #DependencyWarnings w
                        WHERE w.object_id = c.object_id
                          AND w.column_id = c.column_id
                        FOR XML PATH(''), TYPE
                    ).value('.', 'nvarchar(max)')
                    , 1, 2, N''
                ) COLLATE DATABASE_DEFAULT
            , warning_message = N'Column with structural dependencies. DROP/CREATE before realign.' COLLATE DATABASE_DEFAULT
        FROM #Candidates c
        WHERE EXISTS
        (
            SELECT 1
            FROM #DependencyWarnings w
            WHERE w.object_id = c.object_id
              AND w.column_id = c.column_id
        )
          AND c.status = 'PENDING';

        UPDATE c
        SET status = 'SKIPPED_DEPENDENCY'
        FROM #Candidates c
        WHERE c.has_dependencies = 1
          AND c.status = 'PENDING';

        ---------------------------------------------------------------------
        -- Cursor execution / preview
        ---------------------------------------------------------------------
        DECLARE
              @candidate_id   int
            , @schema_name    sysname
            , @table_name     sysname
            , @column_name    sysname
            , @alter_sql      nvarchar(max);

        DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
            SELECT
                  candidate_id
                , schema_name
                , table_name
                , column_name
                , alter_sql
            FROM #Candidates
            WHERE status = 'PENDING'
            ORDER BY schema_name, table_name, column_id;

        OPEN cur;

        FETCH NEXT FROM cur INTO @candidate_id, @schema_name, @table_name, @column_name, @alter_sql;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                IF @Debug = 1
                BEGIN
                    PRINT 'Processing ' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name) + '.' + QUOTENAME(@column_name);
                    PRINT CONVERT(varchar(max), @alter_sql);
                END;

                IF @Execute = 1
                BEGIN
                    EXEC sys.sp_executesql @alter_sql;

                    UPDATE #Candidates
                    SET status = 'SUCCESS'
                    WHERE candidate_id = @candidate_id;

                    INSERT INTO #Results(schema_name, table_name, column_name, status, reason, executed_sql)
                    VALUES
                    (
                          @schema_name COLLATE DATABASE_DEFAULT
                        , @table_name COLLATE DATABASE_DEFAULT
                        , @column_name COLLATE DATABASE_DEFAULT
                        , 'SUCCESS'
                        , NULL
                        , @alter_sql COLLATE DATABASE_DEFAULT
                    );
                END
                ELSE
                BEGIN
                    UPDATE #Candidates
                    SET status = 'PREVIEW'
                    WHERE candidate_id = @candidate_id;

                    INSERT INTO #Results(schema_name, table_name, column_name, status, reason, executed_sql)
                    VALUES
                    (
                          @schema_name COLLATE DATABASE_DEFAULT
                        , @table_name COLLATE DATABASE_DEFAULT
                        , @column_name COLLATE DATABASE_DEFAULT
                        , 'PREVIEW'
                        , N'@Execute = 0, No modification applied.' COLLATE DATABASE_DEFAULT
                        , @alter_sql COLLATE DATABASE_DEFAULT
                    );
                END;
            END TRY
            BEGIN CATCH
                UPDATE #Candidates
                SET
                      status = 'FAILED'
                    , error_message = ERROR_MESSAGE() COLLATE DATABASE_DEFAULT
                WHERE candidate_id = @candidate_id;

                INSERT INTO #Results(schema_name, table_name, column_name, status, reason, executed_sql)
                VALUES
                (
                      @schema_name COLLATE DATABASE_DEFAULT
                    , @table_name COLLATE DATABASE_DEFAULT
                    , @column_name COLLATE DATABASE_DEFAULT
                    , 'FAILED'
                    , ERROR_MESSAGE() COLLATE DATABASE_DEFAULT
                    , @alter_sql COLLATE DATABASE_DEFAULT
                );

                IF @Debug = 1
                BEGIN
                    PRINT 'ERROR on ' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name) + '.' + QUOTENAME(@column_name)
                        + ' -> ' + CONVERT(varchar(max), ERROR_MESSAGE());
                END;
            END CATCH;

            FETCH NEXT FROM cur INTO @candidate_id, @schema_name, @table_name, @column_name, @alter_sql;
        END;

        CLOSE cur;
        DEALLOCATE cur;

        ---------------------------------------------------------------------
        -- Persist skipped rows
        ---------------------------------------------------------------------
        INSERT INTO #Results(schema_name, table_name, column_name, status, reason, executed_sql)
        SELECT
              c.schema_name
            , c.table_name
            , c.column_name
            , c.status
            , COALESCE(c.error_message, c.warning_message, c.dependency_summary)
            , c.alter_sql
        FROM #Candidates c
        WHERE c.status IN ('SKIPPED_DEPENDENCY', 'SKIPPED_UNSUPPORTED');

        ---------------------------------------------------------------------
        -- Result set 1 : summary
        ---------------------------------------------------------------------
        SELECT
              DB_NAME() AS database_name
            , @DatabaseCollation AS target_collation
            , COUNT(*) AS initial_columns_to_process
            , SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count
            , SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failure_count
            , SUM(CASE WHEN status = 'SKIPPED_DEPENDENCY' THEN 1 ELSE 0 END) AS skipped_dependency_count
            , SUM(CASE WHEN status = 'SKIPPED_UNSUPPORTED' THEN 1 ELSE 0 END) AS skipped_unsupported_count
            , SUM(CASE WHEN status = 'PREVIEW' THEN 1 ELSE 0 END) AS preview_count
        FROM #Candidates;

        ---------------------------------------------------------------------
        -- Result set 2 : details
        ---------------------------------------------------------------------
        SELECT
              schema_name
            , table_name
            , column_name
            , status
            , reason
            , executed_sql
        FROM #Results
        ORDER BY
              CASE status
                  WHEN 'FAILED' THEN 1
                  WHEN 'SKIPPED_DEPENDENCY' THEN 2
                  WHEN 'SKIPPED_UNSUPPORTED' THEN 3
                  WHEN 'SUCCESS' THEN 4
                  WHEN 'PREVIEW' THEN 5
                  ELSE 99
              END,
              schema_name, table_name, column_name;

        ---------------------------------------------------------------------
        -- Result set 3 : warnings / proposed scripts
        ---------------------------------------------------------------------
        SELECT
              schema_name
            , table_name
            , column_name
            , dependency_type
            , dependency_name
            , details
            , drop_script
            , create_script
        FROM #DependencyWarnings
        ORDER BY schema_name, table_name, column_name, dependency_type, dependency_name;

        ---------------------------------------------------------------------
        -- Result set 4 : candidate list
        ---------------------------------------------------------------------
        SELECT
              schema_name
            , table_name
            , column_name
            , current_collation
            , target_collation
            , type_name
            , column_definition
            , has_dependencies
            , dependency_summary
            , status
            , error_message
            , alter_sql
        FROM #Candidates
        ORDER BY schema_name, table_name, column_name;

    END TRY
    BEGIN CATCH
        DECLARE
              @ErrNum      int            = ERROR_NUMBER()
            , @ErrSev      int            = ERROR_SEVERITY()
            , @ErrState    int            = ERROR_STATE()
            , @ErrLine     int            = ERROR_LINE()
            , @ErrProc     nvarchar(256)  = ERROR_PROCEDURE()
            , @ErrMsg      nvarchar(4000) = ERROR_MESSAGE();

        SELECT
              DB_NAME()          AS database_name
            , @DatabaseCollation AS target_collation
            , @ErrNum            AS error_number
            , @ErrSev            AS error_severity
            , @ErrState          AS error_state
            , @ErrLine           AS error_line
            , @ErrProc           AS error_procedure
            , @ErrMsg            AS error_message;

        THROW;
    END CATCH
END;
GO