-- ===================================================================================
-- This stored procedure retrieves the database roles for all logins across user databases.
-- It populates the security.LoginDbRoles table with the current state of login roles.


CREATE OR ALTER   PROCEDURE [security].[sp_GetLoginDbRoles]
    @IncludeSystemDatabases BIT = 0  -- 0 = exclut master, model, msdb, tempdb
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @now DATETIME2(0) = SYSUTCDATETIME();

    DECLARE @db SYSNAME, @sql NVARCHAR(MAX);

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT name
        FROM sys.databases
        WHERE state_desc = 'ONLINE'
          AND (
                @IncludeSystemDatabases = 1 OR
                name NOT IN ('master','model','msdb','tempdb')
              );

    OPEN cur;
    FETCH NEXT FROM cur INTO @db;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = N'
WITH rolemap AS
(
    -- membres de rôles (sauf public)
    SELECT
        dp_member.name  AS user_name,
        dp_member.type_desc AS user_type,
        dp_role.name    AS role_name
    FROM ' + QUOTENAME(@db) + N'.sys.database_role_members drm
    JOIN ' + QUOTENAME(@db) + N'.sys.database_principals dp_role
         ON dp_role.principal_id = drm.role_principal_id
    JOIN ' + QUOTENAME(@db) + N'.sys.database_principals dp_member
         ON dp_member.principal_id = drm.member_principal_id
    WHERE dp_role.name <> ''public''
),
users_all AS
(
    -- tous les users "réels" (pas rôles, pas schémas)
    SELECT
        dp.name      AS user_name,
        dp.type_desc AS user_type,
        dp.sid       AS user_sid
    FROM ' + QUOTENAME(@db) + N'.sys.database_principals dp
    WHERE dp.type IN (''S'',''U'',''G'') -- SQL_USER, WINDOWS_USER, WINDOWS_GROUP (db-level)
      AND dp.principal_id > 0
)
INSERT INTO security.LoginDbRoles
(
    controldate, login_name, login_type, is_disabled, default_database,
    [database], [user_name], user_type, [role_name]
)
-- 1) Users avec rôle(s)
SELECT
    @now,
    sp.name                                       AS login_name,
    sp.type_desc                                  AS login_type,
    sp.is_disabled,
    sp.default_database_name                      AS default_database,
    '''+@db+'''                                     AS [database],
    rm.user_name,
    ua.user_type,
    rm.role_name
FROM rolemap rm
JOIN users_all ua
     ON ua.user_name = rm.user_name
LEFT JOIN sys.server_principals sp
     ON ua.user_sid = sp.sid
WHERE (sp.type IN (''S'',''G'') OR sp.sid IS NULL) -- garde SQL_LOGIN / WINDOWS_GROUP; users orphelins => login NULL

UNION ALL
-- 2) Users sans rôle explicite (role_name = NULL)
SELECT
    @now,
    sp.name,
    sp.type_desc,
    sp.is_disabled,
    sp.default_database_name,
    '''+@db+'''                                      AS [database],
    ua.user_name,
    ua.user_type,
    CAST(NULL AS SYSNAME)                         AS role_name
FROM users_all ua
LEFT JOIN rolemap rm
       ON rm.user_name = ua.user_name
LEFT JOIN sys.server_principals sp
       ON ua.user_sid = sp.sid
WHERE rm.user_name IS NULL
  AND (sp.type IN (''S'',''G'') OR sp.sid IS NULL)

UNION ALL
-- 3) Map explicite dbo -> db_owner
SELECT
    @now,
    sp_owner.name                                 AS login_name,
    sp_owner.type_desc                            AS login_type,
    sp_owner.is_disabled,
    sp_owner.default_database_name,
    '''+@db+'''                                        AS [database],
    N''dbo''                                      AS user_name,
    N''SQL_USER''                                 AS user_type,
    N''db_owner''                                 AS role_name
FROM ' + QUOTENAME(@db) + N'.sys.database_principals dp_dbo
CROSS JOIN sys.databases d
LEFT JOIN sys.server_principals sp_owner
       ON sp_owner.sid = d.owner_sid
WHERE dp_dbo.name = N''dbo''
  AND d.database_id = DB_ID()
  AND NOT EXISTS
  (
      SELECT 1
      FROM rolemap r0
      WHERE r0.user_name = N''dbo''
        AND r0.role_name = N''db_owner''
  );
';

PRINT @sql;

        EXEC sys.sp_executesql @sql, N'@now DATETIME2(0)', @now=@now;

        FETCH NEXT FROM cur INTO @db;
    END

    CLOSE cur; DEALLOCATE cur;
END
