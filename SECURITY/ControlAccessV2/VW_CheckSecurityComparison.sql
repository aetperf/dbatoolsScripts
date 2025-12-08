USE [DBATOOLS]

GO

 

/****** Object:  View [security].[VW_CheckSecurityComparison]    Script Date: 08/12/2025 09:42:21 ******/

SET ANSI_NULLS ON

GO

 

SET QUOTED_IDENTIFIER ON

GO

 

 

 

 

 

CREATE VIEW [security].[VW_CheckSecurityComparison] AS

SELECT

    e.LastModifiedBy AS Grantor,

    COALESCE(e.DatabaseName, c.DatabaseName) AS DatabaseName,

    COALESCE(e.LoginName,   c.LoginName)     AS LoginName,

    COALESCE(e.RoleName,    c.RoleName)      AS RoleName,

    c.AuditDate as CurrentSecurityAuditDate,

              e.ValidFrom as ExpectedSecurityAuditDate,

              c.HasDbAccess,

    CASE

                            WHEN c.HasDbAccess = 0 THEN 3

        WHEN e.DatabaseName IS NOT NULL AND c.DatabaseName IS NOT NULL THEN 0

        WHEN e.DatabaseName IS NULL THEN 1

        WHEN c.DatabaseName IS NULL THEN 2

        ELSE -1

    END AS CheckStatus,

    CASE

                            WHEN c.HasDbAccess = 0 THEN 'ANALYSE HASDBACCESS'

        WHEN e.DatabaseName IS NULL THEN 'REVOKE'

        WHEN c.DatabaseName IS NULL THEN 'GRANT'

        ELSE 'OK'

    END AS CheckStatusDescription,

 

    /* ===========================

       SQLStatement : action à faire

       =========================== */

    CASE

        -- Rôle attendu (e) mais pas présent en base (c) -> on GRANT

        WHEN c.DatabaseName IS NULL THEN

            'USE ' + QUOTENAME(e.DatabaseName) + '; ' +

            'IF SUSER_ID(N''' + e.LoginName + ''') IS NOT NULL ' +

            'BEGIN ' +

                'IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N''' + e.LoginName + ''') ' +

                    'CREATE USER ' + QUOTENAME(e.LoginName) + ' FOR LOGIN ' + QUOTENAME(e.LoginName) + '; ' +

                'IF NOT EXISTS ( ' +

                    'SELECT 1 ' +

                    'FROM sys.database_permissions p ' +

                    'JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id ' +

                    'WHERE dp.name = N''' + e.LoginName + ''' ' +

                      'AND p.permission_name = ''CONNECT'' ' +

                      'AND p.state_desc IN (''GRANT'',''GRANT_WITH_GRANT_OPTION'') ' +

                ') ' +

                    'GRANT CONNECT TO ' + QUOTENAME(e.LoginName) + '; ' +

                'IF NOT EXISTS ( ' +

                    'SELECT 1 ' +

                    'FROM sys.database_role_members drm ' +

                    'JOIN sys.database_principals r ON drm.role_principal_id  = r.principal_id ' +

                    'JOIN sys.database_principals m ON drm.member_principal_id = m.principal_id ' +

                    'WHERE r.name = N''' + e.RoleName + ''' ' +

                      'AND m.name = N''' + e.LoginName + ''' ' +

                ') ' +

                    'ALTER ROLE ' + QUOTENAME(e.RoleName) + ' ADD MEMBER ' + QUOTENAME(e.LoginName) + '; ' +

            'END'

 

        -- Rôle présent en base (c) mais non attendu (e) -> on REVOKE

        WHEN e.DatabaseName IS NULL THEN

            'USE ' + QUOTENAME(c.DatabaseName) + '; ' +

            'IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N''' + c.LoginName + ''') ' +

            'BEGIN ' +

                'DECLARE @Uid INT = USER_ID(N''' + c.LoginName + '''); ' +

                'IF EXISTS ( ' +

                    'SELECT 1 ' +

                    'FROM sys.database_role_members drm ' +

                    'JOIN sys.database_principals r ON drm.role_principal_id  = r.principal_id ' +

                    'JOIN sys.database_principals m ON drm.member_principal_id = m.principal_id ' +

                    'WHERE r.name = N''' + c.RoleName + ''' ' +

                      'AND m.name = N''' + c.LoginName + ''' ' +

                ') ' +

                    'ALTER ROLE ' + QUOTENAME(c.RoleName) + ' DROP MEMBER ' + QUOTENAME(c.LoginName) + '; ' +

 

                'IF NOT EXISTS ( ' +

                    'SELECT 1 ' +

                    'FROM sys.database_role_members drm ' +

                    'JOIN sys.database_principals r ON drm.role_principal_id = r.principal_id ' +

                    'WHERE drm.member_principal_id = @Uid ' +

                      'AND r.name <> ''public'' ' +

                ') ' +

                'AND NOT EXISTS (SELECT 1 FROM sys.schemas s WHERE s.principal_id = @Uid) ' +

                'AND NOT EXISTS (SELECT 1 FROM sys.objects o WHERE o.principal_id = @Uid) ' +

                'BEGIN ' +

                    'DROP USER ' + QUOTENAME(c.LoginName) + '; ' +

                'END ' +

            'END'

 

        ELSE NULL

    END AS SQLStatement,

 

    /* ===========================

       UndoSQLStatement : rollback

       =========================== */

    CASE

        -- Undo du GRANT (c.DatabaseName IS NULL) : retirer du rôle

        WHEN c.DatabaseName IS NULL THEN

            'USE ' + QUOTENAME(e.DatabaseName) + '; ' +

            'IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N''' + e.LoginName + ''') ' +

            'BEGIN ' +

                'IF EXISTS ( ' +

                    'SELECT 1 ' +

                    'FROM sys.database_role_members drm ' +

                    'JOIN sys.database_principals r ON drm.role_principal_id  = r.principal_id ' +

                    'JOIN sys.database_principals m ON drm.member_principal_id = m.principal_id ' +

                    'WHERE r.name = N''' + e.RoleName + ''' ' +

                      'AND m.name = N''' + e.LoginName + ''' ' +

                ') ' +

                    'ALTER ROLE ' + QUOTENAME(e.RoleName) + ' DROP MEMBER ' + QUOTENAME(e.LoginName) + '; ' +

            'END'

 

        -- Undo du REVOKE (e.DatabaseName IS NULL) : recréer l'appartenance au rôle

        WHEN e.DatabaseName IS NULL THEN

            'USE ' + QUOTENAME(c.DatabaseName) + '; ' +

            'IF SUSER_ID(N''' + c.LoginName + ''') IS NOT NULL ' +

            'BEGIN ' +

                'IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N''' + c.LoginName + ''') ' +

                    'CREATE USER ' + QUOTENAME(c.LoginName) + ' FOR LOGIN ' + QUOTENAME(c.LoginName) + '; ' +

                'IF NOT EXISTS ( ' +

                    'SELECT 1 ' +

                    'FROM sys.database_permissions p ' +

                    'JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id ' +

                    'WHERE dp.name = N''' + c.LoginName + ''' ' +

                      'AND p.permission_name = ''CONNECT'' ' +

                      'AND p.state_desc IN (''GRANT'',''GRANT_WITH_GRANT_OPTION'') ' +

                ') ' +

                    'GRANT CONNECT TO ' + QUOTENAME(c.LoginName) + '; ' +

                'IF NOT EXISTS ( ' +

                    'SELECT 1 ' +

                    'FROM sys.database_role_members drm ' +

                    'JOIN sys.database_principals r ON drm.role_principal_id  = r.principal_id ' +

                    'JOIN sys.database_principals m ON drm.member_principal_id = m.principal_id ' +

                    'WHERE r.name = N''' + c.RoleName + ''' ' +

                      'AND m.name = N''' + c.LoginName + ''' ' +

                ') ' +

                    'ALTER ROLE ' + QUOTENAME(c.RoleName) + ' ADD MEMBER ' + QUOTENAME(c.LoginName) + '; ' +

            'END'

 

        ELSE NULL

    END AS UndoSQLStatement

 

FROM [security].[PermissionsExpected] e

FULL OUTER JOIN [security].[VW_GetCurrentLoginsPermissions] c

    ON e.DatabaseName = c.DatabaseName

   AND e.LoginName   = c.LoginName

   AND e.RoleName    = c.RoleName

GO