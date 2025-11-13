-- View to compare expected and current security permissions
USE [DBATOOLS]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [security].[VW_CheckSecurityComparison] AS
SELECT
    COALESCE(e.DatabaseName, c.DatabaseName) AS DatabaseName,
    COALESCE(e.LoginName, c.LoginName) AS LoginName,
    COALESCE(e.RoleName, c.RoleName) AS RoleName,
    c.AuditDate,
    CASE
        WHEN e.DatabaseName IS NOT NULL AND c.DatabaseName IS NOT NULL THEN 0
        WHEN e.DatabaseName IS NULL THEN 1
        WHEN c.DatabaseName IS NULL THEN 2
        ELSE -1
    END AS CheckStatus,
    CASE
        WHEN e.DatabaseName IS NULL THEN 'Extra (present in audit but not expected)'
        WHEN c.DatabaseName IS NULL THEN 'Missing (expected but absent in database)'
        ELSE 'OK'
    END AS CheckStatusDescription,
    CASE
        WHEN c.DatabaseName IS NULL THEN
            'USE [' + e.DatabaseName + ']; ' +
            'EXEC sp_addrolemember @rolename = ''' + e.RoleName + ''', @membername = ''' + e.LoginName + ''';'
        WHEN e.DatabaseName IS NULL THEN
            'USE [' + c.DatabaseName + ']; ' +
            'EXEC sp_droprolemember @rolename = ''' + c.RoleName + ''', @membername = ''' + c.LoginName + ''';'
        ELSE NULL
    END AS SQLStatement,
    CASE
        WHEN c.DatabaseName IS NULL THEN
            'USE [' + e.DatabaseName + ']; ' +
            'EXEC sp_droprolemember @rolename = ''' + e.RoleName + ''', @membername = ''' + e.LoginName + ''';'
        WHEN e.DatabaseName IS NULL THEN
            'USE [' + c.DatabaseName + ']; ' +
            'EXEC sp_addrolemember @rolename = ''' + c.RoleName + ''', @membername = ''' + c.LoginName + ''';'
        ELSE NULL
    END AS UndoSQLStatement
FROM [security].[PermissionsExpected] e
FULL OUTER JOIN [security].[VW_GetLoginsPermissions] c
    ON e.DatabaseName = c.DatabaseName
   AND e.LoginName = c.LoginName
   AND e.RoleName = c.RoleName;
GO

