-- View to get the latest logins permissions state
USE [DBATOOLS]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [security].[VW_GetLoginsPermissions] AS
SELECT *
FROM [DBATOOLS].[security].[LoginsPermissionsHistory]
WHERE [AuditDate] = (
    SELECT MAX(AuditDate)
    FROM [DBATOOLS].[security].[LoginsPermissionsHistory]
)
GO