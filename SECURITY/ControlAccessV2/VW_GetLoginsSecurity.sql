-- View to get the latest security logins state
USE [DBATOOLS]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [security].[VW_GetLoginsSecurity] AS
SELECT *
FROM [DBATOOLS].[security].[LoginsSecurityHistory]
WHERE [AuditDate] = (
    SELECT MAX(AuditDate)
    FROM [DBATOOLS].[security].[LoginsSecurityHistory]
)
GO