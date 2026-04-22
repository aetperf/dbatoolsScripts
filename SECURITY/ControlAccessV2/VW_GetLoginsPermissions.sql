USE [DBATOOLS]

GO

 

/****** Object:  View [security].[VW_GetCurrentLoginsPermissions]    Script Date: 08/12/2025 09:42:49 ******/

SET ANSI_NULLS ON

GO

 

SET QUOTED_IDENTIFIER ON

GO

 

 

 

 

 

CREATE     VIEW [security].[VW_GetCurrentLoginsPermissions]

AS

SELECT  [Id], [DatabaseName], [LoginName], [RoleName], [AuditDate], [HasDbAccess], [Sid]

FROM [security].[LoginsPermissionsHistory]

WHERE [AuditDate] = (

                            SELECT MAX(AuditDate)

                            FROM [security].[LoginsPermissionsHistory]

)

GO