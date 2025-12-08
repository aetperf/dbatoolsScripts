USE [DBATOOLS]

GO

 

/****** Object:  View [security].[VW_GetCurrentLoginsSecurity]    Script Date: 08/12/2025 09:43:08 ******/

SET ANSI_NULLS ON

GO

 

SET QUOTED_IDENTIFIER ON

GO

 

 

 

 

 

CREATE     VIEW [security].[VW_GetCurrentLoginsSecurity]

AS

SELECT *

FROM [security].[LoginsSecurityHistory]

WHERE [AuditDate] = (

                            SELECT MAX(AuditDate)

                            FROM [security].[LoginsSecurityHistory]

)

GO