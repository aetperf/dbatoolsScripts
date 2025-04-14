USE [DBATOOLS]
GO

CREATE ROLE [db_dbsnapshoter]
GO
GRANT EXECUTE ON [dbo].[sp_createdbsnapshot] TO [db_dbsnapshoter]  AS [dbo]
GO
GRANT EXECUTE ON [dbo].[sp_dropdbsnapshot] TO [db_dbsnapshoter]  AS [dbo]
GO
