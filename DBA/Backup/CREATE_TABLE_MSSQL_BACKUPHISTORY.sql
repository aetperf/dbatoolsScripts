USE [DBATOOLS]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MSSQLBackupHistory]') AND type in (N'U'))
DROP TABLE [dbo].[MSSQLBackupHistory]
GO



CREATE TABLE [dbo].[MSSQLBackupHistory](
	[ComputerName] [NVARCHAR](2000) NULL,
	[InstanceName] [NVARCHAR](2000) NULL,
	[SqlInstance] [NVARCHAR](2000) NULL,
	[AvailabilityGroupName] [NVARCHAR](2000) NULL,
	[Database] [NVARCHAR](2000) NULL,
	[UserName] [NVARCHAR](2000) NULL,
	[Start] [DATETIME2](7) NULL,
	[End] [DATETIME2](7) NULL,
	[Duration] [BIGINT] NULL,
	[Path] [NVARCHAR](2000) NULL,
	[TotalSize] [BIGINT] NULL,
	[CompressedBackupSize] [BIGINT] NULL,
	[CompressionRatio] [FLOAT] NULL,
	[Type] [NVARCHAR](2000) NULL,
	[BackupSetId] [NVARCHAR](2000) NULL,
	[DeviceType] [NVARCHAR](2000) NULL,
	[Software] [NVARCHAR](2000) NULL,
	[FullName] [NVARCHAR](2000) NULL,
	[FileList] [NVARCHAR](2000) NULL,
	[Position] [INT] NULL,
	[FirstLsn] [NVARCHAR](2000) NULL,
	[DatabaseBackupLsn] [NVARCHAR](2000) NULL,
	[CheckpointLsn] [NVARCHAR](2000) NULL,
	[LastLsn] [NVARCHAR](2000) NULL,
	[SoftwareVersionMajor] [INT] NULL,
	[IsCopyOnly] [BIT] NULL,
	[LastRecoveryForkGUID] [UNIQUEIDENTIFIER] NULL,
	[RecoveryModel] [NVARCHAR](2000) NULL,
	[KeyAlgorithm] [NVARCHAR](2000) NULL,
	[EncryptorThumbprint] [NVARCHAR](2000) NULL,
	[EncryptorType] [NVARCHAR](2000) NULL
) ON [PRIMARY]
GO

CREATE UNIQUE CLUSTERED INDEX [PK_MSSQLBackupHistory] ON [dbo].[MSSQLBackupHistory]
(
	[ComputerName] ASC,
	[InstanceName] ASC,
	[Database] ASC,
	[Start] ASC,
	[Path] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO