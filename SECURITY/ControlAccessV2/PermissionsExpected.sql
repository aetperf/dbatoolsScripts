-- Table for expected permissions with system versioning
USE [DBATOOLS]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [security].[PermissionsExpected](
    [DatabaseName] [sysname] NOT NULL,
    [LoginName] [sysname] NOT NULL,
    [RoleName] [sysname] NOT NULL,
    [LastModifiedBy] [nvarchar](100) NOT NULL,
    [ValidFrom] [datetime2](7) GENERATED ALWAYS AS ROW START NOT NULL,
    [ValidTo] [datetime2](7) GENERATED ALWAYS AS ROW END NOT NULL,
    CONSTRAINT [PK_PermissionsExpected] PRIMARY KEY CLUSTERED
    (
        [DatabaseName] ASC,
        [LoginName] ASC,
        [RoleName] ASC
    ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY],
    PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo])
) ON [PRIMARY]
WITH
(
    SYSTEM_VERSIONING = ON (HISTORY_TABLE = [security].[PermissionsExpected_History])
)
GO