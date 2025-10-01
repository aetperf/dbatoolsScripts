CREATE TABLE [security].[LoginDbRoles](
	[controldate] [datetime2](0) NOT NULL,
	[login_name] [sysname] NULL,
	[login_type] [nvarchar](60) NULL,
	[is_disabled] [bit] NULL,
	[default_database] [sysname] NULL,
	[database] [sysname] NOT NULL,
	[user_name] [sysname] NOT NULL,
	[user_type] [nvarchar](60) NOT NULL,
	[role_name] [sysname] NULL
) ON [PRIMARY]
GO

ALTER TABLE [security].[LoginDbRoles] ADD  CONSTRAINT [DF_LoginDbRoles_controldate]  DEFAULT (sysutcdatetime()) FOR [controldate]
GO

