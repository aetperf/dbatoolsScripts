USE [DBATOOLS]

GO

 

/****** Object:  Table [security].[PermissionsExpected_Staging]    Script Date: 08/12/2025 09:41:36 ******/

SET ANSI_NULLS ON

GO

 

SET QUOTED_IDENTIFIER ON

GO

 

CREATE TABLE [security].[PermissionsExpected_Staging](

              [DatabaseName] [sysname] NOT NULL,

              [LoginName] [sysname] NOT NULL,

              [RoleName] [sysname] NOT NULL,

              [MetaUser] [nvarchar](256) NOT NULL,

              [LoadDate] [datetime2](7) NULL,

CONSTRAINT [PK_PermissionsExpected_Staging] PRIMARY KEY CLUSTERED

(

              [DatabaseName] ASC,

              [LoginName] ASC,

              [RoleName] ASC

)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]

) ON [PRIMARY]

GO

 

ALTER TABLE [security].[PermissionsExpected_Staging] ADD  CONSTRAINT [DF__Permissio__LoadD__078C1F06]  DEFAULT (sysutcdatetime()) FOR [LoadDate]

GO

 