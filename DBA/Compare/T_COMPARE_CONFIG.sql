

/****** Object:  Table [dbo].[T_COMPARE_CONFIG]    Script Date: 2025-09-02 14:54:59 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[T_COMPARE_CONFIG]') AND type in (N'U'))
DROP TABLE [dbo].[T_COMPARE_CONFIG]
GO

*

CREATE TABLE [dbo].[T_COMPARE_CONFIG](
	[testname] [nvarchar](255) NOT NULL,
	[sourcedatabase] [sysname] NULL,
	[sourceschema] [sysname] NOT NULL,
	[sourcetable] [sysname] NOT NULL,
	[targetdatabase] [sysname] NULL,
	[targetschema] [sysname] NOT NULL,
	[targettable] [sysname] NOT NULL,
	[keycolumns] [nvarchar](1000) NOT NULL,
 CONSTRAINT [PK_T_COMPARE_CONFIG] PRIMARY KEY CLUSTERED 
(
	[testname] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


