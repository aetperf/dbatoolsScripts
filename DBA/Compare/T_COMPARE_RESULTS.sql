IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[T_COMPARE_RESULTS]') AND type in (N'U'))
DROP TABLE [dbo].[T_COMPARE_RESULTS]
GO

CREATE TABLE [dbo].[T_COMPARE_RESULTS](
	[testname] [nvarchar](255) NOT NULL,
	[testrunid] [varchar](128) NOT NULL,
	[testdate] [datetime] NOT NULL,
	[sourcedatabase] [sysname] NOT NULL,
	[sourceschema] [sysname] NOT NULL,
	[sourcetable] [sysname] NOT NULL,
	[targetdatabase] [sysname] NOT NULL,
	[targetschema] [sysname] NOT NULL,
	[targettable] [sysname] NOT NULL,
	[keycolumns] [nvarchar](2000) NOT NULL,
	[columnstested] [sysname] NOT NULL,
	[diffcount] [bigint] NOT NULL,
 CONSTRAINT [PK_T_COMPARE_RESULTS] PRIMARY KEY CLUSTERED 
(
	[testname] ASC,
	[testrunid] ASC,
	[columnstested] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


