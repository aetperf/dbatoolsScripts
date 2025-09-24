USE [DBATOOLS]
GO

/****** Object:  Table [dbo].[T_COMPARE_RESULTS]    Script Date: 2025-09-03 22:55:22 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[T_COMPARE_RESULTS]') AND type in (N'U'))
DROP TABLE [dbo].[T_COMPARE_RESULTS]
GO

/****** Object:  Table [dbo].[T_COMPARE_RESULTS]    Script Date: 2025-09-03 22:55:22 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
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
	[diffdistinct] [bigint] NULL,
	[samplekeysetwhere] [varchar](max) NULL,
	[iscutted] bit NOT NULL
 CONSTRAINT [PK_T_COMPARE_RESULTS] PRIMARY KEY CLUSTERED 
(
	[testname] ASC,
	[testrunid] ASC,
	[columnstested] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


