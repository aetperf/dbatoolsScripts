USE [AdventureWorksDW2022]
GO

/****** Object:  Table [dba].[IndexLandscape]    Script Date: 11/02/2025 13:38:45 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE IF EXISTS [dba].[IndexLandscape];
CREATE TABLE [dba].[IndexLandscape](
	[SchemaName] [sysname] NOT NULL,
	[TableName] [sysname] NOT NULL,
	[IndexName] [nvarchar](200) NOT NULL,
	[IndexType] [sysname] NOT NULL,
	[IndexedColumns] [nvarchar](max) NOT NULL,
	[IncludeColumns] [nvarchar](max),
	[FilterCondition] [nvarchar](max),
 CONSTRAINT [PK_IndexLandscape] PRIMARY KEY CLUSTERED 
(
	[SchemaName] ASC,
	[TableName] ASC,
	[IndexName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

--Populate table [dba].[IndexLandscape] 
INSERT INTO [dba].[IndexLandscape]
        SELECT 
            s.name AS SchemaName,  
            t.name AS TableName,
            i.name AS IndexName,
            i.type_desc AS IndexType,
            STRING_AGG(
                CASE 
                    WHEN i.type_desc LIKE '%COLUMNSTORE%' THEN c.name
                    WHEN ic.is_included_column = 0 THEN c.name 
                END, ', ') 
                WITHIN GROUP (ORDER BY ic.index_column_id) 
                AS IndexedColumns,
            STRING_AGG(
                CASE 
                    WHEN i.type_desc LIKE '%COLUMNSTORE%' THEN NULL
                    WHEN ic.is_included_column = 1 THEN c.name 
                END, ', ') 
                WITHIN GROUP (ORDER BY ic.index_column_id) 
                AS IncludeColumns,
            i.filter_definition AS FilterCondition

        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id  
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.type > 0  -- Exclut les index HEAP (tables sans index clusterisé)
        AND s.name LIKE '%'
        GROUP BY s.name, t.name, i.name, i.type_desc, i.filter_definition

