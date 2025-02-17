CREATE TABLE ParamIndexTable(
	SchemaName sysname NOT NULL,
    TableName sysname NOT NULL,
    Updatetime DATETIME,
	FileGroup nvarchar(100),
    CONSTRAINT PK_ParamIndexTable PRIMARY KEY (SchemaName, TableName))