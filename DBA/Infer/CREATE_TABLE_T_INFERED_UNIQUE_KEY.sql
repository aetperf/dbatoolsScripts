
DROP TABLE IF EXISTS dbo.InferedUniqueKey;
GO

CREATE TABLE dbo.InferedUniqueKey
(
    controldate      datetime      NOT NULL DEFAULT SYSUTCDATETIME(),
    dbname        sysname       NOT NULL,
    schemaname    sysname       NOT NULL,
    tablename     sysname       NOT NULL,
    eligiblecolumnslist nvarchar(max) NULL,
    excludedcolumnslist nvarchar(max) NULL,
    uk_found      nvarchar(max) NULL,
    best_unique_approximation nvarchar(max) NULL,
    testdurationseconds int          NOT NULL DEFAULT 0,
    CONSTRAINT PK_InferedUniqueKey PRIMARY KEY (controldate,dbname, schemaname, tablename)
);