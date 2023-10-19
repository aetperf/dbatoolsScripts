DECLARE @SearchString NVARCHAR(max);
DECLARE @SearchQuery NVARCHAR(MAX);

SET @SearchString = 'sendmail'

DROP TABLE ##sql_modules_search
CREATE TABLE ##sql_modules_search
(
dbname sysname,
objname sysname,
schemaname sysname,
type VARCHAR(10),
searchedstring NVARCHAR(255)
);

SET @SearchQuery = 
'USE [?];
INSERT INTO ##sql_modules_search
SELECT DB_NAME(), o.name objname, s.name schemaname,  o.type, '''+ @SearchString +''' searchedstring
FROM sys.all_sql_modules m INNER JOIN sys.objects o ON o.object_id=m.object_id INNER JOIN sys.schemas s ON o.schema_id=s.schema_id
WHERE definition collate FRENCH_CI_AS LIKE ''%'+ @SearchString +'%''';

EXECUTE master.sys.sp_MSforeachdb
@SearchQuery


SELECT * FROM ##sql_modules_search
WHERE dbname <>'msdb';
