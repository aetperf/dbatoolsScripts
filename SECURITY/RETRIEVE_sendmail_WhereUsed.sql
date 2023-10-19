DROP TABLE ##sql_modules_xp_cmdshell;

CREATE TABLE ##sql_modules_xp_cmdshell
(
dbname sysname,
objname sysname,
schemaname sysname,
type VARCHAR(10)
);

EXECUTE master.sys.sp_MSforeachdb
'USE [?];
INSERT INTO ##sql_modules_xp_cmdshell
SELECT DB_NAME(), o.name objname, s.name schemaname,  o.type
FROM sys.all_sql_modules m INNER JOIN sys.objects o ON o.object_id=m.object_id INNER JOIN sys.schemas s ON o.schema_id=s.schema_id
WHERE definition collate FRENCH_CI_AS LIKE ''%xp_CmdShell%''';

SELECT * FROM ##sql_modules_xp_cmdshell;


-- Test 2

DROP TABLE ##sql_modules_xp_cmdshell;

CREATE TABLE ##sql_modules_xp_cmdshell
(
dbname sysname,
objname sysname,
schemaname sysname,
type VARCHAR(10)
);

EXECUTE master.sys.sp_MSforeachdb
'USE [?];
IF DB_ID(''?'') >4
INSERT INTO ##sql_modules_xp_cmdshell
SELECT DB_NAME() dbname, o.name objname, s.name schemaname,  o.type
FROM sys.all_sql_modules m INNER JOIN sys.objects o ON o.object_id=m.object_id INNER JOIN sys.schemas s ON o.schema_id=s.schema_id
WHERE definition collate FRENCH_CI_AS LIKE ''%xp\_cmdshell%'' escape ''\''
AND definition collate FRENCH_CI_AS LIKE ''%\\\\%'' escape ''\''';

SELECT * FROM ##sql_modules_xp_cmdshell 
WHERE dbname<>'DBATOOLS';