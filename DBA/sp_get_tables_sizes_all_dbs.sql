CREATE PROCEDURE [dbo].[sp_get_tables_sizes_all_dbs]
 

AS BEGIN
 
IF (SELECT count(*) FROM tempdb.sys.objects WHERE name = '##TABLESIZES_ALLDB')=1 BEGIN
DROP TABLE ##TABLESIZES_ALLDB;
END
 
CREATE TABLE ##TABLESIZES_ALLDB (
snapdate datetime,
srv nvarchar(1000),
sv nvarchar(1000),
dbname sysname,
SchemaName sysname,
TableName sysname,
"partition_id" bigint,
"partition_number" int,
lignes bigint,
"memory (kB)" bigint,
"data (kB)" bigint,
"indexes (kb)" bigint,
"data_compression" int,
data_compression_desc nvarchar(1000)
)
 IF (cast(REPLACE(LEFT(CAST( SERVERPROPERTY( 'ProductVersion' ) AS varchar( 30 ) ),2),'.','') as tinyint) > 9)
 BEGIN
EXECUTE master.sys.sp_MSforeachdb
'USE [?];
IF DB_ID(''?'') <> 2
BEGIN
insert into ##TABLESIZES_ALLDB
select getdate() as snapdate,cast(serverproperty(''MachineName'') as nvarchar(1000)) svr,cast(@@servicename as nvarchar(1000)) sv, ''?'' dbname, Object_Schema_Name(p.object_id),TableName= object_name(p.object_id),p.partition_id,p.partition_number,
lignes = sum(
CASE
When (p.index_id < 2) and (a.type = 1) Then p.rows
Else 0
END
),
''memory (kB)'' = cast(ltrim(str(sum(a.total_pages)* 8192 / 1024.,15,0)) as float),
''data (kB)'' = ltrim(str(sum(
CASE
When a.type <> 1 Then a.used_pages
When p.index_id < 2 Then a.data_pages
Else 0
END
) * 8192 / 1024.,15,0)),
''indexes (kb)'' = ltrim(str((sum(a.used_pages)-sum(
CASE
When a.type <> 1 Then a.used_pages
When p.index_id < 2 Then a.data_pages
Else 0
END) )* 8192 / 1024.,15,0)),p.data_compression,
p.data_compression_desc
 
from sys.partitions p, sys.allocation_units a ,sys.sysobjects s
where p.partition_id = a.container_id
and p.object_id = s.id and s.type = ''U'' -- User table type (system tables exclusion)
group by p.object_id,p.partition_id,p.partition_number,p.data_compression,p.data_compression_desc
order by 3 desc;
END
'
;

END
ELSE
BEGIN
EXECUTE master.sys.sp_MSforeachdb
'USE [?];
IF DB_ID(''?'') <> 2
BEGIN
insert into ##TABLESIZES_ALLDB
select getdate() as snapdate,cast(serverproperty(''MachineName'') as nvarchar(1000)) svr,cast(@@servicename as nvarchar(1000)) sv, ''?'' dbname, Object_Schema_Name(p.object_id),TableName= object_name(p.object_id),p.partition_id,p.partition_number,
lignes = sum(
CASE
When (p.index_id < 2) and (a.type = 1) Then p.rows
Else 0
END
),
''memory (kB)'' = cast(ltrim(str(sum(a.total_pages)* 8192 / 1024.,15,0)) as float),
''data (kB)'' = ltrim(str(sum(
CASE
When a.type <> 1 Then a.used_pages
When p.index_id < 2 Then a.data_pages
Else 0
END
) * 8192 / 1024.,15,0)),
''indexes (kb)'' = ltrim(str((sum(a.used_pages)-sum(
CASE
When a.type <> 1 Then a.used_pages
When p.index_id < 2 Then a.data_pages
Else 0
END) )* 8192 / 1024.,15,0)),0 data_compression,
''NONE'' data_compression_desc
 
from sys.partitions p, sys.allocation_units a ,sys.sysobjects s
where p.partition_id = a.container_id
and p.object_id = s.id and s.type = ''U'' -- User table type (system tables exclusion)
group by p.object_id,p.partition_id,p.partition_number
order by 3 desc;
END'
;

END
 
SELECT * FROM ##TABLESIZES_ALLDB
 
END
GO
