WITH TAVGLENPROFILE AS
(
SELECT string_agg('CAST(AVG(DATALENGTH('+CAST(quotename(c.name) as nvarchar(max))+')) as nvarchar(4000)) '+ quotename(c.name),',') col_profile ,OBJECT_NAME(c.object_id) table_name, SCHEMA_NAME([schema_id]) [schema_name], string_agg(quotename(c.name),',') colname
from sys.all_columns c inner join sys.all_objects o on o.object_id=c.object_id
where o.type='U' and o.object_id>0
group by OBJECT_NAME(c.object_id),SCHEMA_NAME([schema_id])
),
TMAXLENPROFILE AS 
(
SELECT string_agg('CAST(MAX(DATALENGTH('+CAST(quotename(c.name) as nvarchar(max))+')) as nvarchar(4000))'+ quotename(c.name),',') col_profile ,OBJECT_NAME(c.object_id) table_name, SCHEMA_NAME([schema_id]) [schema_name], string_agg(quotename(c.name),',') colname
from sys.all_columns c inner join sys.all_objects o on o.object_id=c.object_id
where o.type='U' and o.object_id>0
group by OBJECT_NAME(c.object_id),SCHEMA_NAME([schema_id])
),
TCOUNTDPROFILE AS 
(
SELECT string_agg('CAST(COUNT_BIG(DISTINCT '+CAST(quotename(c.name) as nvarchar(max))+') as nvarchar(4000))'+ quotename(c.name),',') col_profile ,OBJECT_NAME(c.object_id) table_name, SCHEMA_NAME([schema_id]) [schema_name], string_agg(quotename(c.name),',') colname
from sys.all_columns c inner join sys.all_objects o on o.object_id=c.object_id
where o.type='U' and o.object_id>0 and c.system_type_id not in (241,34,35,99,165)
group by OBJECT_NAME(c.object_id),SCHEMA_NAME([schema_id])
),
TMAXVALUEPROFILE AS 
(
SELECT string_agg('CAST(MAX('+CAST(quotename(c.name) as nvarchar(max))+') as nvarchar(4000)) '+ quotename(c.name),',') col_profile ,OBJECT_NAME(c.object_id) table_name, SCHEMA_NAME([schema_id]) [schema_name], string_agg(quotename(c.name),',') colname
from sys.all_columns c inner join sys.all_objects o on o.object_id=c.object_id
where o.type='U' and o.object_id>0 and c.system_type_id not in (104,241,35,99,34,165)
group by OBJECT_NAME(c.object_id),SCHEMA_NAME([schema_id])
)
,
TMINVALUEPROFILE AS 
(
SELECT string_agg('CAST(MIN('+CAST(quotename(c.name) as nvarchar(max))+') as nvarchar(4000)) '+ quotename(c.name),',') col_profile ,OBJECT_NAME(c.object_id) table_name, SCHEMA_NAME([schema_id]) [schema_name], string_agg(quotename(c.name),',') colname
from sys.all_columns c inner join sys.all_objects o on o.object_id=c.object_id
where o.type='U' and o.object_id>0 and c.system_type_id not in (104,241,35,99,34,165)
group by OBJECT_NAME(c.object_id),SCHEMA_NAME([schema_id])
)
,
TAVGVALUEPROFILE AS 
(
SELECT string_agg('CAST(AVG(CAST('+CAST(quotename(c.name) as nvarchar(max))+' as DECIMAL(24,2))) as nvarchar(4000)) '+ quotename(c.name),',') col_profile ,OBJECT_NAME(c.object_id) table_name, SCHEMA_NAME([schema_id]) [schema_name], string_agg(quotename(c.name),',') colname
from sys.all_columns c inner join sys.all_objects o on o.object_id=c.object_id
where o.type='U' and c.system_type_id in  (48,52,56,59,62,106,108,127)  and o.object_id>0
group by OBJECT_NAME(c.object_id),SCHEMA_NAME([schema_id])
)
SELECT *
FROM
(
	SELECT QUOTENAME(DB_NAME())  [database_name], QUOTENAME(schema_name) [schema_name], QUOTENAME(table_name) [table_name],'avg_data_len' [profile], 'SELECT DB_NAME() database_name,table_full_name, colname, ''avg_data_len'' profile,val FROM (SELECT '''+ quotename([schema_name])+'.'+quotename(table_name) +''' as table_full_name,' + col_profile +' FROM '+ quotename([schema_name])+'.'+quotename(table_name) +') src '+
	'UNPIVOT (val FOR colname in ('+colname+')) d' profile_command
	FROM TAVGLENPROFILE
	UNION ALL
	SELECT QUOTENAME(DB_NAME())  [database_name], QUOTENAME(schema_name) [schema_name], QUOTENAME(table_name) [table_name],'max_data_len' [profile], 'SELECT DB_NAME() database_name, table_full_name, colname, ''max_data_len'' profile,val FROM (SELECT '''+ quotename([schema_name])+'.'+quotename(table_name) +''' as table_full_name,' + col_profile +' FROM '+ quotename([schema_name])+'.'+quotename(table_name) +') src '+
	'UNPIVOT (val FOR colname in ('+colname+')) d' profile_command
	FROM TMAXLENPROFILE
	UNION ALL
	SELECT QUOTENAME(DB_NAME())  [database_name], QUOTENAME(schema_name) [schema_name], QUOTENAME(table_name) [table_name],'count_distinct' [profile], 'SELECT DB_NAME() database_name,table_full_name, colname, ''count_distinct'' profile,val FROM (SELECT '''+ quotename([schema_name])+'.'+quotename(table_name) +''' as table_full_name,' + col_profile +' FROM '+ quotename([schema_name])+'.'+quotename(table_name) +') src '+
	'UNPIVOT (val FOR colname in ('+colname+')) d' profile_command
	FROM TCOUNTDPROFILE
	UNION ALL
	SELECT QUOTENAME(DB_NAME())  [database_name], QUOTENAME(schema_name) [schema_name], QUOTENAME(table_name) [table_name],'min_value' [profile], 'SELECT DB_NAME() database_name,table_full_name, colname, ''min_value'' profile,val FROM (SELECT '''+ quotename([schema_name])+'.'+quotename(table_name) +''' as table_full_name,' + col_profile +' FROM '+ quotename([schema_name])+'.'+quotename(table_name) +') src '+
	'UNPIVOT (val FOR colname in ('+colname+')) d' profile_command
	FROM TMINVALUEPROFILE
	UNION ALL
	SELECT QUOTENAME(DB_NAME()) [database_name], QUOTENAME(schema_name) [schema_name], QUOTENAME(table_name) [table_name],'max_value' [profile], 'SELECT DB_NAME() database_name,table_full_name, colname, ''max_value'' profile,val FROM (SELECT '''+ quotename([schema_name])+'.'+quotename(table_name) +''' as table_full_name,' + col_profile +' FROM '+ quotename([schema_name])+'.'+quotename(table_name) +') src '+
	'UNPIVOT (val FOR colname in ('+colname+')) d' profile_command
	FROM TMAXVALUEPROFILE	
	UNION ALL
	SELECT QUOTENAME(DB_NAME())  [database_name], QUOTENAME(schema_name) [schema_name], QUOTENAME(table_name) [table_name],'avg_value' [profile], 'SELECT DB_NAME() database_name,table_full_name, colname, ''avg_value'' profile,val FROM (SELECT '''+ quotename([schema_name])+'.'+quotename(table_name) +''' as table_full_name,' + col_profile +' FROM '+ quotename([schema_name])+'.'+quotename(table_name) +') src '+
	'UNPIVOT (val FOR colname in ('+colname+')) d' profile_command
	FROM TAVGVALUEPROFILE	
) tcmd
order by 1,2,3,4;






