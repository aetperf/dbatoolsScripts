# take a database as parameter
# take a degree as paramter
# use a query to retrieve the alter order to run a use a parallel loop

param(
    [string]$database,
    [int]$degree
)

$getsqlcommands = """SELECT 'ALTER TABLE '+QUOTENAME(nc.[NAME])+' ALTER COLUMN '+ QUOTENAME(nc.ATTNAME) + ' ' + UPPER(TYPE_NAME(c.system_type_id))
+ IIF(c.system_type_id in (167,175,231,239),'('+cast(max_length/(system_type_id/100) as varchar(5))+')','')
+' NOT NULL;' sqlcmdaltercol
  FROM [DBATOOLS].[netezza].[_V_RELATION_COLUMN] nc inner join sys.all_columns c on OBJECT_NAME(c.object_id)=nc.NAME and c.name=ATTNAME
  inner join sys.tables t on c.object_id=t.object_id
  WHERE REPLACE([DATABASE],'_PPRD','')=DB_NAME()
  AND ATTNOTNULL=1 and c.is_nullable =1
  order by CRYPT_GEN_RANDOM(7) --for parallel loop limit percusion of altering the same table
  """

$altercommands = Invoke-Sqlcmd -ServerInstance "localhost" -Database $database -Query $getsqlcommands

$altercommands | ForEach-Object -Parallel {
    Invoke-Sqlcmd -ServerInstance "localhost" -Database $database -Query $_.sqlcmdaltercol
} -ThrottleLimit $degree

