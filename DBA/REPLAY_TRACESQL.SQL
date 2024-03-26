DECLARE @DBNAME SYSNAME
DECLARE @SQLCMD NVARCHAR(max)

-- create a cursor for retrieving the database names and sqlcmd
DECLARE sql_cursor CURSOR FOR
SELECT DatabaseName, TextData
  FROM [EnedisTrace].[dbo].[ZPWC00CT]
  where Duration is not null and EventClass=12
  order by DatabaseName;

-- open the cursor
OPEN sql_cursor
FETCH NEXT FROM sql_cursor INTO @DBNAME, @SQLCMD

-- loop through the cursor
WHILE @@FETCH_STATUS = 0
BEGIN

    -- print the database name  
    --RAISERROR('Database: %s', 0, 1, @DBNAME) WITH NOWAIT;
    

    SET @SQLCMD = 'USE ' + @DBNAME + ';
	' + CHAR(10) + @SQLCMD;

    print cast( substring(@SQLCMD, 1, 16000) as ntext )
    print cast( substring(@SQLCMD, 16001, 32000) as ntext )
    print cast( substring(@SQLCMD, 32001, 48000) as ntext )
    print cast( substring(@SQLCMD, 48001, 64000) as ntext )
    print cast( substring(@SQLCMD, 64001, 80000) as ntext )
    print cast( substring(@SQLCMD, 80001, 96000) as ntext )
    print cast( substring(@SQLCMD, 96001, 112000) as ntext )
    print cast( substring(@SQLCMD, 112001, 128000) as ntext )
    print cast( substring(@SQLCMD, 128001, 144000) as ntext )
    print cast( substring(@SQLCMD, 144001, 160000) as ntext )
    
        
    -- execute the sqlcmd  
    EXEC sp_executesql @SQLCMD;

  -- get the next sqlcmd
  FETCH NEXT FROM sql_cursor INTO @DBNAME, @SQLCMD
END

CLOSE sql_cursor;