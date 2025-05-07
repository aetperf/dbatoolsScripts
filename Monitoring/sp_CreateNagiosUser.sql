CREATE PROCEDURE [dbo].[sp_CreateNagiosUser]
    @NagiosPassword NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @dbname NVARCHAR(255);
    DECLARE @check_mssql_health_USER NVARCHAR(255);
    DECLARE @check_mssql_health_ROLE NVARCHAR(255);
    DECLARE @source NVARCHAR(255);
    DECLARE @options NVARCHAR(255);
    DECLARE @backslash INT;

    SET @check_mssql_health_USER = 'nagios';
    SET @check_mssql_health_ROLE = 'monitoring';

    SET @options = 'DEFAULT_DATABASE=MASTER, DEFAULT_LANGUAGE=English';
    SET @backslash = CHARINDEX('\', @check_mssql_health_USER);

    IF @backslash > 0
    BEGIN
        SET @source = ' FROM WINDOWS';
        SET @options = ' WITH ' + @options;
    END
    ELSE
    BEGIN
        SET @source = '';
        SET @options = ' WITH PASSWORD=''' + @NagiosPassword + ''', ' + @options;
    END

    PRINT 'Creating Nagios plugin user ' + @check_mssql_health_USER;

	IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @check_mssql_health_USER)
	BEGIN
		EXEC ('CREATE LOGIN ' + @check_mssql_health_USER + @source + @options);
		PRINT 'Login ' + @check_mssql_health_USER + ' created.';
	END
	ELSE
	BEGIN
		PRINT 'Login ' + @check_mssql_health_USER + ' already exists. Skipping creation.';
	END

	EXEC ('USE MASTER; GRANT VIEW SERVER STATE TO ' + @check_mssql_health_USER);
	EXEC ('USE MASTER; GRANT ALTER TRACE TO ' + @check_mssql_health_USER);
	EXEC ('USE MASTER; GRANT VIEW ANY DEFINITION TO ' + @check_mssql_health_USER);

	IF NOT EXISTS (
		SELECT 1 FROM msdb.sys.database_principals 
		WHERE name = @check_mssql_health_USER
	)
	BEGIN
		EXEC ('USE MSDB; CREATE USER ' + @check_mssql_health_USER + ' FOR LOGIN ' + @check_mssql_health_USER);
		PRINT 'User ' + @check_mssql_health_USER + ' created in MSDB.';
	END
	ELSE
	BEGIN
		PRINT 'User ' + @check_mssql_health_USER + ' already exists in MSDB. Skipping creation.';
	END

	EXEC ('USE MSDB; EXEC sp_addrolemember ''db_datareader'', ''' + @check_mssql_health_USER + '''');


    DECLARE dblist CURSOR FOR
        SELECT name FROM sysdatabases WHERE name NOT IN ('master', 'tempdb', 'msdb');

    OPEN dblist;
    FETCH NEXT FROM dblist INTO @dbname;

    WHILE @@FETCH_STATUS = 0
	BEGIN
		PRINT 'Granting permissions in DB "' + @dbname + '"';

		EXEC (
			'USE [' + @dbname + '];
			 IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = ''' + @check_mssql_health_ROLE + ''')
			 BEGIN
				 CREATE ROLE ' + @check_mssql_health_ROLE + ';
			 END'
		);

		EXEC ('USE [' + @dbname + ']; GRANT EXECUTE TO ' + @check_mssql_health_ROLE);
		EXEC ('USE [' + @dbname + ']; GRANT VIEW DATABASE STATE TO ' + @check_mssql_health_ROLE);
		EXEC ('USE [' + @dbname + ']; GRANT VIEW DEFINITION TO ' + @check_mssql_health_ROLE);

		EXEC (
			'USE [' + @dbname + '];
			 IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = ''' + @check_mssql_health_USER + ''')
			 BEGIN
				 CREATE USER ' + @check_mssql_health_USER + ' FOR LOGIN ' + @check_mssql_health_USER + ';
			 END
			 ELSE
			 BEGIN
				 ALTER USER ' + @check_mssql_health_USER + ' WITH LOGIN = ' + @check_mssql_health_USER + ';
			 END'
		);

		EXEC ('USE [' + @dbname + ']; EXEC sp_addrolemember ''' + @check_mssql_health_ROLE + ''', ''' + @check_mssql_health_USER + '''');
		PRINT 'Permissions granted in DB "' + @dbname + '"';

		FETCH NEXT FROM dblist INTO @dbname;
	END


    CLOSE dblist;
    DEALLOCATE dblist;
END;
