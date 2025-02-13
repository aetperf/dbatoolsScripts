CREATE OR ALTER PROCEDURE [dbo].[CreateLoginFromAD]
/*
Samples :
EXEC [dbo].[CreateLoginFromAD] @Domain='AAZTEC', @RouteDN='LDAP://OU=MSS,OU=Groupes,DC=aaztec,DC=dom', @ObjectClass='group', @Execute=1, @Debug=1

*/
@Domain NVARCHAR(255) = 'ADAlwaysOn',
@RouteDN NVARCHAR(255) = 'LDAP://OU=MSS,OU=Groups,DC=ADAlwaysOn,DC=local',
@ObjectClass NVARCHAR(100)='group',
@Execute BIT = 0,
@Debug BIT = 1,
@Force BIT = 0,
@LDAPSearchUser NVARCHAR(500) = '',
@LDAPSearchUserPassword NVARCHAR(500)='',
@GroupNameLike NVARCHAR(100)='%'

AS

BEGIN
       DECLARE @GroupName NVARCHAR(255);
       DECLARE @SQL NVARCHAR(MAX);
       DECLARE @SQLOpenRowSet NVARCHAR(MAX);
	   DECLARE @SQLOpenRowSetHide NVARCHAR(MAX);
       DECLARE @SQLDropLogin  NVARCHAR(MAX);
       DECLARE @ErrorMessage NVARCHAR(4000);
       DECLARE @LDAPSearchUserAndPassword NVARCHAR(500) = '';
	   DECLARE @LDAPSearchUserAndPasswordHide NVARCHAR(500) = '';

       IF (@LDAPSearchUser<>'')
			BEGIN
              SET @LDAPSearchUserAndPassword = ';'''+@LDAPSearchUser+''';'''+ @LDAPSearchUserPassword+'''';
			  SET @LDAPSearchUserAndPassword = ';'''+@LDAPSearchUser+''';''*************''';
			END

       SET @SQLOpenRowSet='SELECT CN FROM 
		OPENROWSET(''ADsDSOObject'', 
               ''adsdatasource'' '+@LDAPSearchUserAndPassword+' , 
                     ''SELECT CN FROM '''''+@RouteDN+''''' WHERE objectClass = '''''+@ObjectClass+''''''') AS ADGroups;';
		SET @SQLOpenRowSetHide='SELECT CN FROM 
		OPENROWSET(''ADsDSOObject'', 
               ''adsdatasource'' '+@LDAPSearchUserAndPassword+' , 
                     ''SELECT CN FROM '''''+@RouteDN+''''' WHERE objectClass = '''''+@ObjectClass+''''''') AS ADGroups;';

       DROP TABLE IF EXISTS #ADGroups;
       CREATE TABLE #ADGroups (GroupName NVARCHAR(255));

       IF @Debug=1
              PRINT @SQLOpenRowSetHide;

       INSERT INTO #ADGroups (GroupName)
       EXEC sp_executesql @SQLOpenRowSet;

       DECLARE group_cursor CURSOR FOR
       SELECT GroupName FROM #ADGroups WHERE GroupName LIKE ''+@GroupNameLike+'';

       OPEN group_cursor;

       FETCH NEXT FROM group_cursor INTO @GroupName;
       WHILE @@FETCH_STATUS = 0
              BEGIN
                     SET @SQL = 'CREATE LOGIN [' + @Domain + '\' + @GroupName + '] FROM WINDOWS;';   
                     SET @SQLDropLogin = 'IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '''+ @Domain + '\' + @GroupName +''' AND type = ''S'') BEGIN DROP LOGIN [ADAlwaysOn\Group2]; END'
                     IF @Force=1 AND @Debug=1
                            BEGIN
                                   RAISERROR('Drop Login %s if exists', 1, 1, @GroupName) WITH NOWAIT;
                                   RAISERROR('SQL Statement : %s', 1, 1, @SQLDropLogin) WITH NOWAIT;
                            END
                     IF @Execute=1 AND @Force=1
                            EXEC sp_executesql @SQLDropLogin;

                     IF @Debug=1
                            BEGIN
                                   RAISERROR('Create Login %s', 1, 1, @GroupName) WITH NOWAIT;
                                   RAISERROR('SQL Statement : %s', 1, 1, @SQL) WITH NOWAIT;
                            END
                     IF @Execute=1
                            BEGIN TRY
                                   EXEC sp_executesql @SQL;
                            END TRY
                            BEGIN CATCH 
                                   SET @ErrorMessage=ERROR_MESSAGE();       
                                   RAISERROR('Error message : %s', 1, 1, @ErrorMessage) WITH NOWAIT; 
                            END CATCH
                            
    
                     FETCH NEXT FROM group_cursor INTO @GroupName;
              END

       DROP TABLE #ADGroups;
       CLOSE group_cursor;
       DEALLOCATE group_cursor;
END;
