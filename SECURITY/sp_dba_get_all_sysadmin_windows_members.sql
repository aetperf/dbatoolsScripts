
DROP PROCEDURE sp_dba_get_all_sysadmin_windows_members;
GO

CREATE PROCEDURE sp_dba_get_all_sysadmin_windows_members
AS

DECLARE @LoginName sysname 
DECLARE @sql NVARCHAR (2000) 

BEGIN 


CREATE TABLE #xplogininfo (
account_name nvarchar(1000), 
type SYSNAME, 
privilege SYSNAME,
mappep_login_name nvarchar(1000),
permission_path nvarchar(4000));

INSERT INTO #xplogininfo
EXEC xp_logininfo


CREATE TABLE #sysadminlist (
account_name nvarchar(1000), 
type SYSNAME, 
privilege SYSNAME,
mappep_login_name nvarchar(1000),
permission_path nvarchar(4000));



   DECLARE cur_Loginfetch CURSOR FOR 
    
   SELECT account_name 
   FROM #xplogininfo 
   WHERE 
   privilege='admin' AND type = 'group' and account_name not like 'NT %'
    
   OPEN cur_Loginfetch 
    
   FETCH NEXT FROM cur_Loginfetch INTO @LoginName 
   WHILE @@FETCH_STATUS = 0 
       BEGIN 
		   INSERT INTO #sysadminlist
           EXEC xp_logininfo @LoginName , 'members' ;

           FETCH NEXT FROM cur_Loginfetch INTO @LoginName;
       END 
   CLOSE cur_Loginfetch 
   DEALLOCATE cur_Loginfetch 

   SELECT DISTINCT * FROM #sysadminlist
   UNION
   SELECT * FROM #xplogininfo WHERE privilege='admin' AND type = 'user';

   RETURN 
END 