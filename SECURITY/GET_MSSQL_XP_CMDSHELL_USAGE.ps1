# EnableException for all dbatools commands and ErrorAction Stop for all commands so


$PSDefaultParameterValues['*-Dba*:EnableException'] = $true
$PSDefaultParameterValues['*:EnableException'] = "Stop"

##################################
## Searched String
##################################
$p_SearchedString = 'cmdshell'


Start-Transcript -Path "D:\MSSQL\MSSQL15.DBA01\MSSQL\JOBS\POWERTOOLS_Audit.log" -Force

$startdate = $(GET-DATE)
Write-Output "Starting"

Write-Output "p_SearchedString = ${p_SearchedString}"

Write-Output "Get Instances using central resgitered server"
#Get Instances using central resgitered server
try
{
	$MSSQLInstances = Get-DbaRegServer -SqlInstance grasrlyosqldba\DBA01 -Group ALL | Select-Object -Unique -ExpandProperty ServerName
	Write-Output "Target List : {"
	$MSSQLInstances
	Write-Output "}"
}
catch
{
	$errormsg = $_.Exception.GetBaseException()
	Write-Output "There was an error - $errormsg"
	$results
	Stop-Transcript
	[System.Environment]::Exit(1)
}


Write-Output "Get and Write dbname and procedure name where ${p_SearchedString} is found"
#Get and Write MSSQLSysAdminLogins
try{
	#$QuerySearchString = 'SELECT @@SERVERNAME SqlInstance,sid, status, name,createdate CreateDate,updatedate Date_Last_Modified , dbname Default_Database,''sysadmin'' Role,getdate() ExtractDatetime  FROM SYS.syslogins WHERE sysadmin=1'
	$QuerySearchString = "SELECT @@SERVERNAME SqlInstance,DB_NAME() dbname, o.name objname, s.name schemaname,  o.type, '${p_SearchedString}' searchedstring FROM sys.all_sql_modules m INNER JOIN sys.objects o ON o.object_id=m.object_id INNER JOIN sys.schemas s ON o.schema_id=s.schema_id WHERE definition collate FRENCH_CI_AS LIKE '%${p_SearchedString}%'";
	$SearchStringInModules = $MSSQLInstances | Get-DbaDatabase | Invoke-DbaQuery -Query $QuerySearchString
	Write-DbaDbTableData -SqlInstance grasrlyosqldba\DBA01 -InputObject $SearchStringInModules -Table SearchStringInModules -Database DBATOOLS -AutoCreateTable -Truncate
}
catch
{
	$errormsg = $_.Exception.GetBaseException()
	Write-Output "There was an error - $errormsg"
	$results
	Stop-Transcript
	[System.Environment]::Exit(1)
}
 
$endtime = $(GET-DATE)

New-TimeSpan -Start $startdate -End $endtime
Write-Output "Completed Successfully"
Stop-Transcript

#Exit OK
[System.Environment]::Exit(0)