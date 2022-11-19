# EnableException for all dbatools commands and ErrorAction Stop for all commands so


$PSDefaultParameterValues['*-Dba*:EnableException'] = $true
$PSDefaultParameterValues['*:EnableException'] = "Stop"



Start-Transcript -Path "D:\MSSQL\MSSQL15.DBA01\MSSQL\JOBS\POWERTOOLS_Audit.log" -Force

$startdate = $(GET-DATE)
Write-Output "Starting"

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

Write-Output "Get and Write MSSQLInstanceConfigurations"
#Get and Write MSSQLInstanceConfigurations
try{
	$SQLconfigurations = $MSSQLInstances | Get-DbaSpConfigure
	$SQLconfigurations | Add-Member -MemberType NoteProperty "ExtractDatetime" -Value $startdate
	Write-DbaDbTableData -SqlInstance grasrlyosqldba\DBA01 -InputObject $SQLConfigurations -Table MSSQLInstanceConfigurations -Database DBATOOLS -AutoCreateTable -Truncate
	
}
catch
{
	$errormsg = $_.Exception.GetBaseException()
	Write-Output "There was an error - $errormsg"
	$results
	Stop-Transcript
	[System.Environment]::Exit(1)
}

Write-Output "Get and Write MSSQLDatabasesProperties"
#Get and Write MSSQLDatabasesProperties
try
{
	$MSSQLDatabasesProperties = $MSSQLInstances | Get-DbaDatabase | select SqlInstance, Name, RecoveryModel,SizeMB,Compatibility,Collation,Owner
	$MSSQLDatabasesProperties | Add-Member -MemberType NoteProperty "ExtractDatetime" -Value $startdate
	Write-DbaDbTableData -SqlInstance grasrlyosqldba\DBA01 -InputObject $MSSQLDatabasesProperties -Table MSSQLDatabasesProperties -Database DBATOOLS -AutoCreateTable -Truncate
 }
catch
{
	$errormsg = $_.Exception.GetBaseException()
	Write-Output "There was an error - $errormsg"
	$results
	Stop-Transcript
	[System.Environment]::Exit(1)
}

Write-Output "Get and Write MSSQLDbRoleMember"
#Get and Write MSSQLDbRoleMember
try
{
	$DbRoleMember = $MSSQLInstances | Get-DbaDbRoleMember | select SqlInstance,Database,Role,UserName,Login
	$DbRoleMember | Add-Member -MemberType NoteProperty "ExtractDatetime" -Value $startdate
	Write-DbaDbTableData -SqlInstance grasrlyosqldba\DBA01 -InputObject $DbRoleMember -Table MSSQLDbRoleMember -Database DBATOOLS -AutoCreateTable -Truncate
}
catch
{
	$errormsg = $_.Exception.GetBaseException()
	Write-Output "There was an error - $errormsg"
	$results
	Stop-Transcript
	[System.Environment]::Exit(1)
}

Write-Output "Get and Write MSSQLSysAdminLogins"
#Get and Write MSSQLSysAdminLogins
try{
	$QueryGetSysAdminLoginsInfo = 'SELECT @@SERVERNAME SqlInstance,sid, status, name,createdate CreateDate,updatedate Date_Last_Modified , dbname Default_Database,''sysadmin'' Role,getdate() ExtractDatetime  FROM SYS.syslogins WHERE sysadmin=1'
	#$SysAdminLogins = $MSSQLInstances | Get-DbaServerRoleMember -ServerRole 'sysadmin' | select SqlInstance, Role, Name
	$SysAdminLogins = $MSSQLInstances | Get-DbaDatabase -Database master | Invoke-DbaQuery -Query $QueryGetSysAdminLoginsInfo
	#$SysAdminLogins | Add-Member -MemberType NoteProperty "ExtractDatetime" -Value $startdate
	Write-DbaDbTableData -SqlInstance grasrlyosqldba\DBA01 -InputObject $SysAdminLogins -Table MSSQLSysAdminLogins -Database DBATOOLS -AutoCreateTable -Truncate
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





