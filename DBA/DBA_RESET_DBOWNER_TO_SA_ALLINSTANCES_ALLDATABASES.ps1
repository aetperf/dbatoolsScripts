#reset owner of all databases of all instances to 'sa'

$MSSQLInstances = Get-DbaRegServer -SqlInstance grasrlyosqldba\DBA01 -Group ALL | Select-Object -Unique -ExpandProperty ServerName
$MSSQLInstances | Set-DbaDbOwner 