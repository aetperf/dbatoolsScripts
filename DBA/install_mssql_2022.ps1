#Mount Iso File

$MountDisk=Mount-DiskImage -ImagePath "C:\Jocker\MS SQL Server\MS SQL Server 2022\SQLServer2022-x64-ENU.iso"
$driveLetter = ($MountDisk | Get-Volume).DriveLetter

$InstanceName="MUPR2201"
$Version="2022"
$Port=22433
$FeatureList="SQLEngine,FullText,BC,Conn"
$SourceMSSQLPath="${driveLetter}:"
$AuthenticationMode="Mixed"
$DataPath="D:\MSSQL"
$LogPath="E:\MSSQL"
$TempDbPath="D:\MSSQL"
$BackupPath="S:\MSSQL"
$UpdateSourcePath="C:\Jocker\MS SQL Server\MS SQL Server 2022"
$AdminAccount="CORP\SQL_PROD_SYSADMIN"
$SqlCollation="French_BIN2"
$EngineCredential="CORP\sqlsvc"

$Configuration=@{
FEATURES = $FeatureList
TCPENABLED = 1
AGTSVCSTARTUPTYPE = "Automatic"
BROWSERSVCSTARTUPTYPE = "Automatic"
IACCEPTSQLSERVERLICENSETERMS = 1
ENU = 1
}


$MSSQLInstallResult=Install-DbaInstance -InstanceName $InstanceName -Version $Version -Port $Port -Configuration $Configuration -AuthenticationMode $AuthenticationMode -Path $SourceMSSQLPath -DataPath $DataPath -LogPath $LogPath -TempPath $TempDbPath -BackupPath $BackupPath -UpdateSourcePath $UpdateSourcePath -AdminAccount $AdminAccount -SqlCollation $SqlCollation -EngineCredential $EngineCredential -PerformVolumeMaintenanceTasks -WhatIf

Return $MSSQLInstallResult