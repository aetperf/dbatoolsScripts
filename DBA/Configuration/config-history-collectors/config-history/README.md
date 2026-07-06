# Windows / SQL Server configuration history collectors

## 1. Create repository tables

```powershell
sqlcmd -S localhost -d DBATOOLS -E -i .\00_CreateConfigHistorySchema.sql
```

## 2. Run all collectors from a SQL Server host

```powershell
$repo = "Server=localhost;Database=DBATOOLS;Integrated Security=True;TrustServerCertificate=True;Application Name=ConfigHistoryCollector;"
$instances = @("localhost", "localhost\INST2")

.\Run-All.ps1 -RepositoryConnectionString $repo -SqlInstance $instances
```

## 3. Run only SQL collectors

```powershell
.\Run-All.ps1 -RepositoryConnectionString $repo -SqlInstance @("localhost") -SkipHardware -SkipOperatingSystem -SkipDefender
```

## 4. SQL authentication template

Use `{instance}` as placeholder.

```powershell
$template = "Server={instance};Database=master;User ID=collector_login;Password=***;TrustServerCertificate=True;Application Name=ConfigHistoryCollector;"
.\Run-All.ps1 -RepositoryConnectionString $repo -SqlInstance @("server1\prod") -SqlAuthenticationConnectionStringTemplate $template
```

## Notes

- PowerShell 7 is recommended.
- Run as local administrator for complete OS, disk, Defender and local security policy collection.
- SQL login needs read access to `sys.configurations`, `sys.databases`, `sys.master_files`, resource governor catalog views and `tempdb.sys.database_files`.
- `Lock pages in memory` is stored as `SeLockMemoryPrivilege`.
- `Perform volume maintenance tasks` / Instant File Initialization is stored as `SeManageVolumePrivilege`.
