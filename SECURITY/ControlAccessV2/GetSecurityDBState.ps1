<#
.SYNOPSIS
    Audits SQL Server logins and their AD group members, then saves the results in a dedicated table.
.DESCRIPTION
    This script:
    - Retrieves all logins from the SQL Server instance.
    - For each login, checks if it is an AD group and recursively lists its members.
    - Creates the LoginsSecurityHistory table if it does not exist.
    - Inserts the results into this table.
.PARAMETER ServerInstance
    Name of the SQL Server instance (e.g., "SRVSQL" or "SRVSQL\INSTANCE").
.PARAMETER LogDatabaseName
    Name of the database where to create/store the table (default: "DBATOOLS").
.EXAMPLE
    .\CheckRoleDb.ps1 -ServerInstance "localhost" -LogDatabaseName "DBATOOLS"
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$ServerInstance,
    [string]$LogDatabaseName = "DBATOOLS"
)

# Check and load required modules
if (-not (Get-Module -Name dbatools -ListAvailable)) {
    Write-Error "The dbatools module is required. Install it with: Install-Module -Name dbatools -Force -AllowClobber -Scope CurrentUser"
    exit 1
}
if (-not (Get-Module -Name ActiveDirectory -ListAvailable)) {
    Write-Error "The ActiveDirectory module is required. Install it with: Install-WindowsFeature RSAT-AD-PowerShell"
    exit 1
}
Import-Module dbatools, ActiveDirectory -ErrorAction Stop

# Create the LoginsSecurityHistory table if it does not exist
$createTableSql = "
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'security.LoginsSecurityHistory')
BEGIN
    CREATE TABLE [$LogDatabaseName].[security].[LoginsSecurityHistory](
        [Id] [int] IDENTITY(1,1) NOT NULL,
        [GroupName] [nvarchar](255) NULL,
        [LoginName] [nvarchar](255) NOT NULL,
        [LoginType] [nvarchar](255) NOT NULL,
        [MemberName] [nvarchar](255) NULL,
        [MemberType] [nvarchar](50) NULL,
        [AuditDate] [datetime] NOT NULL DEFAULT (GETDATE()),
        CONSTRAINT [PK_LoginsSecurityHistory] PRIMARY KEY CLUSTERED ([Id] ASC)
    )
END
"

$AuditDate = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
try {
    # Connect to the SQL Server instance
    $instance = Connect-DbaInstance -SqlInstance $ServerInstance
    # Create the table if necessary
    Invoke-DbaQuery -SqlInstance $instance -Database $LogDatabaseName -Query $createTableSql -ErrorAction Stop
    # Retrieve all logins from the instance
    $logins = Get-DbaLogin -SqlInstance $instance
    foreach ($login in $logins) {
        $isGroup = $false
        $groupName = $null
        $memberName = $null
        $memberType = $null
        # Check if the login is an AD group
        try {
            if ($login.LoginType -eq "WindowsGroup") {
                $adServer = $login.Name.Split('\')[0]
                $groupIdentity = $login.Name.Split('\')[1]
                $isGroup = $true
                $groupName = $login.Name
                # Recursively retrieve group members
                $members = Get-ADGroupMember -Identity $groupIdentity -Server $adServer -Recursive
                foreach ($member in $members) {
                    $memberName = $member.SamAccountName
                    $memberType = $member.objectClass -join ","
                    # Insert into the table
                    $insertSql = "
                    INSERT INTO [$LogDatabaseName].[security].[LoginsSecurityHistory]
                    (GroupName, LoginName, LoginType, MemberName, MemberType, AuditDate)
                    VALUES
                    ('$groupName', '$($login.Name)', '$($login.LoginType)', '$memberName', '$memberType', '$AuditDate')
                    "
                    Invoke-DbaQuery -SqlInstance $instance -Database $LogDatabaseName -Query $insertSql -ErrorAction Stop
                }
            }
        } catch {
            Write-Warning "Unable to check login $($login.Name) in AD: $_"
        }
        # Insert the login itself (even if not a group)
        if (-not $isGroup) {
            $insertSql = "
            INSERT INTO [$LogDatabaseName].[security].[LoginsSecurityHistory]
            (GroupName, LoginName, LoginType, MemberName, MemberType, AuditDate)
            VALUES
            (NULL, '$($login.Name)', '$($login.LoginType)', NULL, NULL, '$AuditDate')
            "
            Invoke-DbaQuery -SqlInstance $instance -Database $LogDatabaseName -Query $insertSql -ErrorAction Stop
        }
    }
    Write-Host "Audit complete. Results are saved in [$LogDatabaseName].[dbo].[LoginsSecurityHistory]."
    # Create the LoginsPermissionsHistory table if it does not exist
    $createPermTableSql = "
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'security.LoginsPermissionsHistory')
    BEGIN
        CREATE TABLE [$LogDatabaseName].[security].[LoginsPermissionsHistory](
            [Id] INT IDENTITY(1,1) PRIMARY KEY,
            [DatabaseName] NVARCHAR(255) NOT NULL,
            [LoginName] NVARCHAR(255) NOT NULL,
            [RoleName] NVARCHAR(255) NULL,
            [AuditDate] DATETIME NOT NULL DEFAULT (GETDATE())
        )
    END
    "
    Invoke-DbaQuery -SqlInstance $instance -Database $LogDatabaseName -Query $createPermTableSql -ErrorAction Stop
    $AuditDate = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    # Retrieve all databases (except system)
    $databases = Get-DbaDatabase -SqlInstance $instance | Where-Object { -not $_.IsSystemObject }
    foreach ($db in $databases) {
        Write-Host "Analyzing database $($db.Name)..."
        # Retrieve database roles and their members
        $roleMembers = Get-DbaDbRoleMember -SqlInstance $instance -Database $db.Name
        foreach ($rm in $roleMembers) {
            $insertSql = "
            INSERT INTO [$LogDatabaseName].[security].[LoginsPermissionsHistory]
            (DatabaseName, LoginName, RoleName, AuditDate)
            VALUES
            ('$($db.Name)', '$($rm.UserName)', '$($rm.Role)', '$AuditDate')
            "
            Invoke-DbaQuery -SqlInstance $instance -Database $LogDatabaseName -Query $insertSql
        }
    }
    exit 0
}
Catch {
    Write-Error "Error: $_"
    exit 2
}
