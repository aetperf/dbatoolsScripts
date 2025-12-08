<#

 

.SYNOPSIS

    Audite les logins SQL Server et leurs membres AD, puis enregistre les résultats dans une table dédiée.

 

.DESCRIPTION

    Ce script :

    - Récupère tous les logins de l'instance SQL Server.

    - Pour chaque login, vérifie s'il s'agit d'un groupe AD et liste ses membres de manière récursive.

    - Crée les tables de tracking si elles n'existent pas.

    - Insère les résultats dans les tables LoginsSecurityHistory et LoginsPermissionsHistory.

 

.PARAMETER ServerInstance

    Nom de l'instance SQL Server (ex: "SRVSQL" ou "SRVSQL\INSTANCE").

 

.PARAMETER LogDatabaseName

    Nom de la base de données où créer/stocker la table (par défaut: "DBATOOLS").

 

.PARAMETER AuditDate

    Date et heure de l'audit. Par défaut : maintenant.

 

.EXAMPLE

    .\CheckRoleDb.ps1 -ServerInstance "localhost" -LogDatabaseName "DBATOOLS"

 

.EXAMPLE

    .\CheckRoleDb.ps1 -ServerInstance "localhost" -AuditDate "2025-01-01 00:00:00"

 

#>

 

param(

    [Parameter(Mandatory=$true)]

    [string]$ServerInstance,

 

    [string]$LogDatabaseName = "DBATOOLS"

 

    # [string]$AuditDate = "",

    # [string]$AuditTime = ""

)

 

# if ($AuditDate -eq "" -or $AuditTime -eq ""){

  

# } else {

#     $DateFull = "$AuditDate$AuditTime"

#     $DateTime = [DateTime]::ParseExact($DateFull,"yyyyMMddHHmmss",$null)

#     $AuditDateSql = $DateTime.ToString("yyyy-MM-dd HH:mm:ss")

# }

 

$AuditDateSql = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")

 

# Vérification des modules

if (-not (Get-Module -Name dbatools -ListAvailable)) {

    Write-Error "Le module dbatools est requis. Installez-le avec : Install-Module -Name dbatools -Force -AllowClobber -Scope CurrentUser"

    exit 1

}

 

if (-not (Get-Module -Name ActiveDirectory -ListAvailable)) {

    Write-Error "Le module ActiveDirectory est requis. Installez-le avec : Install-WindowsFeature RSAT-AD-PowerShell"

    exit 1

}

 

Import-Module dbatools, ActiveDirectory -ErrorAction Stop

 

 

# --- Création de la table LoginsSecurityHistory ---

$createTableSql = @"

IF NOT EXISTS (

    SELECT 1 FROM sys.tables t

    JOIN sys.schemas s ON s.schema_id = t.schema_id

    WHERE t.name = 'LoginsSecurityHistory' AND s.name = 'security'

)

BEGIN

    CREATE TABLE [$LogDatabaseName].[security].[LoginsSecurityHistory](

        [Id] INT IDENTITY(1,1) PRIMARY KEY,

        [GroupName] NVARCHAR(255) NULL,

        [LoginName] NVARCHAR(255) NOT NULL,

        [LoginType] NVARCHAR(255) NOT NULL,

        [MemberName] NVARCHAR(255) NULL,

        [MemberType] NVARCHAR(50) NULL,

        [AuditDate] DATETIME NOT NULL DEFAULT (GETDATE())

    )

END

"@

 

 

# --- Script principal ---

try {

    # Connexion SQL

    $instance = Connect-DbaInstance -SqlInstance $ServerInstance

 

    # Création de la table d'audit des logins si nécessaire

    Invoke-DbaQuery -SqlInstance $instance -Database $LogDatabaseName -Query $createTableSql -ErrorAction Stop

 

    # Récupération de tous les logins

    $logins = Get-DbaLogin -SqlInstance $instance

 

    foreach ($login in $logins) {

 

        $isGroup = $false

        $groupName = $null

        $memberName = $null

        $memberType = $null

 

        # Vérifier si c'est un groupe AD

        try {

            if ($login.LoginType -eq "WindowsGroup") {

 

                $adServer     = $login.Name.Split('\')[0]

                $groupIdentity = $login.Name.Split('\')[1]

 

                $isGroup = $true

                $groupName = $login.Name

 

                # Récupérer récursivement les membres AD

                $members = Get-ADGroupMember -Identity $groupIdentity -Server $adServer -Recursive

 

                foreach ($member in $members) {

 

                    $memberName = $member.SamAccountName

                    $memberType = $member.objectClass -join ","

 

                    # INSERT SQL

                    $insertSql = @"

INSERT INTO [$LogDatabaseName].[security].[LoginsSecurityHistory]

(GroupName, LoginName, LoginType, MemberName, MemberType, AuditDate)

VALUES

('$groupName', '$($login.Name)', '$($login.LoginType)', '$memberName', '$memberType', '$AuditDateSql')

"@

 

                    Invoke-DbaQuery -SqlInstance $instance -Database $LogDatabaseName -Query $insertSql -ErrorAction Stop

                }

            }

        }

        catch {

            Write-Warning "Impossible de vérifier le login $($login.Name) dans l'AD : $_"

        }

 

        # Insérer le login même si ce n'est pas un groupe

        if (-not $isGroup) {

            $insertSql = @"

INSERT INTO [$LogDatabaseName].[security].[LoginsSecurityHistory]

(GroupName, LoginName, LoginType, MemberName, MemberType, AuditDate)

VALUES

(NULL, '$($login.Name)', '$($login.LoginType)', NULL, NULL, '$AuditDateSql')

"@

 

            Invoke-DbaQuery -SqlInstance $instance -Database $LogDatabaseName -Query $insertSql -ErrorAction Stop

        }

    }

 

 

    Write-Host "Audit des logins terminé."

 

 

    # ----- Création de la table LoginsPermissionsHistory -----

    $createPermTableSql = @"

IF NOT EXISTS (

    SELECT 1 FROM sys.tables t

    JOIN sys.schemas s ON s.schema_id = t.schema_id

    WHERE t.name = 'LoginsPermissionsHistory' AND s.name = 'security'

)

BEGIN

    CREATE TABLE [$LogDatabaseName].[security].[LoginsPermissionsHistory](

        [Id] INT IDENTITY(1,1) PRIMARY KEY,

        [DatabaseName] NVARCHAR(255) NOT NULL,

        [LoginName] NVARCHAR(255) NOT NULL,

        [RoleName] NVARCHAR(255) NULL,

        [AuditDate] DATETIME NOT NULL DEFAULT (GETDATE()),

        [HasDbAccess]  BIT NULL,

    )

END

"@

 

    Invoke-DbaQuery -SqlInstance $instance -Database $LogDatabaseName -Query $createPermTableSql -ErrorAction Stop

 

 

    # --- Audit des permissions ---

    $databases = Get-DbaDatabase -SqlInstance $instance | Where-Object { -not $_.IsSystemObject }

 

    foreach ($db in $databases) {

 

        Write-Host "Analyse de la base $($db.Name)..."

 

        # Requête SQL : uniquement les users avec un rôle !!!

        $sqlPerm = @"

INSERT INTO [$LogDatabaseName].[security].[LoginsPermissionsHistory]

(DatabaseName, LoginName, RoleName, AuditDate, HasDbAccess)

SELECT

    '$($db.Name)'           AS DatabaseName,

    dp.name                AS LoginName,

    r.name                 AS RoleName,

    '$AuditDateSql'        AS AuditDate,

    u.hasdbaccess          AS HasDbAccess

FROM [$($db.Name)].sys.database_principals dp

JOIN [$($db.Name)].sys.sysusers u

    ON u.uid = dp.principal_id

JOIN [$($db.Name)].sys.database_role_members drm     

    ON dp.principal_id = drm.member_principal_id

JOIN [$($db.Name)].sys.database_principals r

    ON drm.role_principal_id = r.principal_id

WHERE

    dp.type IN ('S','U','G')

    AND dp.name NOT IN ('dbo','guest','sys','INFORMATION_SCHEMA');

"@

 

        Invoke-DbaQuery -SqlInstance $instance -Database $LogDatabaseName -Query $sqlPerm

    }

 

 

 

    Write-Host "Audit terminé avec succès."

    exit 0

}

 

catch {

    Write-Error "Erreur : $_"

    exit 2

}