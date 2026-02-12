<#
.SYNOPSIS
    Audite les logins SQL Server et leurs membres AD, puis enregistre les résultats dans des tables dédiées.
    Peut auditer :
      - une ou plusieurs instances passées en paramètre
      - toutes les instances d’un groupe d’un CMS SQL Server
    Et peut :
      - centraliser les logs sur un serveur de logs
      - ou enregistrer localement sur chaque instance auditée

.DESCRIPTION
    Ce script :
    - Récupère tous les logins de chaque instance SQL Server auditée.
    - Pour chaque login, vérifie s'il s'agit d'un groupe AD et liste ses membres de manière récursive.
    - Crée les tables de tracking si elles n'existent pas (LoginsSecurityHistory et LoginsPermissionsHistory).
    - Ajoute une colonne InstanceName dans ces tables si elle n'existe pas déjà.
    - Ajoute une colonne Sid dans ces tables si elle n'existe pas déjà.
      * LoginsSecurityHistory.Sid = SID du login serveur (format hex 0x... si possible, sinon SidString)
      * LoginsPermissionsHistory.Sid = SID du user de la base (sys.sysusers.sid, stocké en hex 0x...)
    - Insère les résultats dans les tables, avec le nom de l’instance auditée.
    - (AJOUT) Audite aussi les rôles serveur (instance) via sys.server_role_members

.PARAMETER ServerInstance
    Nom(s) de l'instance SQL Server à auditer (ex: "SRVSQL" ou "SRVSQL\INSTANCE").
    Peut être une liste : "SRV1","SRV2\INST2"...
    Utilisé si CmsServer n’est pas fourni.

.PARAMETER CmsServer
    Nom de l’instance SQL Server hébergeant le CMS (Central Management Server).
    Si renseigné, on récupère la liste des instances à auditer depuis le CMS.

.PARAMETER CmsGroup
    Chemin du groupe dans le CMS, par ex : "ALL\PRD".
    Exemple complet : CMS = SERVERCMS, groupe = ALL\PRD
    → utilisation : Get-DbaRegServer -SqlInstance SERVERCMS -Group 'ALL\PRD'

.PARAMETER LogServer
    Nom de l’instance SQL Server sur laquelle centraliser les logs.
    Si non renseigné :
      - les logs sont enregistrés localement sur chaque instance auditée.

.PARAMETER LogDatabaseName
    Nom de la base de données où créer/stocker les tables de log (par défaut: "DBATOOLS").
#>

param(
    [string[]]$ServerInstance,
    [string]$CmsServer,
    [string]$CmsGroup,
    [string]$LogServer,
    [string]$LogDatabaseName = "DBATOOLS"
)

# Date/heure de l’audit
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

# Convertit un byte[] en string hex SQL "0x...."
function Convert-BytesToHexString {
    param($bytes)

    if (-not $bytes) { return $null }

    if ($bytes -is [byte[]]) {
        return "0x" + ( ($bytes | ForEach-Object { $_.ToString("X2") }) -join "" )
    }

    # fallback si déjà en string "0x..."
    $s = [string]$bytes
    if ($s -match '^0x[0-9A-Fa-f]+$') { return $s }

    return $null
}

function Invoke-InstanceSecurityAudit {
    param(
        [Parameter(Mandatory = $true)]
        $InstanceAudit,   # objet Connect-DbaInstance de l’instance auditée

        [Parameter(Mandatory = $true)]
        $InstanceLog,     # objet Connect-DbaInstance de l’instance sur laquelle on log

        [Parameter(Mandatory = $true)]
        [string]$LogDatabaseName,

        [Parameter(Mandatory = $true)]
        [string]$InstanceName  # nom de l’instance auditée (texte)
    )

    Write-Host ">>> Audit de l’instance : $InstanceName"

    $InstanceNameSql = $InstanceName.Replace("'", "''")

    # --- Création/ajout de la table LoginsSecurityHistory ---
    $createTableSql = @"
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.name = 'LoginsSecurityHistory' AND s.name = 'security'
)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'security')
    BEGIN
        EXEC('CREATE SCHEMA [security]')
    END

    CREATE TABLE [security].[LoginsSecurityHistory](
        [Id] INT IDENTITY(1,1) PRIMARY KEY,
        [InstanceName] NVARCHAR(255) NULL,
        [GroupName] NVARCHAR(255) NULL,
        [LoginName] NVARCHAR(255) NOT NULL,
        [LoginType] NVARCHAR(255) NOT NULL,
        [MemberName] NVARCHAR(255) NULL,
        [MemberType] NVARCHAR(50) NULL,
        [Sid] NVARCHAR(200) NULL,
        [AuditDate] DATETIME NOT NULL DEFAULT (GETDATE())
    )
END;

IF COL_LENGTH('security.LoginsSecurityHistory', 'InstanceName') IS NULL
BEGIN
    ALTER TABLE [security].[LoginsSecurityHistory]
    ADD [InstanceName] NVARCHAR(255) NULL;
END;

IF COL_LENGTH('security.LoginsSecurityHistory', 'Sid') IS NULL
BEGIN
    ALTER TABLE [security].[LoginsSecurityHistory]
    ADD [Sid] NVARCHAR(200) NULL;
END;
"@

    Invoke-DbaQuery -SqlInstance $InstanceLog -Database $LogDatabaseName -Query $createTableSql -ErrorAction Stop

    # ----- Création de la table LoginsPermissionsHistory -----
    $createPermTableSql = @"
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.name = 'LoginsPermissionsHistory' AND s.name = 'security'
)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'security')
    BEGIN
        EXEC('CREATE SCHEMA [security]')
    END

    CREATE TABLE [security].[LoginsPermissionsHistory](
        [Id] INT IDENTITY(1,1) PRIMARY KEY,
        [InstanceName] NVARCHAR(255) NULL,
        [DatabaseName] NVARCHAR(255) NOT NULL,
        [LoginName] NVARCHAR(255) NOT NULL,
        [RoleName] NVARCHAR(255) NULL,
        [AuditDate] DATETIME NOT NULL DEFAULT (GETDATE()),
        [HasDbAccess] BIT NULL,
        [Sid] NVARCHAR(200) NULL
    )
END;

IF COL_LENGTH('security.LoginsPermissionsHistory', 'InstanceName') IS NULL
BEGIN
    ALTER TABLE [security].[LoginsPermissionsHistory]
    ADD [InstanceName] NVARCHAR(255) NULL;
END;

IF COL_LENGTH('security.LoginsPermissionsHistory', 'Sid') IS NULL
BEGIN
    ALTER TABLE [security].[LoginsPermissionsHistory]
    ADD [Sid] NVARCHAR(200) NULL;
END;
"@

    Invoke-DbaQuery -SqlInstance $InstanceLog -Database $LogDatabaseName -Query $createPermTableSql -ErrorAction Stop

    # ----- (AJOUT) Création table LoginsServerRolesHistory -----
    $createSrvRolesTableSql = @"
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.name = 'LoginsServerRolesHistory' AND s.name = 'security'
)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'security')
    BEGIN
        EXEC('CREATE SCHEMA [security]')
    END

    CREATE TABLE [security].[LoginsServerRolesHistory](
        [Id] INT IDENTITY(1,1) PRIMARY KEY,
        [InstanceName] NVARCHAR(255) NULL,
        [LoginName] NVARCHAR(255) NOT NULL,
        [LoginType] NVARCHAR(255) NULL,
        [ServerRoleName] NVARCHAR(255) NOT NULL,
        [Sid] NVARCHAR(200) NULL,
        [AuditDate] DATETIME NOT NULL DEFAULT (GETDATE())
    )
END;
"@

    Invoke-DbaQuery -SqlInstance $InstanceLog -Database $LogDatabaseName -Query $createSrvRolesTableSql -ErrorAction Stop

    # --- Audit des logins (lecture sur InstanceAudit, insert sur InstanceLog) ---
    $logins = Get-DbaLogin -SqlInstance $InstanceAudit

    foreach ($login in $logins) {
        $isGroup    = $false
        $groupName  = $null
        $memberName = $null
        $memberType = $null

        # --- SID du login ---
        $sidValue = $null

        if ($login.PSObject.Properties.Name -contains "Sid" -and $login.Sid) {
            $sidValue = Convert-BytesToHexString $login.Sid
        }

        if (-not $sidValue -and ($login.PSObject.Properties.Name -contains "SidString") -and $login.SidString) {
            $sidValue = $login.SidString
        }

        $sidSqlValue = if ($sidValue) { "'" + $sidValue.Replace("'", "''") + "'" } else { "NULL" }

        try {
            if ($login.LoginType -eq "WindowsGroup") {

                $adServer      = $login.Name.Split('\')[0]
                $groupIdentity = $login.Name.Split('\')[1]

                $isGroup   = $true
                $groupName = $login.Name

                $members = Get-ADGroupMember -Identity $groupIdentity -Server $adServer -Recursive

                foreach ($member in $members) {

                    $memberName = $member.SamAccountName
                    $memberType = $member.objectClass -join ","

                    $groupNameSql   = $groupName.Replace("'", "''")
                    $loginNameSql   = $login.Name.Replace("'", "''")
                    $loginTypeSql   = $login.LoginType.ToString().Replace("'", "''")
                    $memberNameSql  = $memberName.Replace("'", "''")
                    $memberTypeSql  = $memberType.Replace("'", "''")

                    $insertSql = @"
INSERT INTO [security].[LoginsSecurityHistory]
(InstanceName, GroupName, LoginName, LoginType, MemberName, MemberType, Sid, AuditDate)
VALUES
('$InstanceNameSql', '$groupNameSql', '$loginNameSql', '$loginTypeSql', '$memberNameSql', '$memberTypeSql', $sidSqlValue, '$AuditDateSql');
"@

                    Invoke-DbaQuery -SqlInstance $InstanceLog -Database $LogDatabaseName -Query $insertSql -ErrorAction Stop
                }
            }
        }
        catch {
            Write-Warning "Impossible de vérifier le login $($login.Name) dans l'AD sur l'instance $InstanceName : $_"
        }

        if (-not $isGroup) {
            $loginNameSql = $login.Name.Replace("'", "''")
            $loginTypeSql = $login.LoginType.ToString().Replace("'", "''")

            $insertSql = @"
INSERT INTO [security].[LoginsSecurityHistory]
(InstanceName, GroupName, LoginName, LoginType, MemberName, MemberType, Sid, AuditDate)
VALUES
('$InstanceNameSql', NULL, '$loginNameSql', '$loginTypeSql', NULL, NULL, $sidSqlValue, '$AuditDateSql');
"@

            Invoke-DbaQuery -SqlInstance $InstanceLog -Database $LogDatabaseName -Query $insertSql -ErrorAction Stop
        }
    }

    Write-Host ">>> Audit des logins terminé pour $InstanceName."

    # ----- (AJOUT) Audit des rôles serveur (instance) -----
    Write-Host ">>> Audit des rôles serveur (instance) pour $InstanceName..."

    $serverRolesQuery = @"
SELECT
    m.name      AS LoginName,
    m.type_desc AS LoginType,
    r.name      AS ServerRoleName,
    CONVERT(varchar(200), m.sid, 1) AS SidHex
FROM sys.server_role_members rm
JOIN sys.server_principals r ON r.principal_id = rm.role_principal_id
JOIN sys.server_principals m ON m.principal_id = rm.member_principal_id
WHERE m.type IN ('S','U','G')
  AND m.name NOT LIKE '##%';
"@

    $serverRoles = Invoke-DbaQuery -SqlInstance $InstanceAudit -Database "master" -Query $serverRolesQuery -ErrorAction Stop

    foreach ($row in $serverRoles) {
        $loginNameSql = $row.LoginName.Replace("'", "''")
        $loginTypeSql = ("" + $row.LoginType).Replace("'", "''")
        $roleNameSql  = $row.ServerRoleName.Replace("'", "''")

        $sidSql = if ($row.SidHex) { "'" + $row.SidHex.Replace("'", "''") + "'" } else { "NULL" }


        $insertSrvRoleSql = @"
INSERT INTO [security].[LoginsServerRolesHistory]
(InstanceName, LoginName, LoginType, ServerRoleName, Sid, AuditDate)
VALUES
('$InstanceNameSql', '$loginNameSql', '$loginTypeSql', '$roleNameSql', $sidSql, '$AuditDateSql');
"@
        Invoke-DbaQuery -SqlInstance $InstanceLog -Database $LogDatabaseName -Query $insertSrvRoleSql -ErrorAction Stop
    }

    Write-Host ">>> Audit des rôles serveur terminé pour $InstanceName."

    # --- Audit des permissions ---
    $databases = Get-DbaDatabase -SqlInstance $InstanceAudit | Where-Object { -not $_.IsSystemObject }

    foreach ($db in $databases) {

        Write-Host "Analyse des permissions sur la base $($db.Name) de l’instance $InstanceName..."

        $permQuery = @"
SELECT
    dp.name       AS LoginName,
    r.name        AS RoleName,
    u.hasdbaccess AS HasDbAccess,
    u.sid         AS Sid
FROM sys.database_principals dp
JOIN sys.sysusers u
    ON u.uid = dp.principal_id
JOIN sys.database_role_members drm
    ON dp.principal_id = drm.member_principal_id
JOIN sys.database_principals r
    ON drm.role_principal_id = r.principal_id
WHERE
    dp.type IN ('S','U','G')
    AND dp.name NOT IN ('dbo','guest','sys','INFORMATION_SCHEMA');
"@

        $permResults = Invoke-DbaQuery -SqlInstance $InstanceAudit -Database $db.Name -Query $permQuery -ErrorAction Stop

        if (-not $permResults) { continue }

        foreach ($row in $permResults) {
            $dbNameSql     = $db.Name.Replace("'", "''")
            $loginNameSql  = $row.LoginName.Replace("'", "''")

            if ($null -ne $row.RoleName) {
                $roleNameSql       = $row.RoleName.Replace("'", "''")
                $roleNameSqlValue  = "'$roleNameSql'"
            }
            else {
                $roleNameSqlValue  = "NULL"
            }

            if ($null -eq $row.HasDbAccess) {
                $hasDbAccessVal = "NULL"
            }
            elseif ($row.HasDbAccess -eq $true -or $row.HasDbAccess -eq 1) {
                $hasDbAccessVal = "1"
            }
            else {
                $hasDbAccessVal = "0"
            }

            $sidHex = Convert-BytesToHexString $row.Sid
            $sidPermSqlValue = if ($sidHex) { "'" + $sidHex.Replace("'", "''") + "'" } else { "NULL" }

            $insertPermSql = @"
INSERT INTO [security].[LoginsPermissionsHistory]
(InstanceName, DatabaseName, LoginName, RoleName, AuditDate, HasDbAccess, Sid)
VALUES
('$InstanceNameSql', '$dbNameSql', '$loginNameSql', $roleNameSqlValue, '$AuditDateSql', $hasDbAccessVal, $sidPermSqlValue);
"@

            Invoke-DbaQuery -SqlInstance $InstanceLog -Database $LogDatabaseName -Query $insertPermSql -ErrorAction Stop
        }
    }

    Write-Host ">>> Audit des permissions terminé pour $InstanceName."
}

try {
    $instances = @()

    if ($CmsServer) {
        Write-Host "Connexion au CMS $CmsServer avec TrustServerCertificate..."
        try {
            $cmsInstance = Connect-DbaInstance -SqlInstance $CmsServer -TrustServerCertificate -ErrorAction Stop
        }
        catch {
            Write-Error "Impossible de se connecter au CMS $CmsServer (même avec -TrustServerCertificate) : $_"
            exit 1
        }

        if ($CmsGroup) {
            Write-Host "Récupération des instances à partir du CMS $CmsServer, groupe : $CmsGroup"
            $cmsServers = Get-DbaRegServer -SqlInstance $cmsInstance -Group $CmsGroup -ErrorAction Stop
        }
        else {
            Write-Host "Récupération des instances à partir du CMS : $CmsServer (tous les serveurs)"
            $cmsServers = Get-DbaRegServer -SqlInstance $cmsInstance -ErrorAction Stop
        }

        if (-not $cmsServers) {
            Write-Error "Aucun serveur trouvé dans le CMS $CmsServer pour le groupe '$CmsGroup'."
            exit 1
        }

        $instances = $cmsServers | Select-Object -ExpandProperty ServerName
    }
    else {
        if (-not $ServerInstance -or $ServerInstance.Count -eq 0) {
            Write-Error "Vous devez spécifier au moins un ServerInstance ou un CmsServer."
            exit 1
        }

        $instances = $ServerInstance
    }

    foreach ($inst in $instances) {
        Write-Host "==============================="
        Write-Host "Traitement de l’instance : $inst"
        Write-Host "==============================="

        try {
            $instanceAudit = Connect-DbaInstance -SqlInstance $inst -TrustServerCertificate -ErrorAction Stop
        }
        catch {
            Write-Warning "Impossible de se connecter à l’instance à auditer $inst (même avec -TrustServerCertificate) : $_"
            continue
        }

        if ($LogServer) {
            try {
                $instanceLog = Connect-DbaInstance -SqlInstance $LogServer -TrustServerCertificate -ErrorAction Stop
            }
            catch {
                Write-Warning "Impossible de se connecter au serveur de logs $LogServer pour l’instance $inst (même avec -TrustServerCertificate) : $_"
                Write-Warning "Abandon de l’audit pour cette instance."
                continue
            }
        }
        else {
            $instanceLog = $instanceAudit
        }

        Invoke-InstanceSecurityAudit -InstanceAudit $instanceAudit `
                                     -InstanceLog $instanceLog `
                                     -LogDatabaseName $LogDatabaseName `
                                     -InstanceName $inst
    }

    Write-Host "=== Audit global terminé avec succès. ==="
    exit 0
}
catch {
    Write-Error "Erreur générale dans le script : $_"
    exit 2
}
