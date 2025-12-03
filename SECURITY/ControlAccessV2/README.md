# Security Permissions Management Scripts

This repository contains a set of SQL and PowerShell scripts to help you audit, align, and manage SQL Server database security permissions across multiple environments.

## Overview

The solution is composed of:
- **PowerShell script**: Audits SQL Server logins and their Active Directory group members, and stores the results in dedicated tables.
- **SQL scripts**: Define tables, views, and procedures to compare, stage, and align security permissions.

## File Description
- `GetSecurityDBState.ps1`: Powershell script that audits logins, AD group members, and database role membership, creating the required history tables if they do not exist.
- `PermissionsExpected.sql`: Creates the system-versioned table that stores expected permissions over time.
- `PermissionsExpected_Staging.sql`: Creates the staging table used to load proposed changes before synchronization.
- `VW_GetLoginsSecurity.sql`: View that exposes the latest login security audit snapshot.
- `VW_GetLoginsPermissions.sql`: View that exposes the latest login-to-role mapping snapshot.
- `VW_CheckSecurityComparison.sql`: View comparing expected permissions with the current state, generating alignment commands.
- `AlignSecurityPermissions.sql`: Stored procedure that filters and executes the alignment statements, logging every action.

## Usage Instructions

### 1. Audit Logins and Permissions
- Use the provided PowerShell script (`GetSecurityDBState.ps1`) to audit SQL Server logins and their AD group members.
- Parameters:
    - `ServerInstance` (mandatory): SQL Server instance to audit, for example `SRVSQL` or `SRVSQL\INSTANCE`.
    - `LogDatabaseName` (optional, default `DBATOOLS`): Database that will host the audit tables.
- The script automatically creates the `security.LoginsSecurityHistory` and `security.LoginsPermissionsHistory` tables if they do not already exist, then populates them with audit data.

**Recommendation:**
- Create a SQL Server Agent Job to schedule and execute the PowerShell script regularly (e.g., daily or weekly) to keep your audit data up to date.

### 2. Populate the Staging Table
- The `security.PermissionsExpected_Staging` table is used to stage the expected permissions before updating the main table.
- You can populate this table manually or by importing data from a CSV file, depending on your workflow.

**Recommendation:**
- Create a SQL Server Agent Job to automate the import of your expected permissions into the staging table.

### 3. Update the Temporal Table
- To synchronize your main expected permissions table (`security.PermissionsExpected`) with the staging table, use the following SQL statement:

```sql
MERGE security.PermissionsExpected AS target
USING security.PermissionsExpected_Staging AS source
    ON target.DatabaseName = source.DatabaseName
   AND target.LoginName = source.LoginName
   AND target.RoleName = source.RoleName
WHEN NOT MATCHED BY TARGET THEN
    INSERT (DatabaseName, LoginName, RoleName, LastModifiedBy)
    VALUES (source.DatabaseName, source.LoginName, source.RoleName, source.MetaUser)
WHEN MATCHED AND target.LastModifiedBy <> source.MetaUser THEN
    UPDATE SET target.LastModifiedBy = source.MetaUser
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
```

**Recommendation:**
- Create a SQL Server Agent Job to execute this MERGE statement after the staging table is populated.

### 4. Align Actual Permissions
- Use the stored procedure `[security].[AlignSecurityPermissions]` to compare the expected permissions against the current state and optionally execute alignment commands.
- Key parameters:
    - `@IgnoreRoles`: Comma-separated list of SQL role patterns to skip during alignment (supports wildcards).
    - `@IncludeLogins`: Comma-separated list of login patterns to include; if NULL, all logins are considered.
    - `@ExcludeLogins`: Comma-separated list of login patterns to exclude from processing.
    - `@Execute`: `'Y'` to apply the generated SQL statements, `'N'` to only log the actions.
    - `@DatabaseGroupName`: Comma-separated list of database group patterns used to limit the scope of the alignment.
- The procedure writes every planned action into `[security].[AlignSecurityPermissionsLogs]` (created automatically on first run) and, when `@Execute = 'Y'`, runs each statement via dynamic SQL. It also generates undo statements for traceability.
- Example invocation:

```sql
EXEC security.AlignSecurityPermissions
        @IgnoreRoles = 'db_datareader,db_datawriter',
        @IncludeLogins = NULL,
        @ExcludeLogins = 'svc_%',
        @Execute = 'N',
        @DatabaseGroupName = 'Prod_%';
```

## Summary of Jobs to Create
1. **PowerShell Audit Job**: Runs the PowerShell script to audit logins and permissions.
2. **Staging Table Load Job**: Loads data into `PermissionsExpected_Staging` (manually or via CSV import).
3. **Permissions Update Job**: Runs the MERGE SQL to update `PermissionsExpected` from the staging table.






