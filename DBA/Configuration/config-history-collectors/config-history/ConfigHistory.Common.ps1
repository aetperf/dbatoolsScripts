Set-StrictMode -Version 3.0
$ErrorActionPreference = 'Stop'

function New-ConfigHistoryDataTable {
    param(
        [Parameter(Mandatory)] [string] $TableName,
        [Parameter(Mandatory)] [string[]] $Columns
    )

    $dt = [System.Data.DataTable]::new($TableName)
    foreach ($c in $Columns) {
        [void]$dt.Columns.Add($c, [object])
    }
    return , $dt
}

function Add-ConfigHistoryRow {
    param(
        [Parameter(Mandatory)] [System.Data.DataTable] $Table,
        [Parameter(Mandatory)] [hashtable] $Values
    )

    $row = $Table.NewRow()
    foreach ($col in $Table.Columns) {
        $name = $col.ColumnName
        if ($Values.ContainsKey($name) -and $null -ne $Values[$name]) {
            $val = $Values[$name]
            # Unwrap PSObject to base .NET type for SqlBulkCopy compatibility
            if ($val -is [psobject]) { $val = $val.psobject.BaseObject }
            # Convert enums and other non-primitive types to string
            if ($null -ne $val -and $val -isnot [string] -and $val -isnot [DBNull]) {
                $vtype = $val.GetType()
                if (-not ($vtype.IsPrimitive -or $vtype -eq [decimal] -or $vtype -eq [datetime] -or $vtype -eq [guid] -or $vtype -eq [byte[]])) {
                    $val = $val.ToString()
                }
            }
            $row[$name] = $val
        }
        else {
            $row[$name] = [DBNull]::Value
        }
    }
    [void]$Table.Rows.Add($row)
}

function Get-SqlClientConnection {
    param([Parameter(Mandatory)] [string] $ConnectionString)

    try {
        Add-Type -AssemblyName Microsoft.Data.SqlClient -ErrorAction Stop
        $conn = [Microsoft.Data.SqlClient.SqlConnection]::new($ConnectionString)
        return $conn
    }
    catch {
        Add-Type -AssemblyName System.Data
        $conn = [System.Data.SqlClient.SqlConnection]::new($ConnectionString)
        return $conn
    }
}

function Invoke-ConfigHistorySqlQuery {
    param(
        [Parameter(Mandatory)] [string] $ConnectionString,
        [Parameter(Mandatory)] [string] $Query,
        [int] $CommandTimeoutSeconds = 120
    )

    $conn = Get-SqlClientConnection -ConnectionString $ConnectionString
    try {
        $conn.Open()
        $cmd = $conn.CreateCommand()
        $cmd.CommandText = $Query
        $cmd.CommandTimeout = $CommandTimeoutSeconds
        $reader = $cmd.ExecuteReader()
        $dt = [System.Data.DataTable]::new()
        $dt.Load($reader)
        return , $dt
    }
    finally {
        $conn.Dispose()
    }
}

function Write-ConfigHistoryDataTable {
    param(
        [Parameter(Mandatory)] [string] $ConnectionString,
        [Parameter(Mandatory)] [System.Data.DataTable] $DataTable,
        [Parameter(Mandatory)] [string] $DestinationTable,
        [int] $BatchSize = 5000,
        [int] $BulkCopyTimeoutSeconds = 120
    )

    if ($DataTable.Rows.Count -eq 0) {
        return 0
    }

    $conn = Get-SqlClientConnection -ConnectionString $ConnectionString
    $bulk = $null
    try {
        $conn.Open()
        $bulkCopyType = $null
        try { $bulkCopyType = [Microsoft.Data.SqlClient.SqlBulkCopy] } catch { $bulkCopyType = [System.Data.SqlClient.SqlBulkCopy] }
        $bulk = $bulkCopyType::new($conn)
        $bulk.DestinationTableName = $DestinationTable
        $bulk.BatchSize = $BatchSize
        $bulk.BulkCopyTimeout = $BulkCopyTimeoutSeconds

        foreach ($col in $DataTable.Columns) {
            [void]$bulk.ColumnMappings.Add($col.ColumnName, $col.ColumnName)
        }

        $bulk.WriteToServer($DataTable)
        return $DataTable.Rows.Count
    }
    finally {
        if ($null -ne $bulk) { $bulk.Close() }
        $conn.Dispose()
    }
}

function ConvertTo-DbBool {
    param($Value)
    if ($null -eq $Value) { return $null }
    return [bool]$Value
}

function ConvertTo-DbDecimalGb {
    param($Bytes)
    if ($null -eq $Bytes) { return $null }
    return [math]::Round(([decimal]$Bytes / 1GB), 2)
}

function Get-ConfigHistoryUtcNow {
    return [datetime]::UtcNow.ToString('yyyy-MM-dd HH:mm:ss.fff')
}

function Get-ConfigHistoryPropertyValue {
    param(
        [Parameter(Mandatory)] $InputObject,
        [Parameter(Mandatory)] [string] $PropertyName
    )

    $prop = $InputObject.PSObject.Properties[$PropertyName]
    if ($null -eq $prop) { return $null }
    return $prop.Value
}
