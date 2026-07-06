param(
    [Parameter(Mandatory)] [string] $RepositoryConnectionString
)

. "$PSScriptRoot\ConfigHistory.Common.ps1"

$collectedAt = Get-ConfigHistoryUtcNow
$machine = $env:COMPUTERNAME

$cpuColumns = @('collected_at_utc','machine_name','socket_count','numa_node_count','cpu_name','cpu_manufacturer','max_clock_mhz','current_clock_mhz','physical_cores','logical_processors','hyperthreading_enabled','virtualization_firmware_enabled','l2_cache_kb','l3_cache_kb','collector_version')
$memoryColumns = @('collected_at_utc','machine_name','device_locator','bank_label','manufacturer','part_number','capacity_mb','speed_mhz','configured_clock_speed_mhz','form_factor','memory_type','serial_number','collector_version')
$numaColumns = @('collected_at_utc','machine_name','node_id','processor_count','memory_size_mb','collector_version')
$netColumns = @('collected_at_utc','machine_name','name','interface_description','status','mac_address','link_speed_bps','link_speed_text','driver_version','driver_date','pnp_device_id','collector_version')
$diskColumns = @('collected_at_utc','machine_name','disk_number','friendly_name','model','serial_number','bus_type','media_type','health_status','operational_status','partition_style','size_gb','logical_sector_size','physical_sector_size','spindle_speed','is_boot','is_system','collector_version')
$volColumns = @('collected_at_utc','machine_name','drive_letter','file_system_label','file_system','health_status','size_gb','size_remaining_gb','allocation_unit_size','path','collector_version')

$cpuDt = New-ConfigHistoryDataTable -TableName 'HardwareCpu' -Columns $cpuColumns
$memDt = New-ConfigHistoryDataTable -TableName 'HardwareMemoryModule' -Columns $memoryColumns
$numaDt = New-ConfigHistoryDataTable -TableName 'HardwareNumaNode' -Columns $numaColumns
$netDt = New-ConfigHistoryDataTable -TableName 'HardwareNetworkAdapter' -Columns $netColumns
$diskDt = New-ConfigHistoryDataTable -TableName 'HardwareDisk' -Columns $diskColumns
$volDt = New-ConfigHistoryDataTable -TableName 'HardwareVolume' -Columns $volColumns

$processors = @(Get-CimInstance Win32_Processor)
$numaNodes = @(Get-CimInstance Win32_NumaNode -ErrorAction SilentlyContinue)
$totalPhysicalCores = ($processors | Measure-Object -Property NumberOfCores -Sum).Sum
$totalLogicalProcessors = ($processors | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum
$firstCpu = $processors | Select-Object -First 1

Add-ConfigHistoryRow -Table $cpuDt -Values @{
    collected_at_utc = $collectedAt
    machine_name = $machine
    socket_count = $processors.Count
    numa_node_count = $numaNodes.Count
    cpu_name = $firstCpu.Name
    cpu_manufacturer = $firstCpu.Manufacturer
    max_clock_mhz = $firstCpu.MaxClockSpeed
    current_clock_mhz = $firstCpu.CurrentClockSpeed
    physical_cores = $totalPhysicalCores
    logical_processors = $totalLogicalProcessors
    hyperthreading_enabled = [bool]($totalLogicalProcessors -gt $totalPhysicalCores)
    virtualization_firmware_enabled = $firstCpu.VirtualizationFirmwareEnabled
    l2_cache_kb = $firstCpu.L2CacheSize
    l3_cache_kb = $firstCpu.L3CacheSize
    collector_version = '1.0'
}

Get-CimInstance Win32_PhysicalMemory | ForEach-Object {
    Add-ConfigHistoryRow -Table $memDt -Values @{
        collected_at_utc = $collectedAt
        machine_name = $machine
        device_locator = $_.DeviceLocator
        bank_label = $_.BankLabel
        manufacturer = $_.Manufacturer
        part_number = ($_.PartNumber -as [string]).Trim()
        capacity_mb = [int64]($_.Capacity / 1MB)
        speed_mhz = $_.Speed
        configured_clock_speed_mhz = $_.ConfiguredClockSpeed
        form_factor = $_.FormFactor
        memory_type = $_.MemoryType
        serial_number = $_.SerialNumber
        collector_version = '1.0'
    }
}

foreach ($node in $numaNodes) {
    $nodeMemory = Get-ConfigHistoryPropertyValue -InputObject $node -PropertyName 'AvailableMemory'
    $nodeMemoryMb = if ($null -ne $nodeMemory) { [int64]($nodeMemory / 1MB) } else { $null }

    Add-ConfigHistoryRow -Table $numaDt -Values @{
        collected_at_utc = $collectedAt
        machine_name = $machine
        node_id = (Get-ConfigHistoryPropertyValue -InputObject $node -PropertyName 'NodeNumber')
        processor_count = (Get-ConfigHistoryPropertyValue -InputObject $node -PropertyName 'NumberOfProcessors')
        memory_size_mb = $nodeMemoryMb
        collector_version = '1.0'
    }
}

$netAdapters = @(Get-NetAdapter -Physical -ErrorAction SilentlyContinue)
foreach ($adapter in $netAdapters) {
    $linkSpeedText = Get-ConfigHistoryPropertyValue -InputObject $adapter -PropertyName 'LinkSpeed'
    $linkSpeedBps = Get-ConfigHistoryPropertyValue -InputObject $adapter -PropertyName 'ReceiveLinkSpeed'
    Add-ConfigHistoryRow -Table $netDt -Values @{
        collected_at_utc = $collectedAt
        machine_name = $machine
        name = (Get-ConfigHistoryPropertyValue -InputObject $adapter -PropertyName 'Name')
        interface_description = (Get-ConfigHistoryPropertyValue -InputObject $adapter -PropertyName 'InterfaceDescription')
        status = (Get-ConfigHistoryPropertyValue -InputObject $adapter -PropertyName 'Status')
        mac_address = (Get-ConfigHistoryPropertyValue -InputObject $adapter -PropertyName 'MacAddress')
        link_speed_bps = $linkSpeedBps
        link_speed_text = if ($null -ne $linkSpeedText) { $linkSpeedText.ToString() } else { $null }
        driver_version = (Get-ConfigHistoryPropertyValue -InputObject $adapter -PropertyName 'DriverVersion')
        driver_date = (Get-ConfigHistoryPropertyValue -InputObject $adapter -PropertyName 'DriverDate')
        pnp_device_id = (Get-ConfigHistoryPropertyValue -InputObject $adapter -PropertyName 'PnPDeviceID')
        collector_version = '1.0'
    }
}

Get-Disk | ForEach-Object {
    $mediaTypeVal = Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'MediaType'
    $busTypeVal = Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'BusType'
    $healthVal = Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'HealthStatus'
    $partStyleVal = Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'PartitionStyle'
    $opStatus = Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'OperationalStatus'
    Add-ConfigHistoryRow -Table $diskDt -Values @{
        collected_at_utc = $collectedAt
        machine_name = $machine
        disk_number = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'Number')
        friendly_name = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'FriendlyName')
        model = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'Model')
        serial_number = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'SerialNumber')
        bus_type = if ($null -ne $busTypeVal) { $busTypeVal.ToString() } else { $null }
        media_type = if ($null -ne $mediaTypeVal) { $mediaTypeVal.ToString() } else { $null }
        health_status = if ($null -ne $healthVal) { $healthVal.ToString() } else { $null }
        operational_status = if ($null -ne $opStatus) { $opStatus -join ',' } else { $null }
        partition_style = if ($null -ne $partStyleVal) { $partStyleVal.ToString() } else { $null }
        size_gb = ConvertTo-DbDecimalGb (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'Size')
        logical_sector_size = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'LogicalSectorSize')
        physical_sector_size = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'PhysicalSectorSize')
        spindle_speed = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'SpindleSpeed')
        is_boot = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'IsBoot')
        is_system = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'IsSystem')
        collector_version = '1.0'
    }
}

Get-Volume | Where-Object { $null -ne (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'FileSystem') } | ForEach-Object {
    $volHealth = Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'HealthStatus'
    Add-ConfigHistoryRow -Table $volDt -Values @{
        collected_at_utc = $collectedAt
        machine_name = $machine
        drive_letter = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'DriveLetter')
        file_system_label = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'FileSystemLabel')
        file_system = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'FileSystem')
        health_status = if ($null -ne $volHealth) { $volHealth.ToString() } else { $null }
        size_gb = ConvertTo-DbDecimalGb (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'Size')
        size_remaining_gb = ConvertTo-DbDecimalGb (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'SizeRemaining')
        allocation_unit_size = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'AllocationUnitSize')
        path = (Get-ConfigHistoryPropertyValue -InputObject $_ -PropertyName 'Path')
        collector_version = '1.0'
    }
}

Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $cpuDt -DestinationTable 'confighistory.HardwareCpu'
Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $memDt -DestinationTable 'confighistory.HardwareMemoryModule'
Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $numaDt -DestinationTable 'confighistory.HardwareNumaNode'
Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $netDt -DestinationTable 'confighistory.HardwareNetworkAdapter'
Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $diskDt -DestinationTable 'confighistory.HardwareDisk'
Write-ConfigHistoryDataTable -ConnectionString $RepositoryConnectionString -DataTable $volDt -DestinationTable 'confighistory.HardwareVolume'
