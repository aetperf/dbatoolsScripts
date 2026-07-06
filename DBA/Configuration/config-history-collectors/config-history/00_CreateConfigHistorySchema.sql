/*
    Config history schema for Windows Server / SQL Server performance configuration inventory.
    Target: SQL Server 2019+

    Usage:
      sqlcmd -S <repository_instance> -d <repository_database> -E -i 00_CreateConfigHistorySchema.sql
*/

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'confighistory')
    EXEC(N'CREATE SCHEMA confighistory AUTHORIZATION dbo;');
GO

CREATE OR ALTER PROCEDURE confighistory.usp_PurgeOldCollections
    @RetentionDays int = 365
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @cutoff datetime2(3) = DATEADD(DAY, -@RetentionDays, SYSUTCDATETIME());

    DELETE FROM confighistory.HardwareCpu WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.HardwareMemoryModule WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.HardwareNumaNode WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.HardwareNetworkAdapter WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.HardwareDisk WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.HardwareVolume WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.OperatingSystemInfo WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.OperatingSystemPrivilege WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.WindowsDefenderStatus WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.WindowsDefenderExclusion WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.MssqlInstanceConfiguration WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.MssqlResourceGovernor WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.MssqlTempdbFile WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.MssqlDatabase WHERE collected_at_utc < @cutoff;
    DELETE FROM confighistory.MssqlDatabaseFile WHERE collected_at_utc < @cutoff;
END;
GO

IF OBJECT_ID(N'confighistory.HardwareCpu', N'U') IS NULL
CREATE TABLE confighistory.HardwareCpu
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    socket_count int NULL,
    numa_node_count int NULL,
    cpu_name nvarchar(256) NULL,
    cpu_manufacturer nvarchar(128) NULL,
    max_clock_mhz int NULL,
    current_clock_mhz int NULL,
    physical_cores int NULL,
    logical_processors int NULL,
    hyperthreading_enabled bit NULL,
    virtualization_firmware_enabled bit NULL,
    l2_cache_kb int NULL,
    l3_cache_kb int NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_HardwareCpu_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.HardwareMemoryModule', N'U') IS NULL
CREATE TABLE confighistory.HardwareMemoryModule
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    device_locator nvarchar(128) NULL,
    bank_label nvarchar(128) NULL,
    manufacturer nvarchar(128) NULL,
    part_number nvarchar(128) NULL,
    capacity_mb bigint NULL,
    speed_mhz int NULL,
    configured_clock_speed_mhz int NULL,
    form_factor int NULL,
    memory_type int NULL,
    serial_number nvarchar(128) NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_HardwareMemory_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.HardwareNumaNode', N'U') IS NULL
CREATE TABLE confighistory.HardwareNumaNode
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    node_id int NULL,
    processor_count int NULL,
    memory_size_mb bigint NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_HardwareNuma_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.HardwareNetworkAdapter', N'U') IS NULL
CREATE TABLE confighistory.HardwareNetworkAdapter
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    name nvarchar(256) NULL,
    interface_description nvarchar(512) NULL,
    status nvarchar(64) NULL,
    mac_address nvarchar(64) NULL,
    link_speed_bps bigint NULL,
    link_speed_text nvarchar(64) NULL,
    driver_version nvarchar(128) NULL,
    driver_date datetime2(0) NULL,
    pnp_device_id nvarchar(512) NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_HardwareNetwork_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.HardwareDisk', N'U') IS NULL
CREATE TABLE confighistory.HardwareDisk
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    disk_number int NULL,
    friendly_name nvarchar(256) NULL,
    model nvarchar(256) NULL,
    serial_number nvarchar(256) NULL,
    bus_type nvarchar(64) NULL,
    media_type nvarchar(64) NULL,
    health_status nvarchar(64) NULL,
    operational_status nvarchar(256) NULL,
    partition_style nvarchar(64) NULL,
    size_gb decimal(19,2) NULL,
    logical_sector_size int NULL,
    physical_sector_size int NULL,
    spindle_speed int NULL,
    is_boot bit NULL,
    is_system bit NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_HardwareDisk_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.HardwareVolume', N'U') IS NULL
CREATE TABLE confighistory.HardwareVolume
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    drive_letter nvarchar(8) NULL,
    file_system_label nvarchar(256) NULL,
    file_system nvarchar(64) NULL,
    health_status nvarchar(64) NULL,
    size_gb decimal(19,2) NULL,
    size_remaining_gb decimal(19,2) NULL,
    allocation_unit_size int NULL,
    path nvarchar(1024) NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_HardwareVolume_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.OperatingSystemInfo', N'U') IS NULL
CREATE TABLE confighistory.OperatingSystemInfo
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    caption nvarchar(256) NULL,
    version nvarchar(64) NULL,
    build_number nvarchar(64) NULL,
    ubr int NULL,
    installation_type nvarchar(128) NULL,
    edition_id nvarchar(128) NULL,
    product_name nvarchar(256) NULL,
    display_version nvarchar(64) NULL,
    release_id nvarchar(64) NULL,
    os_architecture nvarchar(64) NULL,
    install_date datetime2(0) NULL,
    last_boot_up_time datetime2(0) NULL,
    power_plan_name nvarchar(256) NULL,
    power_plan_guid nvarchar(128) NULL,
    time_zone nvarchar(128) NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_OSInfo_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.OperatingSystemPrivilege', N'U') IS NULL
CREATE TABLE confighistory.OperatingSystemPrivilege
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    privilege_name nvarchar(128) NOT NULL,
    privilege_label nvarchar(256) NULL,
    assigned_to_raw nvarchar(max) NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_OSPrivilege_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.WindowsDefenderStatus', N'U') IS NULL
CREATE TABLE confighistory.WindowsDefenderStatus
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    am_service_enabled bit NULL,
    antivirus_enabled bit NULL,
    antispyware_enabled bit NULL,
    behavior_monitor_enabled bit NULL,
    ioav_protection_enabled bit NULL,
    nise_enabled bit NULL,
    on_access_protection_enabled bit NULL,
    real_time_protection_enabled bit NULL,
    tamper_protection_enabled bit NULL,
    antivirus_signature_version nvarchar(128) NULL,
    antispyware_signature_version nvarchar(128) NULL,
    nise_signature_version nvarchar(128) NULL,
    quick_scan_age int NULL,
    full_scan_age int NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_DefenderStatus_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.WindowsDefenderExclusion', N'U') IS NULL
CREATE TABLE confighistory.WindowsDefenderExclusion
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    exclusion_type nvarchar(64) NOT NULL,
    exclusion_value nvarchar(2048) NOT NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_DefenderExclusion_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.MssqlInstanceConfiguration', N'U') IS NULL
CREATE TABLE confighistory.MssqlInstanceConfiguration
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    sql_instance_name nvarchar(256) NOT NULL,
    sql_server_name nvarchar(256) NULL,
    sql_machine_name nvarchar(256) NULL,
    product_version nvarchar(128) NULL,
    product_level nvarchar(128) NULL,
    edition nvarchar(256) NULL,
    engine_edition int NULL,
    configuration_id int NOT NULL,
    name nvarchar(256) NOT NULL,
    value sql_variant NULL,
    value_in_use sql_variant NULL,
    minimum sql_variant NULL,
    maximum sql_variant NULL,
    is_dynamic bit NULL,
    is_advanced bit NULL,
    description nvarchar(1024) NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_MssqlConfig_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.MssqlResourceGovernor', N'U') IS NULL
CREATE TABLE confighistory.MssqlResourceGovernor
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    sql_instance_name nvarchar(256) NOT NULL,
    resource_pool_name sysname NULL,
    workload_group_name sysname NULL,
    pool_max_cpu_percent int NULL,
    pool_cap_cpu_percent int NULL,
    pool_min_cpu_percent int NULL,
    pool_max_memory_percent int NULL,
    group_importance nvarchar(32) NULL,
    group_request_max_memory_grant_percent decimal(9,4) NULL,
    group_request_max_cpu_time_sec int NULL,
    group_max_dop int NULL,
    group_group_max_requests int NULL,
    is_enabled bit NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_MssqlRG_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.MssqlTempdbFile', N'U') IS NULL
CREATE TABLE confighistory.MssqlTempdbFile
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    sql_instance_name nvarchar(256) NOT NULL,
    file_id int NOT NULL,
    type_desc nvarchar(64) NULL,
    logical_name sysname NULL,
    physical_name nvarchar(520) NULL,
    size_mb decimal(19,2) NULL,
    max_size_mb decimal(19,2) NULL,
    growth_desc nvarchar(128) NULL,
    is_percent_growth bit NULL,
    state_desc nvarchar(64) NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_MssqlTempdb_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.MssqlDatabase', N'U') IS NULL
CREATE TABLE confighistory.MssqlDatabase
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    sql_instance_name nvarchar(256) NOT NULL,
    database_id int NOT NULL,
    database_name sysname NOT NULL,
    create_date datetime2(0) NULL,
    compatibility_level tinyint NULL,
    collation_name sysname NULL,
    user_access_desc nvarchar(64) NULL,
    state_desc nvarchar(64) NULL,
    recovery_model_desc nvarchar(64) NULL,
    page_verify_option_desc nvarchar(64) NULL,
    is_read_committed_snapshot_on bit NULL,
    snapshot_isolation_state_desc nvarchar(64) NULL,
    is_auto_create_stats_on bit NULL,
    is_auto_update_stats_on bit NULL,
    is_auto_update_stats_async_on bit NULL,
    is_query_store_on bit NULL,
    target_recovery_time_in_seconds int NULL,
    delayed_durability_desc nvarchar(64) NULL,
    is_encrypted bit NULL,
    owner_sid varbinary(85) NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_MssqlDb_collector_version DEFAULT(N'1.0')
);
GO

IF OBJECT_ID(N'confighistory.MssqlDatabaseFile', N'U') IS NULL
CREATE TABLE confighistory.MssqlDatabaseFile
(
    collected_at_utc datetime2(3) NOT NULL,
    machine_name sysname NOT NULL,
    sql_instance_name nvarchar(256) NOT NULL,
    database_id int NOT NULL,
    database_name sysname NULL,
    file_id int NOT NULL,
    type_desc nvarchar(64) NULL,
    logical_name sysname NULL,
    physical_name nvarchar(520) NULL,
    state_desc nvarchar(64) NULL,
    size_mb decimal(19,2) NULL,
    max_size_mb decimal(19,2) NULL,
    growth_desc nvarchar(128) NULL,
    is_percent_growth bit NULL,
    collector_version nvarchar(32) NOT NULL CONSTRAINT DF_MssqlDbFile_collector_version DEFAULT(N'1.0')
);
GO

CREATE INDEX IX_HardwareCpu_MachineDate ON confighistory.HardwareCpu(machine_name, collected_at_utc DESC);
CREATE INDEX IX_OSInfo_MachineDate ON confighistory.OperatingSystemInfo(machine_name, collected_at_utc DESC);
CREATE INDEX IX_MssqlConfig_InstanceDate ON confighistory.MssqlInstanceConfiguration(sql_instance_name, collected_at_utc DESC);
CREATE INDEX IX_MssqlDb_InstanceDate ON confighistory.MssqlDatabase(sql_instance_name, database_name, collected_at_utc DESC);
GO
