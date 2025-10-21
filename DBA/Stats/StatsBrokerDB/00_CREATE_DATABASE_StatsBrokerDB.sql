USE master;
GO

IF DB_ID(N'StatsBrokerDB') IS NULL
BEGIN
  PRINT 'Creating StatsBrokerDB...';
  EXEC ('CREATE DATABASE StatsBrokerDB');
END
GO

-- Enable Service Broker (forces disconnects to ensure a clean start)
ALTER DATABASE StatsBrokerDB SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;
ALTER DATABASE StatsBrokerDB SET RECOVERY SIMPLE;
ALTER DATABASE StatsBrokerDB SET TRUSTWORTHY ON;  -- simplifies EXECUTE AS OWNER path
GO
