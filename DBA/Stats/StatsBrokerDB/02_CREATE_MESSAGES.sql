USE StatsBrokerDB;
GO

IF (EXISTS (SELECT 1 FROM sys.services WHERE name = N'mx.UpdateStatsService'))
DROP SERVICE  [mx.UpdateStatsService];
GO
IF OBJECT_ID('mx.UpdateStatsQueue') IS NOT NULL
DROP QUEUE mx.UpdateStatsQueue;
GO
IF OBJECT_ID('mx.usp_DequeueAndUpdateStats') IS NOT NULL
DROP PROCEDURE mx.usp_DequeueAndUpdateStats;
GO
IF OBJECT_ID('mx.UpdateStats') IS NOT NULL
DROP PROCEDURE mx.UpdateStats;
GO

IF EXISTS (SELECT 1 FROM sys.service_contracts WHERE name = N'mx//UpdateStatsContract') 
DROP CONTRACT [mx//UpdateStatsContract];
GO
IF EXISTS (SELECT 1 FROM sys.service_message_types WHERE name = N'mx//UpdateStatsRequest')
DROP MESSAGE TYPE [mx//UpdateStatsRequest];
GO


-- Message type, contract, queue, service
CREATE MESSAGE TYPE [mx//UpdateStatsRequest] VALIDATION = WELL_FORMED_XML;  -- we'll send XML payload
GO
CREATE CONTRACT [mx//UpdateStatsContract]
( [mx//UpdateStatsRequest] SENT BY INITIATOR );
GO

-- Activation proc (created later) will be mx.usp_DequeueAndUpdateStats
IF OBJECT_ID('mx.UpdateStatsQueue') IS NOT NULL
  DROP QUEUE mx.UpdateStatsQueue;
GO

CREATE QUEUE mx.UpdateStatsQueue;
GO

IF EXISTS (SELECT 1 FROM sys.services WHERE name = N'mx.UpdateStatsService')
  DROP SERVICE [mx.UpdateStatsService];
GO

CREATE SERVICE [mx.UpdateStatsService]
  ON QUEUE mx.UpdateStatsQueue
  ( [mx//UpdateStatsContract] );
GO
PRINT 'Created Service Broker objects in StatsBrokerDB.';