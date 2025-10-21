USE StatsBrokerDB;
GO

IF EXISTS (SELECT 1 FROM sys.services WHERE name = N'mx.BrokerActivationQueue')
  DROP SERVICE [mx.BrokerActivationQueue];
GO

IF OBJECT_ID('mx.BrokerActivationQueue') IS NOT NULL
  DROP QUEUE mx.BrokerActivationQueue;
GO

IF EXISTS (SELECT 1 FROM sys.service_contracts WHERE name = N'mx//WakeContract') 
DROP CONTRACT [mx//WakeContract];
GO
IF EXISTS (SELECT 1 FROM sys.service_message_types WHERE name = N'mx//Wake')
DROP MESSAGE TYPE [mx//Wake];
GO

CREATE MESSAGE TYPE [mx//Wake] VALIDATION = NONE;
GO

CREATE CONTRACT [mx//WakeContract] ([mx//Wake] SENT BY INITIATOR);
GO

CREATE QUEUE [mx].[BrokerActivationQueue] WITH STATUS = ON , RETENTION = OFF , ACTIVATION (  STATUS = ON , PROCEDURE_NAME = [mx].[usp_BrokerActivationWorker] , MAX_QUEUE_READERS = 8 , EXECUTE AS OWNER  ), POISON_MESSAGE_HANDLING (STATUS = ON)  ON [PRIMARY] ;
GO

CREATE SERVICE [mx.BrokerActivationService]  ON QUEUE [mx].[BrokerActivationQueue] ([mx//WakeContract]);
GO

GO
PRINT 'Created Service Broker objects in StatsBrokerDB.';