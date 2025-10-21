USE StatsBrokerDB;
GO

ALTER QUEUE mx.UpdateStatsQueue WITH ACTIVATION
(
  STATUS = ON,
  PROCEDURE_NAME = mx.usp_DequeueAndUpdateStats,
  MAX_QUEUE_READERS = 8 ,           -- <<<<<<<<<<<< parallel workers
  EXECUTE AS OWNER
);
GO
PRINT 'Altered mx.UpdateStatsQueue to enable activation with 8 readers.';