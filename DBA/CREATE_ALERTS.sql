USE [msdb]
GO
EXEC msdb.dbo.sp_add_alert @name=N'Alerts High Severity 19', 	@message_id=0, 		@severity=19, 		@enabled=1 
GO
EXEC msdb.dbo.sp_add_alert @name=N'Alerts High Severity 20', 	@message_id=0, 		@severity=20, 		@enabled=1 
GO
EXEC msdb.dbo.sp_add_alert @name=N'Alerts High Severity 21', 	@message_id=0, 		@severity=21, 		@enabled=1 
GO
EXEC msdb.dbo.sp_add_alert @name=N'Alerts High Severity 22', 	@message_id=0, 		@severity=22, 		@enabled=1 
GO
EXEC msdb.dbo.sp_add_alert @name=N'Alerts High Severity 23', 	@message_id=0, 		@severity=23, 		@enabled=1 
GO
EXEC msdb.dbo.sp_add_alert @name=N'Alerts High Severity 24', 	@message_id=0, 		@severity=24, 		@enabled=1 
GO
EXEC msdb.dbo.sp_add_alert @name=N'Alerts High Severity 25', 	@message_id=0, 		@severity=25, 		@enabled=1 
GO

SELECT GETDATE()
GO

SELECT @@SERVERNAME
GO

SELECT 
DB_NAME()
GO

USE MSDB
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Backup Success - 18264'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Backup Success - 18264' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Backup Success - 18264', @message_id = 18264, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Backup Failure - 18204'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Backup Failure - 18204' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Backup Failure - 18204', @message_id = 18204, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Backup Failure - 18210'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Backup Failure - 18210' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Backup Failure - 18210', @message_id = 18210, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Backup Failure - 3009'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Backup Failure - 3009' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Backup Failure - 3009', @message_id = 3009, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Backup Failure - 3017'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Backup Failure - 3017' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Backup Failure - 3017', @message_id = 3017, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Backup Failure - 3033'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Backup Failure - 3033' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Backup Failure - 3033', @message_id = 3033, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Backup Failure - 3201'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Backup Failure - 3201' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Backup Failure - 3201', @message_id = 3201, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Restore Success - 18267'))
---- Delete the 
alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Restore Success - 18267' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Restore Success - 18267', @message_id = 18267, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Restore Success - 18268'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Restore Success - 18268' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Restore Success - 18268', @message_id = 18268, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Restore Success - 18269'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Restore Success - 18269' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Restore Success - 18269', @message_id = 18269, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Restore Failure - 3142'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Restore Failure - 3142' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Restore Failure - 3142', @message_id = 3142, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Restore Failure - 3145'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Restore Failure - 3145' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Restore Failure - 3145', @message_id = 3145, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Restore Failure - 3441'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Restore Failure - 3441' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Restore Failure - 3441', @message_id = 3441, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Restore Failure - 3443'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Restore Failure - 3443' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Restore Failure - 3443', @message_id = 3443, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO

IF (EXISTS (SELECT name FROM 
msdb.dbo.sysalerts WHERE name = N'Restore Failure - 4301'))
---- Delete the alert with the same name.
EXECUTE msdb.dbo.sp_delete_alert @name = N'Restore Failure - 4301' 
BEGIN 
EXECUTE msdb.dbo.sp_add_alert @name = N'Restore Failure - 4301', @message_id = 4301, @severity = 0, @enabled = 1, 
@delay_between_responses = 60, @include_event_description_in = 5, @category_name 
= N'[Uncategorized]'
END
GO