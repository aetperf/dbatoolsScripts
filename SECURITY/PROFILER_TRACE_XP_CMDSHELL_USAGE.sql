/****************************************************/
/* Created by: SQL Server 2019 Profiler          */
/* Date: 04/01/2022  04:18:21 PM         */
/****************************************************/


-- Create a Queue
declare @rc int
declare @TraceID int
declare @maxfilesize bigint
set @maxfilesize = 5 

-- Please replace the text InsertFileNameHere, with an appropriate
-- filename prefixed by a path, e.g., c:\MyFolder\MyTrace. The .trc extension
-- will be appended to the filename automatically. If you are writing from
-- remote server to local drive, please use UNC path and make sure server has
-- write access to your network share

exec @rc = sp_trace_create @TraceID output, 0, N'InsertFileNameHere', @maxfilesize, NULL 
if (@rc != 0) goto error

-- Client side File and Table cannot be scripted

-- Set the events
declare @on bit
set @on = 1
exec sp_trace_setevent @TraceID, 11, 1, @on
exec sp_trace_setevent @TraceID, 11, 9, @on
exec sp_trace_setevent @TraceID, 11, 2, @on
exec sp_trace_setevent @TraceID, 11, 10, @on
exec sp_trace_setevent @TraceID, 11, 3, @on
exec sp_trace_setevent @TraceID, 11, 4, @on
exec sp_trace_setevent @TraceID, 11, 6, @on
exec sp_trace_setevent @TraceID, 11, 7, @on
exec sp_trace_setevent @TraceID, 11, 8, @on
exec sp_trace_setevent @TraceID, 11, 11, @on
exec sp_trace_setevent @TraceID, 11, 12, @on
exec sp_trace_setevent @TraceID, 11, 14, @on
exec sp_trace_setevent @TraceID, 11, 26, @on
exec sp_trace_setevent @TraceID, 11, 34, @on
exec sp_trace_setevent @TraceID, 11, 35, @on
exec sp_trace_setevent @TraceID, 11, 41, @on
exec sp_trace_setevent @TraceID, 11, 49, @on
exec sp_trace_setevent @TraceID, 11, 50, @on
exec sp_trace_setevent @TraceID, 11, 51, @on
exec sp_trace_setevent @TraceID, 11, 60, @on
exec sp_trace_setevent @TraceID, 11, 64, @on
exec sp_trace_setevent @TraceID, 44, 1, @on
exec sp_trace_setevent @TraceID, 44, 9, @on
exec sp_trace_setevent @TraceID, 44, 3, @on
exec sp_trace_setevent @TraceID, 44, 4, @on
exec sp_trace_setevent @TraceID, 44, 5, @on
exec sp_trace_setevent @TraceID, 44, 6, @on
exec sp_trace_setevent @TraceID, 44, 7, @on
exec sp_trace_setevent @TraceID, 44, 8, @on
exec sp_trace_setevent @TraceID, 44, 10, @on
exec sp_trace_setevent @TraceID, 44, 11, @on
exec sp_trace_setevent @TraceID, 44, 12, @on
exec sp_trace_setevent @TraceID, 44, 14, @on
exec sp_trace_setevent @TraceID, 44, 22, @on
exec sp_trace_setevent @TraceID, 44, 26, @on
exec sp_trace_setevent @TraceID, 44, 28, @on
exec sp_trace_setevent @TraceID, 44, 29, @on
exec sp_trace_setevent @TraceID, 44, 30, @on
exec sp_trace_setevent @TraceID, 44, 34, @on
exec sp_trace_setevent @TraceID, 44, 35, @on
exec sp_trace_setevent @TraceID, 44, 41, @on
exec sp_trace_setevent @TraceID, 44, 49, @on
exec sp_trace_setevent @TraceID, 44, 50, @on
exec sp_trace_setevent @TraceID, 44, 51, @on
exec sp_trace_setevent @TraceID, 44, 55, @on
exec sp_trace_setevent @TraceID, 44, 60, @on
exec sp_trace_setevent @TraceID, 44, 61, @on
exec sp_trace_setevent @TraceID, 44, 62, @on
exec sp_trace_setevent @TraceID, 44, 64, @on
exec sp_trace_setevent @TraceID, 13, 1, @on
exec sp_trace_setevent @TraceID, 13, 9, @on
exec sp_trace_setevent @TraceID, 13, 3, @on
exec sp_trace_setevent @TraceID, 13, 11, @on
exec sp_trace_setevent @TraceID, 13, 4, @on
exec sp_trace_setevent @TraceID, 13, 6, @on
exec sp_trace_setevent @TraceID, 13, 7, @on
exec sp_trace_setevent @TraceID, 13, 8, @on
exec sp_trace_setevent @TraceID, 13, 10, @on
exec sp_trace_setevent @TraceID, 13, 12, @on
exec sp_trace_setevent @TraceID, 13, 14, @on
exec sp_trace_setevent @TraceID, 13, 26, @on
exec sp_trace_setevent @TraceID, 13, 35, @on
exec sp_trace_setevent @TraceID, 13, 41, @on
exec sp_trace_setevent @TraceID, 13, 49, @on
exec sp_trace_setevent @TraceID, 13, 50, @on
exec sp_trace_setevent @TraceID, 13, 51, @on
exec sp_trace_setevent @TraceID, 13, 60, @on
exec sp_trace_setevent @TraceID, 13, 64, @on


-- Set the Filters
declare @intfilter int
declare @bigintfilter bigint

exec sp_trace_setfilter @TraceID, 1, 0, 6, N'%xp_cmdshell%'
exec sp_trace_setfilter @TraceID, 1, 0, 1, NULL
exec sp_trace_setfilter @TraceID, 11, 0, 7, N'GRA2000\S_sqlmonitor'
-- Set the trace status to start
exec sp_trace_setstatus @TraceID, 1

-- display trace id for future references
select TraceID=@TraceID
goto finish

error: 
select ErrorCode=@rc

finish: 
go
