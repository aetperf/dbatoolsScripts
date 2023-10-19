SELECT servicename,service_account
FROM sys.dm_server_services
WHERE UPPER(service_account) LIKE '%S_EXPLOITATION%'