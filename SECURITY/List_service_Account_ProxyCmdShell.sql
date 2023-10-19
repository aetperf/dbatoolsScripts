-- List Dangerous credentials 
SELECT c.credential_id,c.name,c.credential_identity , create_date
FROM sys.credentials c 
WHERE c.name = N'##xp_cmdshell_proxy_account##' or UPPER(c.credential_identity) LIKE N'%S_EXPLOITATION%'