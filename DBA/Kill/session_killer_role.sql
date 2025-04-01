CREATE ROLE session_killer_role;

GRANT EXECUTE ON dbo.sp_kill TO session_killer_role;
GO