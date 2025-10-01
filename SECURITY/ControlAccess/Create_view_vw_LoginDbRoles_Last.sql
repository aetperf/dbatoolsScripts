
CREATE OR ALTER VIEW [security].[vw_LoginDbRoles_Last]
AS
WITH ranked AS
(
    SELECT
        l.*,
        ROW_NUMBER() OVER
        (
            PARTITION BY
                ISNULL(l.login_name, N'#NOLOGIN#'),
                l.[user_name],
                l.[database],
                ISNULL(l.[role_name], N'#NOROLE#')
            ORDER BY l.controldate DESC
        ) AS rn
    FROM dbo.LoginDbRoles AS l
)
SELECT
    controldate, login_name, login_type, is_disabled, default_database,
    [database], [user_name], user_type, [role_name]
FROM ranked
WHERE rn = 1;
GO


