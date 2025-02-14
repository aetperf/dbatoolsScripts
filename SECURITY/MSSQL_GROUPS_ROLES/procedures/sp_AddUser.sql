CREATE OR ALTER PROCEDURE [security].sp_AddUser(@LoginName NVARCHAR(100))
AS
BEGIN

-- Check the login name already exists
IF NOT EXISTS (
    SELECT 1 FROM sys.server_principals WHERE name = @LoginName
)
BEGIN
    RAISERROR('The login %s does not exist.', 16, 1, @LoginName);
    RETURN;
END

-- Check if the user already exists
IF EXISTS (
    SELECT 1 FROM security.Users WHERE UserLogin = @LoginName
)
BEGIN
    RAISERROR('The user %s already exists.', 10, 1, @LoginName);
    RETURN;
END
ELSE
BEGIN
    INSERT INTO security.Users (UserLogin) VALUES (@LoginName);
END

END;