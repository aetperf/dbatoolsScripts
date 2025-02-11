CREATE OR ALTER PROCEDURE [security].sp_AddMemberToGroup(@LoginName NVARCHAR(255), @GroupName NVARCHAR(100))
AS
BEGIN

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @UserID INT;
DECLARE @GroupID INT;

-- Get the UserID
SELECT @UserID = UserID FROM security.Users WHERE UserLogin = @LoginName;
-- Get the GroupID
SELECT @GroupID = GroupID FROM security.Groups WHERE GroupName = @GroupName;

-- Check the login name already exists
IF NOT EXISTS (
    SELECT 1 FROM sys.server_principals WHERE name = @LoginName
)
BEGIN
    RAISERROR('The login %s does not exist.', 16, 1, @LoginName);
    RETURN;
END

-- Check if the user exists
IF (@UserID IS NULL)
BEGIN
    RAISERROR('The user %s not exists in the security.Users table', 16, 1, @LoginName);
    RETURN;
END

-- Chech if the groups exists
IF(@GroupID IS NULL)
BEGIN
    RAISERROR('The group %s not exists in the security.Groups table', 16, 1, @GroupName);
    RETURN;
END


-- Check if the user is already in the group
    IF EXISTS (
        SELECT 1 
        FROM security.GroupMembers gm 
        INNER JOIN security.Users u ON gm.UserID = u.UserID
        INNER JOIN security.Groups g ON gm.GroupID = g.GroupID
        WHERE u.UserLogin = @LoginName AND g.GroupName = @GroupName)
    BEGIN
        RAISERROR('The user %s is already in the group %s.', 10, 1, @LoginName, @GroupName);
        RETURN;
    END
    ELSE
    BEGIN
        -- INSERT in the GroupMembers table
        INSERT INTO security.GroupMembers (GroupID, UserID) VALUES (@GroupID, @UserID);
        RAISERROR('The user %s is now member of the group %s.', 10, 1, @LoginName, @GroupName);
    END

END

