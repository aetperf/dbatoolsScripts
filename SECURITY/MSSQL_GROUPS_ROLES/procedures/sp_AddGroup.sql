CREATE PROCEDURE [security].sp_AddGroup(@GroupName NVARCHAR(100))
AS
BEGIN

-- Check if the group already exists
IF EXISTS (
    SELECT 1 FROM security.Groups WHERE GroupName = @GroupName
)
BEGIN
    RAISERROR('The group %s already exists.', 10, 1, @GroupName);
    RETURN;
END
ELSE
BEGIN
    INSERT INTO security.Groups (GroupName) VALUES (@GroupName);
END

END;