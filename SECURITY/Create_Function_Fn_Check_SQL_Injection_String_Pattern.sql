-- Create a function that will check for SQL Injection string pattern
-- This function will be used to check for SQL Injection string pattern in the input string
-- This function will return 1 if the input string contains SQL Injection string pattern
-- This function will return 0 if the input string does not contain SQL Injection string pattern
-- This function will be used in the stored procedures to check for SQL Injection string pattern

CREATE OR ALTER FUNCTION [security].Fn_Check_SQL_Injection_String_Pattern(@InputString NVARCHAR(4000))
RETURNS INT
AS
BEGIN
    DECLARE @Result INT = 0;

    DECLARE @Pattern TABLE (SearchItems VARCHAR(1000));

    INSERT INTO @Pattern (SearchItems) VALUES ('%;%');
    INSERT INTO @Pattern (SearchItems) VALUES ('%''%');
    INSERT INTO @Pattern (SearchItems) VALUES ('%--%');
    INSERT INTO @Pattern (SearchItems) VALUES ('%xp\_%');
    INSERT INTO @Pattern (SearchItems) VALUES ('%/*%*/%');
    INSERT INTO @Pattern (SearchItems) VALUES ('%update%');
    INSERT INTO @Pattern (SearchItems) VALUES ('%delete%');
    INSERT INTO @Pattern (SearchItems) VALUES ('%drop%');
    INSERT INTO @Pattern (SearchItems) VALUES ('%alter%');
    INSERT INTO @Pattern (SearchItems) VALUES ('%create%');
    INSERT INTO @Pattern (SearchItems) VALUES ('%exec%');

    SELECT @Result = 1 FROM @Pattern WHERE @InputString LIKE SearchItem  ESCAPE '\';   

    RETURN @Result;
END;