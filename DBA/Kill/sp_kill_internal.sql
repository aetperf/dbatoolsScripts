IF OBJECT_ID('dbo.sp_kill_internal', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_kill_internal;
GO

CREATE PROCEDURE dbo.sp_kill_internal
    @sessionId INT
WITH EXECUTE AS OWNER  -- <-- le propriétaire doit avoir droit de KILL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @KillCommand NVARCHAR(50) = N'KILL ' + CAST(@sessionId AS VARCHAR(10));

    BEGIN TRY
        EXEC(@KillCommand);
    END TRY
    BEGIN CATCH
        -- Pour debug : on ré-affiche la commande et l'erreur
        DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(
            'Echec lors de l''exécution de "%s". Erreur: %s',
            16, 1,
            @KillCommand,
            @ErrMsg
        );
    END CATCH;
END;
GO