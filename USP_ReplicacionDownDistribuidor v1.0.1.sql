USE MAXMANAGER
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ==========================================================================================
-- Author:		Milton Santiago Alvarez C.
-- Create date: 2015-08-12
-- Description:	Procedimiento almacenado de replicación de información de locales
-- ==========================================================================================
CREATE PROCEDURE USP_ReplicacionDownDistribuidor
	@idCadena	int
AS
BEGIN
	DECLARE @retval int = 0
	DECLARE @servername sysname
	DECLARE @databasename VARCHAR(128)
	DECLARE @usrId BIGINT = 0
	DECLARE @IPOrigen VARCHAR(15)
	DECLARE @errorNumber INT
	DECLARE @errorProcedure NVARCHAR(128)
	DECLARE @errorLine INT
	DECLARE @errorMensaje NVARCHAR(4000)
	DECLARE @msg NVARCHAR(2048) = 'Error de prueba'
	DECLARE @min INT
    DECLARE @max INT
	DECLARE @sql NVARCHAR(MAX)
	DECLARE @idUpdateStore int

	BEGIN DISTRIBUTED TRANSACTION

	BEGIN TRY
		SELECT	@servername = CONVERT(sysname, IP + ISNULL('\' + Instancia, ''))
				, @databasename = Databasename
		FROM	dbo.BasesReplicacion
		WHERE	cdn_id = @idCadena
				AND tipo = 2

		EXEC @retval = sp_testlinkedserver @servername
	END TRY
    
	BEGIN CATCH
		IF @@ERROR <> 0
		BEGIN
			--SELECT	@usrId = usr_id
			--FROM	dbo.Users_Pos
			--WHERE	usr_iniciales = 'UR'
			SET @usrId = 0

			SET @IPOrigen = (SELECT	client_net_address
							FROM	sys.dm_exec_connections
							WHERE	Session_id = @@SPID)
			
			SELECT	@errorNumber = ERROR_NUMBER()
					, @errorProcedure = ERROR_PROCEDURE()
					, @errorLine = ERROR_LINE()
					, @errorMensaje = ERROR_MESSAGE()
					--, @usrId = ERROR_SEVERITY()

			GOTO ERROR
		end
	END CATCH


	BEGIN TRY
		SET @sql = 'INSERT INTO [' + @servername + '].[' + @databasename + '].[dbo].[UpdateStore]' + CHAR(13) + 
					'SELECT	us.*' + CHAR(13) + 
					'FROM	dbo.UpdateStore us' + CHAR(13) + 
					'		INNER JOIN dbo.ConfiguracionReplica cr ON cr.mdl_id = us.mdl_id' + CHAR(13) + 
					'			AND cr.rst_id = us.rst_id' + CHAR(13) + 
					'			AND cr.tabla = us.tabla' + CHAR(13) + 
					'WHERE	us.replica = 0' + CHAR(13) + 
					'		AND us.cdn_id = ' + CONVERT(NVARCHAR, @idCadena) + CHAR(13) + 
					'		AND cr.direccionReplica = 2' + CHAR(13) + 
					'		AND isnull(us.trama, '''') != ''''' + CHAR(13) + 
					'ORDER BY us.IdUpdateStore'
		EXECUTE sp_executesql @sql

		UPDATE	dbo.UpdateStore
		SET		replica = 1
		where	cdn_id = @idCadena

		--SET @sql = 'EXEC [' + @servername + '].[' + @databasename + '].[dbo].USP_ReplicacionDownTiendas'

		--EXECUTE sp_executesql @sql

	END TRY

	BEGIN CATCH
		IF @@ERROR <> 0
		BEGIN
			--SELECT	@usrId = usr_id
			--FROM	dbo.Users_Pos
			--WHERE	usr_iniciales = 'UR'
			SET @usrId = 0

			SET @IPOrigen = (SELECT	client_net_address
							FROM	sys.dm_exec_connections
							WHERE	Session_id = @@SPID)
			
			SELECT	@errorNumber = ERROR_NUMBER()
					, @errorProcedure = ERROR_PROCEDURE()
					, @errorLine = ERROR_LINE()
					, @errorMensaje = ERROR_MESSAGE()
					--, @usrId = ERROR_SEVERITY()

			GOTO ERROR
		end
	END CATCH

	IF @@TRANCOUNT > 0
		COMMIT TRANSACTION
	RETURN 1

	ERROR:
		IF @@TRANCOUNT > 0
			ROLLBACK TRANSACTION

		INSERT INTO [SRVV-DESARROLLO].POS_LOG.dbo.[LOG] (FECHA, USR_ID, IPORIGEN, ERRORNUMBER, ERRORPROCEDURE, ERRORLINE, ERRORMESSAGE)
		SELECT	GETDATE() 
				, @usrId -- USR_ID - int
				, @IPOrigen -- IPORIGEN - varchar(15)
				, @errorNumber 
				, @errorProcedure 
				, @errorLine 
				, @errorMensaje
		RETURN 0
END
GO
