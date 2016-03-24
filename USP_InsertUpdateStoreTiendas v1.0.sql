USE MAXPOINT_DEV_DISTRIB
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================================================================
-- Author:		Milton Santiago Alvarez C.
-- Create date: 2015-08-12
-- Description:	Procedimiento almacenado de actualización de registros en UpdateStore
-- =============================================================================================
CREATE PROCEDURE USP_InsertUpdateStoreTiendas
	
AS
BEGIN
	DECLARE @retval int = 0
	DECLARE @servername sysname
	DECLARE @databasename VARCHAR(128)
	DECLARE @usrId bigint
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

	BEGIN TRANSACTION

	BEGIN TRY
		SELECT	ROW_NUMBER() OVER(ORDER BY us.IdUpdateStore) AS rowNum
				, us.*
		INTO #RegistrosReplica
		FROM	dbo.UpdateStore us
				INNER JOIN dbo.ConfiguracionReplica cr ON cr.mdl_id = us.mdl_id
					AND cr.rst_id = us.rst_id
					AND cr.tabla = us.tabla
		WHERE	us.replica = 0
				AND cr.direccionReplica = 2
		GROUP BY	us.IdUpdateStore
					, us.cdn_id
					, us.rst_id
					, us.tabla
					, us.trama
					, us.mdl_id
					, us.usr_id
					, us.Fecha
					, us.Hora
					, us.replica
					, us.intFlag1
					, us.intFlag2
					, us.intFlag3
					, us.bitFlag1
					, us.bitFlag2
					, us.bitFlag3
		ORDER BY	us.IdUpdateStore

		SELECT	@min = MIN(rowNum)
				, @max = MAX(rowNum)
		FROM	#RegistrosReplica

		WHILE @min <= @max
		BEGIN
			SELECT	@sql = trama
					, @idUpdateStore = IdUpdateStore
			FROM	#RegistrosReplica
			WHERE	rowNum = @min

			EXECUTE sp_executesql @sql

			UPDATE	dbo.UpdateStore
			SET		replica = 1
			WHERE	IdUpdateStore = @idUpdateStore
			
			SET @min = @min + 1
		END

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
		INSERT INTO POS_LOG.dbo.[LOG]
		SELECT	GETDATE() 
				, @usrId -- USR_ID - int
				, @IPOrigen -- IPORIGEN - varchar(15)
				, @errorNumber 
				, @errorProcedure 
				, @errorLine 
				, @errorMensaje

		IF @@TRANCOUNT > 0
			ROLLBACK TRANSACTION
		RETURN 0
END
GO
