USE POS_DISTRIBUIDOR
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ==========================================================================================
-- Author:		Milton Santiago Alvarez C.
-- Create date: 2015-08-12
-- Description:	Procedimiento almacenado de replicación de data de Distribuidor a Tiendas
-- ==========================================================================================
ALTER PROCEDURE USP_ReplicacionDownTiendas
	
AS
BEGIN
	DECLARE @retval int = 0
	DECLARE @servername sysname
	DECLARE @databasename VARCHAR(128)
	DECLARE @usrId BIGINT
	DECLARE @idCadena INT
    DECLARE @idTienda INT
	DECLARE @IPOrigen VARCHAR(15)
	DECLARE @errorNumber INT
	DECLARE @errorProcedure NVARCHAR(128)
	DECLARE @errorLine INT
	DECLARE @errorMensaje NVARCHAR(4000)
	DECLARE @msg NVARCHAR(2048) = 'Error de prueba'
	DECLARE @mint INT
    DECLARE @maxt INT
	DECLARE @min INT
    DECLARE @max INT
	DECLARE @sql NVARCHAR(MAX)
	DECLARE @idUpdateStore int

	BEGIN DISTRIBUTED TRANSACTION

	BEGIN TRY
		SELECT	ROW_NUMBER() OVER(ORDER BY us.rst_id) AS rowNum
				, us.rst_id
		INTO #TiendasReplica
		FROM	dbo.UpdateStore us
				INNER JOIN dbo.ConfiguracionReplica cr ON cr.mdl_id = us.mdl_id
					AND cr.rst_id = us.rst_id
					AND cr.tabla = us.tabla
		WHERE	us.replica = 0
				AND cr.direccionReplica = 2
				AND ISNULL(us.trama, '') != ''
		GROUP BY	us.rst_id
		ORDER BY	us.rst_id

		SELECT	@mint = MIN(rowNum)
				, @maxt = MAX(rowNum)
		FROM	#TiendasReplica

		WHILE @mint <= @maxt
		BEGIN
			SELECT	@idTienda = rst_id
			FROM	#TiendasReplica
			WHERE	rowNum = @mint

			SELECT	@idCadena = cdn_id
			FROM	dbo.Restaurante
			WHERE	rst_id = @idTienda

			BEGIN TRY
				SELECT	@servername = CONVERT(sysname, IP + ISNULL('\' + Instancia, ''))
						, @databasename = Databasename
				FROM	dbo.BasesReplicacion
				WHERE	cdn_id = @idCadena
						AND tipo = 2
						AND rst_id = @idTienda

				EXEC @retval = sp_testlinkedserver @servername
			END TRY
    
			BEGIN CATCH
				IF @@ERROR <> 0
				BEGIN
					SELECT	@usrId = usr_id
					FROM	dbo.Users_Pos
					WHERE	usr_iniciales = 'UR'

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

			SELECT	ROW_NUMBER() OVER(ORDER BY us.rst_id, us.IdUpdateStore) AS rowNum
					, us.*
			INTO #RegistrosReplica
			FROM	dbo.UpdateStore us
					INNER JOIN dbo.ConfiguracionReplica cr ON cr.mdl_id = us.mdl_id
						AND cr.rst_id = us.rst_id
						AND cr.tabla = us.tabla
			WHERE	us.replica = 0
					AND cr.direccionReplica = 2
					AND us.rst_id = @idTienda
			GROUP BY	us.rst_id
						, us.IdUpdateStore
						, us.tabla
						, us.trama
						, us.mdl_id
						, us.usr_id
						, us.Fecha
						, us.Hora
						, us.replica
			ORDER BY	us.rst_id
						, us.IdUpdateStore

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

			SET @sql = 'INSERT INTO [' + @servername + '].[' + @databasename + '].[dbo].[UpdateStore]' + CHAR(13) + 
						'SELECT	us.*' + CHAR(13) + 
						'FROM	dbo.UpdateStore us' + CHAR(13) + 
						'		INNER JOIN dbo.ConfiguracionReplica cr ON cr.mdl_id = us.mdl_id' + CHAR(13) + 
						'			AND cr.rst_id = us.rst_id' + CHAR(13) + 
						'			AND cr.tabla = us.tabla' + CHAR(13) + 
						'WHERE	us.replica = 0' + CHAR(13) + 
						'		AND cr.direccionReplica = 2' + CHAR(13) + 
						'		AND rst_id = ' + CONVERT(NVARCHAR, @idTienda) + CHAR(13) + 
						'		AND isnull(us.trama, '''') != ''''' + CHAR(13) + 
						'ORDER BY us.IdUpdateStore'
			EXECUTE sp_executesql @sql

			--UPDATE	dbo.UpdateStore
			--SET		replica = 1
			--WHERE	rst_id = @idTienda

			--SET @sql = 'EXEC [' + @servername + '].[' + @databasename + '].[dbo].USP_InsertUpdateStoreTiendas'
			--EXECUTE sp_executesql @sql

			SET @mint = @mint + 1
		END

	END TRY

	BEGIN CATCH
		IF @@ERROR <> 0
		BEGIN
			SELECT	@usrId = usr_id
			FROM	dbo.Users_Pos
			WHERE	usr_iniciales = 'UR'

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
		INSERT INTO POS_LOG.dbo.LOG
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
