CREATE TRIGGER TRG_RegistroUpdateStore
ON dbo.Plus
AFTER INSERT, UPDATE AS 
BEGIN
	declare @tableName varchar(50)
	declare @esquema varchar(50)
	declare @campos varchar(max)
	declare @valores nvarchar(max)
	declare @sql nvarchar(max)
	declare @parametros nvarchar(500)
	declare @trama nvarchar(max)

	set @tableName = 'Plus'

	select	@esquema = u.name
	from	sysobjects t
			inner join sysusers u on u.uid = t.uid
	where	t.type = 'u'
			and t.name = @tableName


	select	@campos = STUFF((	select	', ' + CAST(c.name as varchar(20)) [text()]
								FROM	sysobjects t
										inner join syscolumns c on c.id = t.id
								where	t.name = @tableName
								order by	t.name
											, c.colorder
								for xml path(''), type)
						.value('.','NVARCHAR(MAX)'),1,2,' ') --as campos
	from	sysobjects t
			inner join syscolumns c on c.id = t.id
	where	t.name = @tableName
	group by	t.name

	--select @esquema, @campos

	select	@valores = STUFF((	select	' + '', '' + isnull(' +
										CASE
											WHEN tip.name = 'char' THEN ''''''''' + '
											WHEN tip.name = 'varchar' THEN ''''''''' + '
											WHEN tip.name = 'date' THEN ''''''''' + '
											WHEN tip.name = 'datetime' THEN ''''''''' + '
											WHEN tip.name = 'varbinary' THEN '''''''0x'' + '
											ELSE ''
										END
										+ 'convert(varchar(max), ' +
										+ CAST(c.name as varchar(20)) +
										CASE
											WHEN tip.name = 'char' THEN ') + '''''''''
											WHEN tip.name = 'varchar' THEN ') + '''''''''
											WHEN tip.name = 'date' THEN ', 21) + '''''''''
											WHEN tip.name = 'datetime' THEN ', 21) + '''''''''
											WHEN tip.name = 'varbinary' THEN ', 2) + '''''''''
											ELSE ')'
										END
										+ ', ''NULL'')'
										[text()]
								FROM	sysobjects t
										inner join syscolumns c on c.id = t.id
										inner join systypes tip on tip.xtype = c.xtype
								where	t.name = @tableName
								order by	t.name
											, c.colorder
								for xml path(''), type)
						.value('.', 'NVARCHAR(MAX)'), 1, 10, ' ') --as campos
	from	sysobjects t
			inner join syscolumns c on c.id = t.id
			inner join systypes tip on tip.xtype = c.xtype
	where	t.name = @tableName
	group by	t.name

	--set @sql = ''
	--set @sql = 'select ''insert into [' + @esquema + '].[' + @tableName + '] (' + @campos + ') values('' + ' + @valores + ' + '')'' from [' + @esquema + '].[' + @tableName + '] where usr_id = 13'
	set @sql = 'select ''insert into [' + @esquema + '].[' + @tableName + '] (' + @campos + ') values('' + ' + @valores + ' + '')'' from inserted'
	--select @sql

	set @parametros = '@tramaOut nvarchar(max) output'

	execute sp_executesql @sql, @parametros, @tramaOut = @trama output

	insert into UpdateStore
	select	'Plus'
			, @trama
			, 0			--	Módulo
			, 0			--	Tienda
			, 0			--	Flag Replica
END
GO