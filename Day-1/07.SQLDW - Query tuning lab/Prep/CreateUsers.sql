if not exists(select * from sys.database_principals where name='smallrc_user')
	create user smallrc_user for login smallrc_user with default_schema=dbo;

if not exists(select * from sys.database_principals where name='largerc_user')
	create user largerc_user for login largerc_user with default_schema=dbo;
go

exec sp_addrolemember 'db_owner', 'smallrc_user';
exec sp_addrolemember 'db_owner', 'largerc_user';
exec sp_addrolemember 'largerc', 'largerc_user';
