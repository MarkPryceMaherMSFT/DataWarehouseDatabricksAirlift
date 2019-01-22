if not exists(select * from sys.sql_logins where name='smallrc_user')
	create login smallrc_user with password='S3cret!Pass';

if not exists(select * from sys.sql_logins where name='largerc_user') 
	create login largerc_user with password='S3cret!Pass';
