create user [asa.sql.workload01_#USER_CONTEXT#] for login [asa.sql.workload01_#USER_CONTEXT#]
create user [asa.sql.workload02_#USER_CONTEXT#] for login [asa.sql.workload02_#USER_CONTEXT#]
execute sp_addrolemember 'db_datareader', 'asa.sql.workload01_#USER_CONTEXT#' 
execute sp_addrolemember 'db_datareader', 'asa.sql.workload02_#USER_CONTEXT#'

CREATE USER [#USER_NAME#] FROM EXTERNAL PROVIDER;
EXEC sp_addrolemember 'db_owner', '#USER_NAME#'