create user [asa.sql.workload01] for login [asa.sql.workload01]
create user [asa.sql.workload02] for login [asa.sql.workload02]
execute sp_addrolemember 'db_datareader', 'asa.sql.workload01' 
execute sp_addrolemember 'db_datareader', 'asa.sql.workload02'


create user [asa.sql.import01] for login [asa.sql.import01]
create user [asa.sql.import02] for login [asa.sql.import02]
execute sp_addrolemember 'db_owner', 'asa.sql.import01'  
execute sp_addrolemember 'db_owner', 'asa.sql.import02' 


create user [asa.sql.highperf] for login [asa.sql.highperf]
execute sp_addrolemember 'db_owner', 'asa.sql.highperf' 
execute sp_addrolemember 'largerc', 'asa.sql.highperf' 