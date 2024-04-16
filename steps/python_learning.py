import os
from snowflake import snowpark
import sf_environment as env

sp_session = snowpark.Session.builder.configs(options=env.connection_paramters).create()
sp_session.sql('USE DATABASE TRAINING_DB').collect()
sp_session.sql('USE SCHEMA ADMIN').collect()
tbl_creation_qry = "CREATE OR REPLACE TABLE TRAINING_DB.ADMIN.EMP_INFO (ID NUMBER(5,0),NAME VARCHAR(20), BAND VARCHAR(5) )"
sp_session.sql(tbl_creation_qry).collect
print('Table successfully created')

sp_session.sql("INSERT INTO TRAINING_DB.ADMIN.EMP_INFO VALUES (1,'Arun','E1.1')").collect()
sp_session.close()