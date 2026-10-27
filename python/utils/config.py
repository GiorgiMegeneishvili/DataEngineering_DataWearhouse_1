from dotenv import load_dotenv
import os

load_dotenv()
#######################################################################################
#################   MSSQL SERVER Source AdventureWorks2022  ###########################
#######################################################################################

DB_SQL_DATABASE_AdventureWorks2022 = os.getenv("DB_SQL_DATABASE_AdventureWorks2022")
DB_SQL_USER = os.getenv("DB_SQL_USER")
DB_SQL_PASSWORD = os.getenv("DB_SQL_PASSWORD")
DB_SQL_DRIVER = os.getenv("DB_SQL_DRIVER")



#######################################################################################
#################   MSSQL SERVER Source NORTHWND  #####################################
#######################################################################################

DB_SQL_DATABASE_NORTHWND = os.getenv("DB_SQL_DATABASE_NORTHWND")
DB_SQL_USER = os.getenv("DB_SQL_USER")
DB_SQL_PASSWORD = os.getenv("DB_SQL_PASSWORD")
DB_SQL_DRIVER = os.getenv("DB_SQL_DRIVER")




#######################################################################################
#################   MSSQL SERVER Source Chinook   #####################################
#######################################################################################

DB_SQL_DATABASE_Chinook = os.getenv("DB_SQL_DATABASE_Chinook")
DB_SQL_USER = os.getenv("DB_SQL_USER")
DB_SQL_PASSWORD = os.getenv("DB_SQL_PASSWORD")
DB_SQL_DRIVER = os.getenv("DB_SQL_DRIVER")


connection_properties = {
    "user": DB_SQL_USER,
    "password": DB_SQL_PASSWORD,  # your real sa password
    "driver": DB_SQL_DRIVER
}



#######################################################################################
#######################################################################################
#######################################################################################


#print(DB_SQL_DATABASE_Chinook, DB_SQL_USER)   # შესამოწმებლად
#print(DB_SQL_DATABASE_NORTHWND, DB_SQL_USER)   # შესამოწმებლად
#print(DB_SQL_DATABASE_AdventureWorks2022, DB_SQL_USER)   # შესამოწმებლად


DB_SQL_HOST = os.getenv("DB_SQL_HOST", "localhost")

jdbc_url_AW22 = f"jdbc:sqlserver://{DB_SQL_HOST}:1433;database={DB_SQL_DATABASE_AdventureWorks2022};encrypt=false;"
jdbc_url_NW1 = f"jdbc:sqlserver://{DB_SQL_HOST}:1433;database={DB_SQL_DATABASE_NORTHWND};encrypt=false;"
jdbc_url_CNK1 = f"jdbc:sqlserver://{DB_SQL_HOST}:1433;database={DB_SQL_DATABASE_Chinook};encrypt=false;"


##########################################################################################
#################   POSTGRESQL SERVER Destination Data LakeHouse #########################
##########################################################################################



DB_POSTGRESQL_DATABASE = os.getenv("DB_POSTGRESQL_DATABASE")
DB_POSTGRESQL_USER = os.getenv("DB_POSTGRESQL_USER")
DB_POSTGRESQL_PASSWORD = os.getenv("DB_POSTGRESQL_PASSWORD")
DB_POSTGRESQL_DRIVER = os.getenv("DB_POSTGRESQL_DRIVER")
DB_POSTGRESQL_HOST = os.getenv("DB_POSTGRESQL_HOST", "localhost")
DB_POSTGRESQL_PORT = os.getenv("DB_POSTGRESQL_PORT", "5432")


# Spark JDBC
POSTGRES_URL = f"jdbc:postgresql://{DB_POSTGRESQL_HOST}:{DB_POSTGRESQL_PORT}/{DB_POSTGRESQL_DATABASE}"
POSTGRES_PROPERTIES = {
    "user": DB_POSTGRESQL_USER,
    "password": DB_POSTGRESQL_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# psycopg2 (Silver layer procedures)
POSTGRES_PG_CONFIG = {
    "host": DB_POSTGRESQL_HOST,
    "port": DB_POSTGRESQL_PORT,
    "dbname": DB_POSTGRESQL_DATABASE,
    "user": DB_POSTGRESQL_USER,
    "password": DB_POSTGRESQL_PASSWORD
}
