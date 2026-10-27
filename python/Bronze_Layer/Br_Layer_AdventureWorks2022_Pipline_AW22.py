from utils.SparkSession import get_spark
from utils.logger import get_logger
from utils.config import (
    connection_properties,   # MSSQL source properties
    jdbc_url_AW22,           # ADVENTUREWORKS2022 DATABASE URL
    POSTGRES_URL,            # PostgreSQL url
    POSTGRES_PROPERTIES,     # PostgreSQL properties
)

spark = get_spark()
logger = get_logger(__name__)



def run_pg_sql(sql):
    """Runs a single SQL statement on PostgreSQL through Spark's JVM (no extra Python libs needed)."""
    jvm = spark._sc._gateway.jvm
    props = jvm.java.util.Properties()
    props.setProperty("user", POSTGRES_PROPERTIES["user"])
    props.setProperty("password", POSTGRES_PROPERTIES["password"])
    conn = jvm.org.postgresql.Driver().connect(POSTGRES_URL, props)
    try:
        st = conn.createStatement()
        st.executeUpdate(sql)
        st.close()
    finally:
        conn.close()


def load_to_bronze(jdbc_url_src, src_table, bronze_table,
                   load_type="full", watermark_col=None, cols=None):
    """
    load_type="full"        -> TRUNCATE (all partitions) + reload whole table (DDL stays)
    load_type="incremental" -> reads only the last 7 days of watermark_col
                               (Oracle: col >= trunc(sysdate) - 7) and APPENDS them.
    cols                    -> explicit column list for tables that contain MSSQL types
                               Spark JDBC cannot read (xml, geography, hierarchyid).
    load_date is not in the DataFrame -> PostgreSQL fills it via DEFAULT CURRENT_DATE.
    """
    incremental = load_type == "incremental"
    select_list = cols or "*"
    read_from = src_table if not cols else f"(SELECT {select_list} FROM {src_table}) AS src"

    if incremental:
        # MSSQL equivalent of Oracle: watermark_col >= trunc(sysdate) - 7
        read_from = (f"(SELECT {select_list} FROM {src_table} "
                     f"WHERE {watermark_col} >= DATEADD(day, -7, CAST(GETDATE() AS date))) AS src")
        print(f"{bronze_table}: incremental, last 7 days by {watermark_col}")

    df = spark.read.jdbc(url=jdbc_url_src, table=read_from, properties=connection_properties)
    df = df.toDF(*[c.lower() for c in df.columns]).cache()

    if not incremental:
        df.count()   # force the MSSQL read BEFORE truncating: a failed read never empties the table
        # Spark's own truncate uses "TRUNCATE TABLE ONLY", which PostgreSQL rejects on partitioned tables
        run_pg_sql(f"TRUNCATE TABLE bronze.{bronze_table}")

    df.write.mode("append").jdbc(url=POSTGRES_URL, table=f"bronze.{bronze_table}",
                                 properties=POSTGRES_PROPERTIES)

    print(f"bronze.{bronze_table}: {df.count()} rows ({load_type})")
    df.unpersist()


# --- incremental variant (big transactional tables by ModifiedDate) - uncomment and comment out the full block below
# AW22_TABLES = {
#     "Sales.SalesOrderHeader": {"target": "aw22_salesorderheader", "load": "incremental", "watermark": "ModifiedDate"},
#     "Sales.SalesOrderDetail": {"target": "aw22_salesorderdetail", "load": "incremental", "watermark": "ModifiedDate"},
#     "Sales.Customer": {"target": "aw22_customer", "load": "full"},
#     "Sales.SalesPerson": {"target": "aw22_salesperson", "load": "full"},
#     "Sales.SalesTerritory": {"target": "aw22_salesterritory", "load": "full"},
#     "Sales.Store": {"target": "aw22_store", "load": "full",
#         "cols": "businessentityid, name, salespersonid, rowguid, modifieddate"},
#     "Person.Person": {"target": "aw22_person", "load": "full",
#         "cols": "businessentityid, persontype, namestyle, title, firstname, middlename, lastname, suffix, emailpromotion, rowguid, modifieddate"},
#     "Person.Address": {"target": "aw22_address", "load": "full",
#         "cols": "addressid, addressline1, addressline2, city, stateprovinceid, postalcode, rowguid, modifieddate"},
#     "Person.StateProvince": {"target": "aw22_stateprovince", "load": "full"},
#     "Person.CountryRegion": {"target": "aw22_countryregion", "load": "full"},
#     "Person.BusinessEntityAddress": {"target": "aw22_businessentityaddress", "load": "full"},
#     "HumanResources.Employee": {"target": "aw22_employee", "load": "full",
#         "cols": "businessentityid, nationalidnumber, loginid, organizationlevel, jobtitle, birthdate, maritalstatus, gender, hiredate, salariedflag, vacationhours, sickleavehours, currentflag, rowguid, modifieddate"},
#     "Production.Product": {"target": "aw22_product", "load": "full"},
#     "Production.ProductSubcategory": {"target": "aw22_productsubcategory", "load": "full"},
#     "Production.ProductCategory": {"target": "aw22_productcategory", "load": "full"},
#     "Production.WorkOrder": {"target": "aw22_workorder", "load": "incremental", "watermark": "ModifiedDate"},
#     "Purchasing.PurchaseOrderHeader": {"target": "aw22_purchaseorderheader", "load": "incremental", "watermark": "ModifiedDate"},
#     "Purchasing.PurchaseOrderDetail": {"target": "aw22_purchaseorderdetail", "load": "incremental", "watermark": "ModifiedDate"},
#     "Purchasing.Vendor": {"target": "aw22_vendor", "load": "full"},
# }

# --- full reload (active)
AW22_TABLES = {
    "Sales.SalesOrderHeader": {"target": "aw22_salesorderheader", "load": "full"},
    "Sales.SalesOrderDetail": {"target": "aw22_salesorderdetail", "load": "full"},
    "Sales.Customer": {"target": "aw22_customer", "load": "full"},
    "Sales.SalesPerson": {"target": "aw22_salesperson", "load": "full"},
    "Sales.SalesTerritory": {"target": "aw22_salesterritory", "load": "full"},
    "Sales.Store": {"target": "aw22_store", "load": "full",
        "cols": "businessentityid, name, salespersonid, rowguid, modifieddate"},
    "Person.Person": {"target": "aw22_person", "load": "full",
        "cols": "businessentityid, persontype, namestyle, title, firstname, middlename, lastname, suffix, emailpromotion, rowguid, modifieddate"},
    "Person.Address": {"target": "aw22_address", "load": "full",
        "cols": "addressid, addressline1, addressline2, city, stateprovinceid, postalcode, rowguid, modifieddate"},
    "Person.StateProvince": {"target": "aw22_stateprovince", "load": "full"},
    "Person.CountryRegion": {"target": "aw22_countryregion", "load": "full"},
    "Person.BusinessEntityAddress": {"target": "aw22_businessentityaddress", "load": "full"},
    "HumanResources.Employee": {"target": "aw22_employee", "load": "full",
        "cols": "businessentityid, nationalidnumber, loginid, organizationlevel, jobtitle, birthdate, maritalstatus, gender, hiredate, salariedflag, vacationhours, sickleavehours, currentflag, rowguid, modifieddate"},
    "Production.Product": {"target": "aw22_product", "load": "full"},
    "Production.ProductSubcategory": {"target": "aw22_productsubcategory", "load": "full"},
    "Production.ProductCategory": {"target": "aw22_productcategory", "load": "full"},
    "Production.WorkOrder": {"target": "aw22_workorder", "load": "full"},
    "Purchasing.PurchaseOrderHeader": {"target": "aw22_purchaseorderheader", "load": "full"},
    "Purchasing.PurchaseOrderDetail": {"target": "aw22_purchaseorderdetail", "load": "full"},
    "Purchasing.Vendor": {"target": "aw22_vendor", "load": "full"},
}


def AW22_ETL():
    logger.info("Data Extract From ADVENTUREWORKS2022 DATABASE has been started:")
    for src, cfg in AW22_TABLES.items():
        load_to_bronze(jdbc_url_AW22, src, cfg["target"],
                       load_type=cfg["load"], watermark_col=cfg.get("watermark"),
                       cols=cfg.get("cols"))
    logger.info("Data Extracted Successfully From ADVENTUREWORKS2022 DATABASE")

# if __name__ == "__main__":
#     # one table:
#     # load_to_bronze(jdbc_url_AW22, "Production.ProductCategory", "aw22_productcategory")   # full
#     AW22_ETL()
