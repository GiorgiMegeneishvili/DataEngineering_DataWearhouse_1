from utils.SparkSession import get_spark
from utils.logger import get_logger
from utils.config import (
    connection_properties,   # MSSQL source properties
    jdbc_url_NW1,            # NORTHWIND DATABASE URL
    POSTGRES_URL,            # PostgreSQL url
    POSTGRES_PROPERTIES,     # PostgreSQL properties
)

spark = get_spark()
logger = get_logger(__name__)


def _sql_literal(v):
    """datetime -> 'YYYY-MM-DD HH:MM:SS.mmm' (MSSQL datetime allows max 3 fractional digits)"""
    if hasattr(v, "hour"):
        return v.isoformat(sep=" ", timespec="milliseconds")
    return str(v)


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
                   load_type="full", watermark_col=None):
    """
    load_type="full"        -> truncate + reload (DDL stays)
    load_type="incremental" -> reads only rows with watermark_col > MAX(watermark_col)
                               already in bronze, and APPENDS them.
                               First run (empty table) = full read.
    load_date is not in the DataFrame -> PostgreSQL fills it via DEFAULT CURRENT_DATE.
    """
    incremental = load_type == "incremental"
    read_from = src_table

    if incremental:
        last = (spark.read
                .jdbc(url=POSTGRES_URL,
                      table=f"(SELECT MAX({watermark_col}) AS mx FROM bronze.{bronze_table}) t",
                      properties=POSTGRES_PROPERTIES)
                .first()["mx"])
        if last is not None:
            read_from = (f"(SELECT * FROM {src_table} "
                         f"WHERE {watermark_col} > '{_sql_literal(last)}') AS src")
        print(f"{bronze_table}: incremental, last {watermark_col} = {last}")

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


# --- incremental variant (Orders by OrderDate) - uncomment and comment out the full block below
# NW1_TABLES = {
#     "Customers":       {"target": "nw1_customers",     "load": "full"},
#     "Employees":       {"target": "nw1_employees",     "load": "full"},
#     "Products":        {"target": "nw1_products",      "load": "full"},
#     "Categories":      {"target": "nw1_categories",    "load": "full"},
#     "Suppliers":       {"target": "nw1_suppliers",     "load": "full"},
#     "Shippers":        {"target": "nw1_shippers",      "load": "full"},
#     "Orders":          {"target": "nw1_orders",        "load": "incremental", "watermark": "OrderDate"},
#     "[Order Details]": {"target": "nw1_order_details", "load": "full"},   # no date column
# }

# --- full reload (active)
NW1_TABLES = {
    "Customers":       {"target": "nw1_customers",     "load": "full"},
    "Employees":       {"target": "nw1_employees",     "load": "full"},
    "Products":        {"target": "nw1_products",      "load": "full"},
    "Categories":      {"target": "nw1_categories",    "load": "full"},
    "Suppliers":       {"target": "nw1_suppliers",     "load": "full"},
    "Shippers":        {"target": "nw1_shippers",      "load": "full"},
    "Orders":          {"target": "nw1_orders",        "load": "full"},
    "[Order Details]": {"target": "nw1_order_details", "load": "full"},   # no date column
}


def nw1_ETL():
    logger.info("Data Extract From NORTHWIND DATABASE has been started:")
    for src, cfg in NW1_TABLES.items():
        load_to_bronze(jdbc_url_NW1, src, cfg["target"],
                       load_type=cfg["load"], watermark_col=cfg.get("watermark"))

    logger.info("Data Extracted Successfully From NORTHWIND DATABASE")
# if __name__ == "__main__":
#     # one table:
#     # load_to_bronze(jdbc_url_NW1, "Customers", "nw1_customers")                     # full
#     # load_to_bronze(jdbc_url_NW1, "Orders", "nw1_orders",
#     #                load_type="incremental", watermark_col="OrderDate")             # incremental
#     nw1_ETL()
