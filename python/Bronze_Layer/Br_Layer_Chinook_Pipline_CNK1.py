from utils.SparkSession import get_spark
from utils.logger import get_logger
from utils.config import (
    connection_properties,   # MSSQL source properties
    jdbc_url_CNK1,           # CHINOOK DATABASE URL MSSQL
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
                   load_type="full", watermark_col=None):
    """
    load_type="full"        -> TRUNCATE (all partitions) + reload whole table (DDL stays)
    load_type="incremental" -> reads only the last 7 days of watermark_col
                               (Oracle: col >= trunc(sysdate) - 7) and APPENDS them.
    load_date is not in the DataFrame -> PostgreSQL fills it via DEFAULT CURRENT_DATE.
    """
    incremental = load_type == "incremental"
    read_from = src_table

    if incremental:
        # MSSQL equivalent of Oracle: watermark_col >= trunc(sysdate) - 7
        read_from = (f"(SELECT * FROM {src_table} "
                     f"WHERE {watermark_col} >= DATEADD(day, -7, CAST(GETDATE() AS date))) AS src")
        print(f"{bronze_table}: incremental, last 7 days by {watermark_col}")

    df = spark.read.jdbc(url=jdbc_url_src, table=read_from, properties=connection_properties)
    df = df.toDF(*[c.lower() for c in df.columns]).cache()

    if not incremental:
        df.count()   # force the MSSQL read BEFORE truncating: a failed read never empties the table
        # Spark's own truncate uses "TRUNCATE TABLE ONLY", which PostgreSQL rejects on partitioned tables
        run_pg_sql(f"TRUNCATE TABLE bronze.{bronze_table}")

    df.write.mode("append").jdbc(url=POSTGRES_URL, table=f"bronze.{bronze_table}", properties=POSTGRES_PROPERTIES)

    print(f"bronze.{bronze_table}: {df.count()} rows ({load_type})")
    df.unpersist()


# --- incremental variant (Invoice by InvoiceDate) - uncomment and comment out the full block below
# CNK1_TABLES = {
#     "Customer":    {"target": "cnk1_customer",    "load": "full"},
#     "Employee":    {"target": "cnk1_employee",    "load": "full"},
#     "Artist":      {"target": "cnk1_artist",      "load": "full"},
#     "Album":       {"target": "cnk1_album",       "load": "full"},
#     "Track":       {"target": "cnk1_track",       "load": "full"},
#     "Genre":       {"target": "cnk1_genre",       "load": "full"},
#     "MediaType":   {"target": "cnk1_mediatype",   "load": "full"},
#     "Invoice":     {"target": "cnk1_invoice",     "load": "incremental", "watermark": "InvoiceDate"},
#     "InvoiceLine": {"target": "cnk1_invoiceline", "load": "full"},   # no date column
# }

# --- full reload (active)
CNK1_TABLES = {
    "Customer":    {"target": "cnk1_customer",    "load": "full"},
    "Employee":    {"target": "cnk1_employee",    "load": "full"},
    "Artist":      {"target": "cnk1_artist",      "load": "full"},
    "Album":       {"target": "cnk1_album",       "load": "full"},
    "Track":       {"target": "cnk1_track",       "load": "full"},
    "Genre":       {"target": "cnk1_genre",       "load": "full"},
    "MediaType":   {"target": "cnk1_mediatype",   "load": "full"},
    "Invoice":     {"target": "cnk1_invoice",     "load": "full"},
    "InvoiceLine": {"target": "cnk1_invoiceline", "load": "full"},   # no date column
}


def CNK1_ETL():
    logger.info("Data Extract From CHINOOK DATABASE has been started:")
    for src, cfg in CNK1_TABLES.items():
        load_to_bronze(jdbc_url_CNK1, src, cfg["target"],
                       load_type=cfg["load"], watermark_col=cfg.get("watermark"))

    logger.info("Data Extracted Successfully From CHINOOK DATABASE")
# if __name__ == "__main__":
#     # one table:
#     # load_to_bronze(jdbc_url_CNK1, "Customer", "cnk1_customer")                     # full
#     # load_to_bronze(jdbc_url_CNK1, "Invoice", "cnk1_invoice",
#     #                load_type="incremental", watermark_col="InvoiceDate")           # incremental
#     CNK1_ETL()
