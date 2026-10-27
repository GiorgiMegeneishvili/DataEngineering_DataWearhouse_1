from utils.postgre_procedures import call_postgres_procedure
from utils.logger import get_logger

logger = get_logger(__name__)


def Silver_etl(batch_id: int):
    procedures = [
        "silver.sp_load_dim_geography",      # პირველი, person/supplier მასზე არიან დამოკიდებული
        "silver.sp_load_dim_person",
        "silver.sp_load_dim_product",
        "silver.sp_load_dim_sales_territory",
        "silver.sp_load_dim_shipper",
        "silver.sp_load_dim_supplier",
        "silver.sp_load_fact_purchase",      # ფაქტები ბოლოს
        "silver.sp_load_fact_sales",
        "silver.sp_load_fact_work_order",
    ]
    for proc in procedures:
        logger.info(f"▶ Silver Layer Procedure Running:  {proc}")
        call_postgres_procedure(proc, batch_id)
        logger.info(f"✔ Silver Layer Procedure Finished:  {proc}")
