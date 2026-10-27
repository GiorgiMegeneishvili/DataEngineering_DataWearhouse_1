from utils.postgre_procedures import call_postgres_procedure
from utils.logger import get_logger

logger = get_logger(__name__)


def Gold_etl(batch_id: int):
    procedures = [
        "gold.sp_load_mart_sales",
        "gold.sp_load_mart_purchase",
        "gold.sp_load_mart_music_sales",
    ]

    for proc in procedures:
        logger.info(f"▶ Gold Layer Procedure Running:  {proc}")
        call_postgres_procedure(proc, batch_id)
        logger.info(f"✔ Gold Layer Procedure Finished:  {proc}")
