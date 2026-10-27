from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def _bronze(name):
    from Bronze_Layer import (
        Br_Layer_AdventureWorks2022_Pipline_AW22 as AW22,
        Br_Layer_Chinook_Pipline_CNK1 as CNK1,
        Br_Layer_NORTHWND_Pipline_NW1 as NW1,
    )
    {"aw22": AW22.AW22_ETL, "cnk1": CNK1.CNK1_ETL, "nw1": NW1.nw1_ETL}[name]()


def _silver(**ctx):
    from Silver_Layer import SL_procedures as Silver_ETL
    Silver_ETL.Silver_etl(int(ctx["ts_nodash"].replace("T", "")))


def _gold(**ctx):
    from Gold_Layer import Gl_procedures as Gold_ETL
    Gold_ETL.Gold_etl(int(ctx["ts_nodash"].replace("T", "")))


with DAG(
    dag_id="dwh_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    bronze = [
        PythonOperator(task_id=f"bronze_{n}", python_callable=_bronze, op_args=[n])
        for n in ("aw22", "cnk1", "nw1")
    ]
    silver = PythonOperator(task_id="silver", python_callable=_silver)
    gold = PythonOperator(task_id="gold", python_callable=_gold)

    bronze[0] >> bronze[1] >> bronze[2] >> silver >> gold
