from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable


DB_CONN = 'Greenplum_YC'
DM_SCHEMA = 'dm'
STG_SCHEMA = 'stg'
DM_MONTH = Variable.get("dm_year_month")

LOAD_MART_MONTH = f"select {STG_SCHEMA}.f_mart_month_load('sales_mart','{DM_SCHEMA}',{DM_MONTH});"
LOAD_RFM_MART_MONTH = f"select {STG_SCHEMA}.f_mart_rfm_month_load('sales_rfm_mart','{DM_SCHEMA}',{DM_MONTH});"


default_dag_args = {
    'depends_on_past': False,
    'owner': 'm.alexandrov',
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    "GP_f_mart_month_load",
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_dag_args,
    catchup=False,
) as dag:
    task_start = DummyOperator(task_id='start')
    

    task_mart_month = PostgresOperator(
        task_id='task_mart_month', postgres_conn_id=DB_CONN, sql=LOAD_MART_MONTH
        )

    task_rfm_mart_month = PostgresOperator(
        task_id='task_rfm_mart_month', postgres_conn_id=DB_CONN, sql=LOAD_RFM_MART_MONTH
        )

    task_end = DummyOperator(task_id='end')

    task_start  >> task_mart_month >> task_rfm_mart_month >> task_end