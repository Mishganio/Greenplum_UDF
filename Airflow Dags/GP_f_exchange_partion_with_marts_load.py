from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable


DB_CONN = 'Greenplum_YC'
DDS_SCHEMA = 'dds'
DM_SCHEMA = 'dm'
STG_SCHEMA = 'stg'
DM_MONTH = Variable.get("dm_year_month")
PART_DB = Variable.get("sales_partition_db")
PART_DE = Variable.get("sales_partition_de")

LOAD_MART_MONTH = f"select {STG_SCHEMA}.f_mart_month_load('sales_mart','{DM_SCHEMA}',{DM_MONTH});"
LOAD_RFM_MART_MONTH = f"select {STG_SCHEMA}.f_mart_rfm_month_load('sales_rfm_mart','{DM_SCHEMA}',{DM_MONTH});"
MD_TABLE_LOAD_QUERY = f"select {STG_SCHEMA}.f_full_load(%(tab_name)s,'{DDS_SCHEMA}','{STG_SCHEMA}', %(file_name)s,'crm-sales');"
LOAD_PART_FACT = f"select {STG_SCHEMA}.f_exchange_partition_load('sales','{DDS_SCHEMA}','{STG_SCHEMA}','sdate',{PART_DB},{PART_DE}, 'sales_2023','crm-sales');"

FULL_LOAD_TABLES = ['card', 'product', 'shop']
FULL_LOAD_FILES = {
    'card': 'card',
    'product': 'product',
    'shop': 'shop'
}

default_dag_args = {
    'depends_on_past': False,
    'owner': 'm.alexandrov',
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    "GP_f_exchange_partion_with_marts_load",
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_dag_args,
    catchup=False,
) as dag:
    task_start = DummyOperator(task_id='start')
    

    with TaskGroup('full_load') as task_full_load:
        for table in FULL_LOAD_TABLES:
            task = PostgresOperator(
                task_id=f'load_table_{table}',
                postgres_conn_id=DB_CONN,
                sql=MD_TABLE_LOAD_QUERY,
                parameters={
                    'tab_name': f'{table}',
                    'file_name': f'{FULL_LOAD_FILES[table]}'
                }
            )

    task_sales_part = PostgresOperator(
        task_id='sales_part', postgres_conn_id=DB_CONN, sql=LOAD_PART_FACT
        )
    
    task_mart_month = PostgresOperator(
        task_id='task_mart_month', postgres_conn_id=DB_CONN, sql=LOAD_MART_MONTH
        )

    task_rfm_mart_month = PostgresOperator(
        task_id='task_rfm_mart_month', postgres_conn_id=DB_CONN, sql=LOAD_RFM_MART_MONTH
        )
    
    task_end = DummyOperator(task_id='end')

    task_start  >> task_full_load >> task_sales_part>>  task_mart_month >> task_rfm_mart_month >> task_end