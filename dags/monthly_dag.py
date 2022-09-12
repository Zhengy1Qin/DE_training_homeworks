import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from data_process import dm_tables_sqls as dm_sqls

default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'

with DAG(
        dag_id="monthly_dag",
        start_date=datetime.datetime(2022, 8, 31),
        schedule_interval="00 00 1 * *",
        default_args=default_args,
        catchup=False,
) as dag:
    dag_start = BashOperator(
        task_id='monthly_dag_start',
        bash_command='echo Load data into dm',
    )

    dm_city_sales = PostgresOperator(
        task_id="dm_city_sales",
        postgres_conn_id=connection_id,
        sql=dm_sqls.dm_city_sales_sql,
    )

    dm_product_sales = PostgresOperator(
        task_id="dm_product_sales",
        postgres_conn_id=connection_id,
        sql=dm_sqls.dm_product_sales_sql,
    )

    dag_finished = BashOperator(
        task_id='monthly_dag_finished',
        bash_command='echo All data have been transformed from dw to dm',
    )

    dag_start >> [dm_city_sales, dm_product_sales] >> dag_finished
