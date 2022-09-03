import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.load_csv_to_postgres import load_csv_to_postgres

from data_process import dw_tables_sqls as dw_sqls

default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'

with DAG(
        dag_id="daily_dag",
        start_date=datetime.datetime(2022, 8, 31),
        schedule_interval="00 23 * * *",
        default_args=default_args,
        catchup=False,
) as dag:
    dag_start = BashOperator(
        task_id='dag_start',
        bash_command='echo Load raw data to ods',
    )

    load_sales_order_to_ods = PythonOperator(
        task_id='load_sales_order_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/sales_order.csv",
            'target': "/data/raw/{{ ds }}/sales_order.csv",
            'table_name': 'sales_order_ods',
            'connection_id': connection_id,
        },
    )

    load_product_to_ods = PythonOperator(
        task_id='load_product_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/product.csv",
            'target': "/data/raw/{{ ds }}/product.csv",
            'table_name': 'product_ods',
            'connection_id': connection_id,
        },
    )

    load_address_to_ods = PythonOperator(
        task_id='load_address_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/address.csv",
            'target': "/data/raw/{{ ds }}/address.csv",
            'table_name': 'address_ods',
            'connection_id': connection_id,
        },
    )

    load_customer_address_to_ods = PythonOperator(
        task_id='load_customer_address_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/customer_address.csv",
            'target': "/data/raw/{{ ds }}/customer_address.csv",
            'table_name': 'customer_address_ods',
            'connection_id': connection_id,
        },
    )

    load_customer_to_ods = PythonOperator(
        task_id='load_customer_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/customer.csv",
            'target': "/data/raw/{{ ds }}/customer.csv",
            'table_name': 'customer_ods',
            'connection_id': connection_id,
        },
    )

    load_product_category_to_ods = PythonOperator(
        task_id='load_product_category_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/product_category.csv",
            'target': "/data/raw/{{ ds }}/product_category.csv",
            'table_name': 'product_category_ods',
            'connection_id': connection_id,
        },
    )

    load_product_model_to_ods = PythonOperator(
        task_id='load_product_model_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/product_model.csv",
            'target': "/data/raw/{{ ds }}/product_model.csv",
            'table_name': 'product_model_ods',
            'connection_id': connection_id,
        },
    )

    load_product_model_product_description_to_ods = PythonOperator(
        task_id='load_product_model_product_description_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/product_model_product_description.csv",
            'target': "/data/raw/{{ ds }}/product_model_product_description.csv",
            'table_name': 'product_model_product_description_ods',
            'connection_id': connection_id,
        },
    )

    load_product_description_to_ods = PythonOperator(
        task_id='load_product_description_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/product_description.csv",
            'target': "/data/raw/{{ ds }}/product_description.csv",
            'table_name': 'product_description_ods',
            'connection_id': connection_id,
        },
    )

    ods_finished = BashOperator(
        task_id='ods_finished',
        bash_command='echo All data have been loaded to ods',
    )

    dw_product_description = PostgresOperator(
        task_id="dw_product_description",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_product_description_sql,
    )
    dw_product_model_product_description = PostgresOperator(
        task_id="dw_product_model_product_description",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_product_model_product_description_sql,
    )
    dw_product_category = PostgresOperator(
        task_id="dw_product_category",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_product_category_sql,
    )
    dw_product_model = PostgresOperator(
        task_id="dw_product_model",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_product_model_sql,
    )
    dw_product = PostgresOperator(
        task_id="dw_product",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_product_sql,
    )
    dw_sales_order_line_item = PostgresOperator(
        task_id="dw_sales_order_line_item",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_sales_order_line_item_sql,
    )
    dw_sales_order = PostgresOperator(
        task_id="dw_sales_order",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_sales_order_sql,
    )
    dw_address = PostgresOperator(
        task_id="dw_address",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_address_sql,
    )
    dw_customer = PostgresOperator(
        task_id="dw_customer",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_customer_sql,
    )
    dw_customer_address = PostgresOperator(
        task_id="dw_customer_address",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_customer_address_sql,
    )

    dw_finished = BashOperator(
        task_id='dw_finished',
        bash_command='echo All data have been transformed from ods to dw',
    )

    dm_city_sales = BashOperator(
        task_id='dm_city_sales',
        bash_command='echo add detail',
    )

    dm_product_sales = BashOperator(
        task_id='dm_product_sales',
        bash_command='echo add detail',
    )

    dag_start >> [load_sales_order_to_ods, load_product_to_ods, load_address_to_ods, load_customer_address_to_ods,
                  load_customer_to_ods, load_product_category_to_ods, load_product_model_to_ods,
                  load_product_model_product_description_to_ods, load_product_description_to_ods] >> ods_finished >> [
        dw_product_description,
        dw_product_model_product_description,
        dw_product_category, dw_product_model, dw_product,
        dw_sales_order_line_item, dw_sales_order, dw_address,
        dw_customer, dw_customer_address] >> dw_finished >> [dm_city_sales, dm_product_sales]
