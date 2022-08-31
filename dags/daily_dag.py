import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator

from utils.load_csv_to_postgres import load_csv_to_postgres

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
