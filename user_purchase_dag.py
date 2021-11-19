import airflow
import os
import psycopg2
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime

"""
Load CSV > Postgres in GCP Cloud SQL Instance
"""


#default arguments 

default_args = {
    'owner': 'oscar.garcia',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 1),
    'email': ['garcia.oscar1729@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
}

#name the DAG and configuration
dag = DAG('insert_user_purchase_postgres',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

def file_path(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path


FILE_NAME = "user_purchase.csv"
TABLE_NAME = "user_purchase"
BUCKET = 'data-bootcamp-terraforms-us'
SCHEMA_NAME = "bronze"
CREATE_SCHEMA_QUERY = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME} ;"
COPY_QUERY = f""" COPY {SCHEMA_NAME}.{TABLE_NAME} from stdin WITH CSV HEADER DELIMITER ',' ESCAPE '"' """
CREATE_TABLE_QUERY = f"""CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (    
                            invoice_number VARCHAR(255),
                            stock_code VARCHAR(255),
                            detail VARCHAR(255),
                            quantity INTEGER,
                            invoice_date TIMESTAMP,
                            unit_price NUMERIC,
                            customer_id INTEGER,
                            country VARCHAR(255));
                            """

def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor()    
    # CSV loading to table
    with open(FILE_NAME, "r") as f:
        next(f)
        #curr.copy_from(f, TABLE_NAME, sep=",")
        curr.copy_expert(COPY_QUERY, file = f)
        get_postgres_conn.commit()
        curr.close()

def delete_file():
    os.remove(FILE_NAME)

#Task 

start_dummy = DummyOperator(task_id='start_dummy', dag = dag)
end_dummy = DummyOperator(task_id='end_dummy', dag = dag)

task_create_schema = PostgresOperator(task_id = 'create_schema',
                        sql=CREATE_SCHEMA_QUERY,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)

task_create_table = PostgresOperator(task_id = 'create_table',
                        sql=CREATE_TABLE_QUERY,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)

task_download_file = GCSToLocalFilesystemOperator(task_id="download_file",
        object_name=FILE_NAME,
        bucket=BUCKET,
        filename=FILE_NAME,
        gcp_conn_id = "google_cloud_default",
        dag = dag
    )

task_load_csv = PythonOperator(task_id='csv_to_database',
                   provide_context=True,
                   python_callable=csv_to_postgres,
                   dag=dag)

task_delete_file = PythonOperator(task_id='delete_file',
                   provide_context=True,
                   python_callable=delete_file,
                   dag=dag)



start_dummy >> task_create_schema >> task_create_table >> task_download_file >> task_load_csv >> task_delete_file >> end_dummy