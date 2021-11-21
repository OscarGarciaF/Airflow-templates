from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from datetime import datetime
import os


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
    'retries': 0,
    'retry_delay': timedelta(seconds=3),
}


SELECT_QUERY="""SELECT u.customer_id AS customer_id, SUM(u.quantity * u.unit_price) AS amount_spent, SUM(r.positive_review) AS positive_review, COUNT(r.cid) AS review_count, NOW() AS insert_date
                FROM silver.reviews r
                JOIN bronze.user_purchase u ON r.cid = u.customer_id
                GROUP BY u.customer_id;
                """

#name the DAG and configuration
dag = DAG('review_silver_spark',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

BUCKET = 'data-bootcamp-terraforms-us'
REGION = "us-central1"
PROJECT_ID = "deliverable3-oscargarciaf"

SCHEMA_NAME = "silver"
create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME} ;"


task_create_schema = PostgresOperator(task_id = 'create_schema',
                        sql=create_schema_query,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)



start_dummy = DummyOperator(task_id='start_dummy', dag = dag)
end_dummy = DummyOperator(task_id='end_dummy', dag = dag)


start_dummy >> task_create_schema >> end_dummy
