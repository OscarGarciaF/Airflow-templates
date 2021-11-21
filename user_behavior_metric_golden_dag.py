from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from datetime import datetime


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


dag = DAG('review_silver_spark',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

BUCKET = 'data-bootcamp-terraforms-us'
REGION = "us-central1"
PROJECT_ID = "deliverable3-oscargarciaf"

SCHEMA_NAME = "golden"
TABLE_NAME = "user_behavior_metric"

create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME} ;"

create_table_query = f"""CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (    
                            customer_id INTEGER,
                            amount_spent DECIMAL(18, 5),
                            positive_review INTEGER,
                            review_count INTEGER,
                            insert_date DATE;"""
                            
create_insert_into_table = f"""TRUNCATE TABLE {SCHEMA_NAME}.{TABLE_NAME};
                        INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
                        SELECT u.customer_id, CAST(SUM(u.quantity * u.unit_price) AS DECIMAL(18, 5)), SUM(r.positive_review), COUNT(r.cid), CURRENT_DATE                      
                        FROM silver.reviews r
                        JOIN bronze.user_purchase u ON r.cid = u.customer_id
                        GROUP BY u.customer_id;
                        """

task_create_schema = PostgresOperator(task_id = 'create_schema',
                        sql=create_schema_query,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)

task_create_table = PostgresOperator(task_id = 'create_table',
                        sql=create_table_query,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)

task_insert_into_table = PostgresOperator(task_id = 'insert_into_table',
                        sql=create_insert_into_table,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)



start_dummy = DummyOperator(task_id='start_dummy', dag = dag)
end_dummy = DummyOperator(task_id='end_dummy', dag = dag)


start_dummy >> task_create_schema >> task_create_table >> task_insert_into_table >> end_dummy
