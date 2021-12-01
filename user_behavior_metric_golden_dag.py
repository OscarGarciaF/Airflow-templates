from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from datetime import datetime
import os


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


dag = DAG('user_behavior_metric_golden',
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
                            review_score INTEGER,
                            review_count INTEGER,
                            insert_date DATE);"""
                            
create_insert_into_table = f"""WITH review_analytics AS (SELECT cid, SUM(positive_review) AS review_score , COUNT(cid) AS review_count  FROM silver.reviews
                            WHERE cid IS NOT NULL
                            GROUP BY cid ),
                            user_analytics AS (SELECT customer_id, CAST(SUM(quantity * unit_price) AS DECIMAL(18, 5)) AS amount_spent FROM bronze.user_purchase
                            WHERE customer_id IS NOT NULL
                            GROUP BY customer_id )
                            SELECT COALESCE(ua.customer_id, ra.cid) AS customer_id, COALESCE(amount_spent, 0) AS amount_spent, COALESCE(review_score, 0) AS review_score, COALESCE(review_count, 0) AS review_count, CURRENT_DATE AS insert_date                      
                            FROM review_analytics ra
                            FULL JOIN user_analytics ua ON ra.cid = ua.customer_id;"""
                        

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
