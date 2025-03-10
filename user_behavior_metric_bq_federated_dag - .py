from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from datetime import datetime
import os

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator

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


dag = DAG('user_behavior_metric_bq_federated',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)


REGION = "us-central1"
PROJECT_ID = "deliverable3-oscargarciaf"
TABLE_NAME = "user_behavior_metric"
DATASET_NAME = "golden"


insert_into_table_query = (           
        f"TRUNCATE TABLE {DATASET_NAME}.{TABLE_NAME}; "
        f"INSERT INTO {DATASET_NAME}.{TABLE_NAME} "
        f"SELECT * FROM EXTERNAL_QUERY(\"projects/deliverable3-oscargarciaf/locations/us-central1/connections/deliverable3-postgres-conn\", "
        f"\"\"\"WITH review_analytics AS (SELECT cid, SUM(positive_review) AS review_score , COUNT(cid) AS review_count  FROM silver.reviews WHERE cid IS NOT NULL GROUP BY cid ), "
        f"user_analytics AS (SELECT customer_id, CAST(SUM(quantity * unit_price) AS DECIMAL(18, 5)) AS amount_spent FROM bronze.user_purchase WHERE customer_id IS NOT NULL GROUP BY customer_id ) "
        f"SELECT COALESCE(ra.cid, ua.customer_id) AS customer_id, COALESCE(amount_spent, 0) AS amount_spent, COALESCE(review_score, 0) AS review_score, COALESCE(review_count, 0) AS review_count, CURRENT_DATE AS insert_date "
        f"FROM review_analytics ra "
        f"FULL JOIN user_analytics ua ON ra.cid = ua.customer_id;\"\"\"); "
    )

create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id = "create_dataset",
            dataset_id = DATASET_NAME,
            project_id = PROJECT_ID,
            location = REGION,
            exists_ok = True,
            dag = dag
        )


create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dataset_id = DATASET_NAME,
    project_id = PROJECT_ID,
    table_id = TABLE_NAME,
    exists_ok = True,
    schema_fields=[
        {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "amount_spent", "type": "DECIMAL", "mode": "REQUIRED"},
        {"name": "review_score", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "review_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "insert_date", "type": "DATE", "mode": "REQUIRED"},
    ],
    dag = dag
)

insert_query_job = BigQueryInsertJobOperator(
    task_id="insert_query_job",
    configuration={
        "query": {
            "query": insert_into_table_query,
            "useLegacySql": False,
        }
    },
    location=REGION,
    project_id = PROJECT_ID,
    dag = dag
)


start_dummy = DummyOperator(task_id='start_dummy', dag = dag)
end_dummy = DummyOperator(task_id='end_dummy', dag = dag)


start_dummy >> create_dataset >> create_table >> insert_query_job >> end_dummy
