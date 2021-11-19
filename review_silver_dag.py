from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator
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

#name the DAG and configuration
dag = DAG('review_silver_spark',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

BUCKET = 'data-bootcamp-terraforms-us'
REGION = "us-central1"
PROJECT_ID = "deliverable3-oscargarciaf"
CLUSTER_NAME = PROJECT_ID + "-spark-cluster"
JAR_MAVEN = "org.postgresql:postgresql:42.3.1"

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": "gs://data-bootcamp-terraforms-us/reviews_job.py"}    
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "software_config" : {
        "properties" : {"spark:spark.jars.packages" : JAR_MAVEN}
    }
}

SCHEMA_NAME = "silver"
create_schema_query = f"CREATE SCHEMA {SCHEMA_NAME} IF NOT EXISTS"#f"""IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')) 
                      #  BEGIN
                      #      EXEC ('CREATE SCHEMA [{SCHEMA_NAME}]')
                       # END"""


task_create_schema = PostgresOperator(task_id = 'create_schema',
                        sql=create_schema_query,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)


create_cluster = DataprocCreateClusterOperator(task_id="create_cluster",
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    use_if_exists = True,
    delete_on_error = True,
    dag = dag)

delete_cluster = DataprocDeleteClusterOperator(task_id = "delete_cluster",
    project_id = PROJECT_ID,
    region=REGION,
    cluster_name  = CLUSTER_NAME,
    trigger_rule = "all_done",
    dag = dag)

submit_spark = DataprocSubmitJobOperator(task_id="review_spark_job",
                                            job=PYSPARK_JOB,
                                            region=REGION,
                                            project_id=PROJECT_ID,
                                            dag = dag
                                        )


start_dummy = DummyOperator(task_id='start_dummy', dag = dag)
end_dummy = DummyOperator(task_id='end_dummy', dag = dag)
end_dummy_spark_job = DummyOperator(task_id='end_dummy_spark_job', dag = dag)


start_dummy >> task_create_schema >> create_cluster >> submit_spark >> delete_cluster >> end_dummy
submit_spark >> end_dummy_spark_job