from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator
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


def file_path(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path


BUCKET = 'data-bootcamp-terraforms-us'
REGION = "us-central1"
PROJECT_ID = "deliverable3-oscargarciaf"
JAR_PATH = "gs://data-bootcamp-terraforms-us/postgresql-42.3.1.jar"
FILE_NAME = "postgresql-42.3.1.jar"
JAR_URL = "https://jdbc.postgresql.org/download/postgresql-42.3.1.jar"

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": "cluster-c9dc"},
    #"pyspark_job": {"main_python_file_uri": "gs://data-bootcamp-terraforms-us/reviews_job.py", "jar_file_uris": ["file://" + file_path(FILE_NAME)]}    
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
        "properties" : {"spark" : "spark.jars.packages=org.postgresql:postgresql:42.3.1"}
    }
}
CLUSTER_NAME = PROJECT_ID + "_spark_cluster"

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
    dag = dag)

#Task 
task_download_jar = GCSToLocalFilesystemOperator(task_id = "download_jar",
        object_name=FILE_NAME,
        bucket=BUCKET,
        filename=FILE_NAME,
        gcp_conn_id = "google_cloud_default",
        dag = dag
    )

submit_spark = DataprocSubmitJobOperator(task_id="review_spark_job",
                                            job=PYSPARK_JOB,
                                            region=REGION,
                                            project_id=PROJECT_ID,
                                            dag = dag
                                        )

start_dummy = DummyOperator(task_id='start_dummy', dag = dag)
end_dummy = DummyOperator(task_id='end_dummy', dag = dag)


#start_dummy >> task_download_jar >> submit_spark >> end_dummy
start_dummy >> create_cluster >> submit_spark >> delete_cluster >> end_dummy