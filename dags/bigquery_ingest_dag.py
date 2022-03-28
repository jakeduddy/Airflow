from datetime import datetime, timedelta
from airflow import DAG
from google.cloud import storage, bigquery
from airflow.contrib.sensors.gcs_sensor  import GCSUploadSessionCompleteSensor
from common.bigquery import (
    write_data_to_bigquery
    ,list_blobs_in_storage
    )

BUCKET_NAME = 'europe-west2-oncacare-airfl-aa522ea7-bucket' 
STORAGE_CLIENT = storage.Client()
STORAGE_BUCKET = STORAGE_CLIENT.get_bucket(BUCKET_NAME)
BIGQUERY_CLIENT = bigquery.Client(location = 'europe-west2')

default_args = {
    'owner': 'duddyj'
    ,'depends_on_past': False
    ,'start_date': datetime(2022, 1, 1)
    ,'depends_on_past': False
    ,'email': ['jake.duddy@oncacare.com']
    ,'email_on_failure': True
    ,'email_on_retry': False
    ,'retries': 1
    ,'retry_delay': timedelta(minutes= 5)
}

with DAG(
    'bigquery_ingest'
    ,default_args=default_args
    ,schedule_interval=timedelta(hours = 1)
    ,catchup=False
    ,tags=['BigQuery', 'Ingest']
    ) as dag:
    
    sense_file = GCSUploadSessionCompleteSensor(
        task_id='sense_file'
        ,bucket=BUCKET_NAME
        ,prefix='data/bigquery/'
        ,inactivity_period=60
        ,min_objects=1
        ,previous_objects=0
        )

    list_blobs = list_blobs_in_storage(
        storage_client=STORAGE_CLIENT
        ,storage_bucket=STORAGE_BUCKET
        ,prefix='data/bigquery/'
    )

    load_big_query = write_data_to_bigquery(
        bigquery_client=BIGQUERY_CLIENT
        ,storage_bucket=STORAGE_BUCKET
        ,blobs=list_blobs
    )

    sense_file >> list_blobs >> load_big_query 