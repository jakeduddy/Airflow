from datetime import datetime, timedelta
from airflow import DAG
from google.cloud import bigquery
from common.bigquery import create_bigquery_table_backup

BIGQUERY_CLIENT = bigquery.Client(location = 'europe-west2')

default_args = {
    'owner': 'duddyj'
    ,'depends_on_past': False
    ,'start_date': datetime(2022, 1, 2) #sunday
    ,'depends_on_past': False
    ,'email': ['jake.duddy@oncacare.com']
    ,'email_on_failure': True
    ,'email_on_retry': False
    ,'retries': 1
    ,'retry_delay': timedelta(minutes= 5)
}

with DAG(
    'bigquery_citeline_backup'
    ,default_args=default_args
    ,schedule_interval=timedelta(days=14) # every second sunday
    ,catchup=False
    ,tags=['Citeline','BigQuery', 'Backup']
    ) as dag:
    
    for table_name in ['trial', 'organization', 'investigator']:
        create_bigquery_backup = create_bigquery_table_backup(
            bigquery_client=BIGQUERY_CLIENT
            ,dataset_name='Citeline'
            ,table_name=table_name
            )
