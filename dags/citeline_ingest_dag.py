from datetime import datetime, timedelta
from airflow import DAG
from google.cloud import storage, bigquery
from common.citeline import(
    request_citeline_auth_token
    ,get_schema
    ,get_data
)
from common.bigquery import trucate_table_in_bigquery

BUCKET_NAME = 'europe-west2-oncacare-airfl-aa522ea7-bucket'
STORAGE_CLIENT = storage.Client()
STORAGE_BUCKET = STORAGE_CLIENT.get_bucket(BUCKET_NAME)
BIGQUERY_CLIENT = bigquery.Client(location = 'europe-west2')

def create_dag(dag_id, schedule_interval, default_args, catchup, tags, dataset_name, payload):
    dag = DAG(
        dag_id
        ,default_args= default_args
        ,schedule_interval=schedule_interval
        ,catchup=catchup
        ,tags=tags
    )
    with dag:
        
        citeline_auth_token = request_citeline_auth_token(
        storage_bucket=STORAGE_BUCKET
        )

        schema = get_schema(
            bigquery_client=BIGQUERY_CLIENT
            ,storage_bucket=STORAGE_BUCKET
            ,dataset_name=dataset_name
            ,citeline_auth_token=citeline_auth_token
            )

        data = get_data(
            storage_bucket=STORAGE_BUCKET
            ,dataset_name=dataset_name
            ,payload=payload
            ,citeline_auth_token=citeline_auth_token
            )
        
        trucate = trucate_table_in_bigquery(
            bigquery_client=BIGQUERY_CLIENT
            ,dataset_name=dataset_name
            )

        citeline_auth_token >> schema >> [data, trucate]
        citeline_auth_token >> data
    
    return dag

citeline_entities = {
    'trial': [0, '{\"pagesize\":\"1000\",\"contains\":{\"value\":\"Oncology*\",\"name\":\"trialTherapeuticAreas.name\"}}']
    ,'investigator': [1, '{\"pagesize\":\"1000\",\"contains\":{\"value\":\"Oncology*\",\"name\":\"diseaseHierarchy\"}}']
    ,'organization': [2, '{\"pagesize\":\"1000\",\"contains\":{\"value\":\"Oncology*\",\"name\":\"diseaseHierarchy\"}}']
    }
for entity, payload in citeline_entities.items():
    dag_id= f'citeline_ingest_{entity}'
    schedule_interval=timedelta(days=14) + timedelta(hours=payload[0]) # every second saturday
    catchup=False
    tags=['Citeline','BigQuery']
    default_args = {
        'owner': 'duddyj'
        ,'depends_on_past': False
        ,'start_date': datetime(2022, 1, 1) #saturaday
        ,'depends_on_past': False
        ,'email': ['jake.duddy@oncacare.com']
        ,'email_on_failure': True
        ,'email_on_retry': False
        ,'retries': 1
        ,'retry_delay': timedelta(minutes= 5)
    }
    entity=entity
    payload=payload[1]

    globals()[dag_id] = create_dag(
        dag_id
        , schedule_interval
        , default_args
        , catchup
        , tags
        , entity
        , payload
        )