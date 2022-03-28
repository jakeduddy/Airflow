from airflow.decorators import task
from google.cloud import bigquery, storage
from datetime import datetime, timedelta
import json
import logging

@task()
def trucate_table_in_bigquery(bigquery_client, dataset_name: str):
    bigquery_client.query(f'TRUNCATE TABLE oncasearch.citeline.{dataset_name}')

@task()
def list_blobs_in_storage(storage_client, storage_bucket, prefix: str):
    prefix = 'data/bigquery/'
    blobs = storage_client.list_blobs(
        storage_bucket
        , prefix=prefix
        )
    blob_lst = []
    for blob in blobs: 
        blob_lst.append(blob.name)
    return blob_lst

@task()
def write_data_to_bigquery(bigquery_client, storage_bucket, blobs: list):
    '''
    Accepts list of JSON. Runs through each object, converting to New Line Delimited and then writing data to BigQuery Table.
    '''
    for blob in blobs:
        dataset_name, table_name, file_name  = blob.replace('data/bigquery/', '').split('/')
        
        # load schema and create config
        schema_file_name = f'data/schema/{dataset_name}/{table_name}.json'
        schema_blob = storage_bucket.blob(schema_file_name)
        with schema_blob.open('r') as schema_file:
            schema = bigquery_client.schema_from_json(schema_file)
        logging.info(f'Loaded Schema: {schema_file_name}')
        job_config = bigquery.LoadJobConfig( 
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON 
            ,schema=schema
            ,write_disposition ='WRITE_APPEND'
            )

        # load citeline_censored_fields
        data_struct_file_name = f'data/data_stucts/Citeline/{table_name}.json'
        data_struct_blob = storage_bucket.blob(data_struct_file_name)
        with data_struct_blob.open('r') as data_struct_file:
            citeline_censored_fields = json.load(data_struct_file)
        
        # load data file
        data_file_name = blob
        data_blob = storage_bucket.blob(data_file_name)
        with data_blob.open('r') as data_file:
            data = json.load(data_file)
        logging.info(f'Loaded Data: {data_file_name}')

        # process data
        Blob = ''
        for record in data['items']:
            for field in citeline_censored_fields:
                if record.get(field) != None: record[citeline_censored_fields.get(field)] = record.pop(field)
            Blob += json.dumps(record) + ',\n' #New Line Delimited
        BlobJson = json.loads('[\n' + Blob[:-2] + '\n]')
        logging.info(f'Processed Data: {data_file_name}')

        # # write data to bigquery
        load_job = bigquery_client.load_table_from_json(
            BlobJson
            , destination=f'{dataset_name}.{table_name}'
            , job_config=job_config
            ) # TODO: Google Suggests writing as a file, is this worth it? Might need to define json new line in job config. BigQueryClient.load_table_from_file(io.StringIO(BlobJson), ...)
        load_job.result() ## wait for update
        logging.info(f'Loaded Data to BigQuery: {data_file_name}')

        #delete blob
        storage_bucket.delete_blob(blob)

@task()
def create_bigquery_table_backup(bigquery_client, dataset_name:str, table_name: str, expiry_period = 14):
    datetime_now = datetime.now()
    datetime_now_str = datetime_now.strftime("%Y_%m_%d_%H_%M_%S")
    datetime_expiry = datetime_now + timedelta(days= expiry_period)
    datetime_expiry_str = f'{datetime_expiry.strftime("%Y-%m-%d %H:%M:%S")}.00-00:00'
    query = f'''
        CREATE SNAPSHOT TABLE
        {dataset_name}.{table_name}_{datetime_now_str}
        CLONE {dataset_name}.{table_name}
        OPTIONS(expiration_timestamp = TIMESTAMP "{datetime_expiry_str}")
    '''
    logging.info(query)
    bigquery_client.query(query)