from airflow.decorators import task
from google.cloud import storage, bigquery
import jsonpointer
import requests
import re
from datetime import datetime
import json
from io import StringIO
import logging

CITELINE_CENSORED_FIELDS = {}

@task()
def request_citeline_auth_token(storage_bucket):
    '''
    Authorize with Citeline. Returns a Authorization Token.
    '''
    blob = storage_bucket.blob(f'dags/keys/citeline-credentials.json')
    with blob.open('r') as config_file:
        config = json.load(config_file)
        CitelineUserEmail = config['username']
        CitelinePassword = config['password']
        CitelineAccessKey = config['access_key']
    AuthHeaders = {'Authorization': CitelineAccessKey}
    AuthData = {'grant_type': 'password', 'username': CitelineUserEmail, 'password': CitelinePassword, 'scope': 'customer-api'}
    Response = requests.post('https://identity.pharmaintelligence.informa.com/connect/token', headers=AuthHeaders, data=AuthData).json()
    CitelineAuthToken = f'{Response["token_type"]} {Response["access_token"]}'
    return CitelineAuthToken

@task()
def get_schema(bigquery_client, storage_bucket, dataset_name: str, citeline_auth_token: str):
    '''
    Get Schema request against citeline. Returns Response as Json
    '''
    request_header = {'Accept':'application/json', 'Content-Type':'application/json', 'Authorization': f'{citeline_auth_token}'}
    url = f'https://api.pharmaintelligence.informa.com/v1/search/{dataset_name}/schema'
    response_json = requests.get(url, headers= request_header).json()
    Pointer = response_json['properties']['items']['items']['$ref']
    schema = translate_schema(response_json, Pointer[1:])
    schema_file = StringIO('')
    bigquery_client.schema_to_json(
        schema_list=schema
        ,destination=schema_file
        )
    blob_name = f'data/schema/Citeline/{dataset_name}.json'
    blob = storage_bucket.blob(blob_name)
    blob.upload_from_string(
        data= schema_file.getvalue(),
        content_type='application/json'
    )
    logging.info(f'Saved Schema to: {blob_name}')

    #write censored_fields
    censored_fields = {}
    for key, val in CITELINE_CENSORED_FIELDS.items():
        if key != val: censored_fields[key] = val
    blob_name = f'data/data_stucts/Citeline/{dataset_name}.json'
    blob = storage_bucket.blob(blob_name)
    blob.upload_from_string(
        data= json.dumps(censored_fields, indent=1),
        content_type='application/json'
    )

@task()
def get_data(storage_bucket, dataset_name:str, payload:str, citeline_auth_token: str):
    '''
    Get Data request against citeline. Saves data to file
    '''
    url = f'https://api.pharmaintelligence.informa.com/v1/search/{dataset_name}'
    header = {'Accept':'application/json', 'Content-Type':'application/json', 'Authorization': f'{citeline_auth_token}'}
    next_page = get_data_loop(
        url
        ,header
        ,payload
        ,dataset_name
        ,storage_bucket
        )
    while next_page is not None:
        next_page = get_data_loop(
            next_page
            ,header ##assuming token will be valid for the entire period
            ,payload
            ,dataset_name
            ,storage_bucket
            )

def get_data_loop(url, header, payload, dataset_name, storage_bucket):
    response = requests.post(url, headers= header, data= payload)
    response_json = response.json()
    if response.status_code == 200:
        # write the blob to cloud storage
        blob_name = f'data/bigquery/Citeline/{dataset_name}/{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.json'
        blob = storage_bucket.blob(blob_name)
        blob.upload_from_string(
            data= json.dumps(response_json, indent=1)
            ,content_type='application/json'
        )
        logging.info(f'Saved Data to {blob_name}')

        next_page = None
        if response_json.get('pagination') != None:
            if response_json['pagination'].get('nextPage') != None:
                next_page = response_json['pagination']['nextPage']

    return next_page

def translate_schema(Schema, Pointer: str):
    '''
    Recursively traverse Citeline json schema translating into a Big Query accepted version. Results returned as a list of BigQuery SchemaField Objects.
    '''
    SchemaList = []
    PointedSchema = jsonpointer.resolve_pointer(Schema, Pointer)
    for _ in PointedSchema['properties']:
        FieldType, FieldMode, Pointer = GetFieldTypeAndRef(_, PointedSchema)
        if Pointer != None: 
            pass
            NestedSchema = translate_schema(Schema, Pointer[1:])
            SchemaList.append(bigquery.SchemaField(ReplaceBigQueryCensoredCharatersInString(_, CITELINE_CENSORED_FIELDS), FieldType, FieldMode, fields= NestedSchema)) #''.join([re.sub('[^a-zA-Z0-9 -]', '_', c) for c in _] replaces invalid field names
        else: SchemaList.append(bigquery.SchemaField(ReplaceBigQueryCensoredCharatersInString(_, CITELINE_CENSORED_FIELDS), FieldType, FieldMode))   
    return SchemaList

def ReplaceBigQueryCensoredCharatersInString(InputString: str, AssessedStrings= {}):
    OutputString = AssessedStrings.get('InputString')
    if OutputString == None: 
        OutputString = ''.join([re.sub('[^a-zA-Z0-9 -]', '_', c) for c in InputString])
        AssessedStrings[InputString] = OutputString
    return OutputString

def GetFieldTypeAndRef(FieldName, Schema):
    FieldMode = "NULLABLE"
    Pointer= None
    if 'oneOf' in Schema['properties'][FieldName]: 
        FieldType = TranslateDataType(Schema['properties'][FieldName]['oneOf'][0]['type']) ##assuming constant structure of oneOf
        Pointer = Schema['properties'][FieldName]['oneOf'][1]['$ref']
    else:   
        if 'format' in Schema['properties'][FieldName]: FieldType = TranslateDataType(Schema['properties'][FieldName]['format'])
        else: FieldType = TranslateDataType(Schema['properties'][FieldName]['type'])
        if 'items' in Schema['properties'][FieldName]: 
            FieldMode = "REPEATED"
            if '$ref' in Schema['properties'][FieldName]['items']: Pointer = Schema['properties'][FieldName]['items']['$ref'] 
            elif 'type' in  Schema['properties'][FieldName]['items']: FieldType = TranslateDataType(Schema['properties'][FieldName]['items']['type'])
    #print('FieldName:', FieldName, ', FieldType:', FieldType, ', FieldMode:', FieldMode,', Pointer:', Pointer)
    return FieldType, FieldMode, Pointer

def TranslateDataType(DataType):
    '''
    hacky way to infer dtype/null from listed_dtypes, assumes ['?dtypename', 'null']
    takes the api stated dtype and translates to name used in bq manually entered dict from inspecting api schema json, 
    '''
    DataTypeDict = {
            'string':'STRING',
            'integer':'INTEGER',
            'date-time': 'TIMESTAMP',
            'int32':'INTEGER',
            'int64':'INTEGER',
            'number':'FLOAT',
            'decimal':'FLOAT',
            'array': 'RECORD',
            'null': 'RECORD'
        }
    if isinstance(DataType, list):
        try: DataType.remove('null') # assuming any type array will have two items, one of which is "null"
        except: pass
        DataType = DataType[0]
    RemappedDataType = DataTypeDict.get(DataType, 'STRING') # default to string if dtype key not present
    return RemappedDataType