from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
import requests
import json
import boto3
import logging


def get_aws_crendetials():
    conn = BaseHook.get_connection('aws_credentials')
    return conn.login, conn.password

def get_mockaroo_api_key():
    conn = BaseHook.get_connection('mockaroo_api')
    return conn.login

def define_parameters(ti):

    logger.info('Defining parameters....')
    
    #define the parameters
    mockaro_api_key = get_mockaroo_api_key()
    aws_access_key, aws_secret_key = get_aws_crendetials()
    params = {
        "MOCKAROO_API_KEY": mockaro_api_key,
        "AWS_ACCESS_KEY":aws_access_key,
        "AWS_SECRET_KEY":aws_secret_key,
        "BASE_URL" :"https://api.mockaroo.com",
        "LOCAL_STORAGE":"./mockaroo_data",
        "SUBFOLDER":"ingestion_data",
        "BUCKET_NAME" : "ecommerce-ingestion-data-pipeline",
        "REGION_NAME" : "us-east-1"
    }

    ti.xcom_push(key='params', value=params)
    logger.info('Parameters defined....')

def fetch_mockaroo_data(schema_name, ti, count=100):
    logger.info('Fetching data from Mockaroo....')
    params = ti.xcom_pull(key='params', task_ids='define_parameters')
    if not params:
        logger.error('Parameters not found, ensure that the define_parameters task completed successfully')
        raise ValueError('Missing parameters in XCom')
    MOCKAROO_API_KEY = params['MOCKAROO_API_KEY']
    BASE_URL = params['BASE_URL']
    url = f"{BASE_URL}/{schema_name}.json"
    query_params = {
        "key":MOCKAROO_API_KEY,
        "count":count
    }

    retries = 3
    while retries > 0:
        try:
            response = requests.get(url, params=query_params, timeout=10)
            response.raise_for_status()
            logger.info(f'Data fetched successfully from Mockaroo: {response.json()}')
            return response.json()
        except requests.exceptions.RequestException as e:
            retries -= 1
            logger.error(f'Error fetching data from Mockaroo: {e}. Retries left {retries}')
            if retries == 0:
                raise

def create_s3_bucket(ti):
    logger.info('Creating S3 bucket....')
    params = ti.xcom_pull(key='params', task_ids='define_parameters')
    if not params:
        logger.error('Parameters not found, ensure that the define_parameters task completed successfully')
        raise ValueError('Missing parameters in XCom')
    AWS_ACCESS_KEY = params['AWS_ACCESS_KEY']
    AWS_SECRET_KEY = params['AWS_SECRET_KEY']
    BUCKET_NAME = params['BUCKET_NAME']
    SUBFOLDER = params['SUBFOLDER']
    REGION_NAME = params['REGION_NAME']

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=REGION_NAME)

    try:
        if not any(bucket['Name'] == BUCKET_NAME for bucket in s3.list_buckets()['Buckets']):
            s3.create_bucket(Bucket=BUCKET_NAME)
            logger.info(f'S3 bucket {BUCKET_NAME} created successfully')
        else:
            logger.info(f'S3 bucket {BUCKET_NAME} already exists')
        s3.put_object(Bucket=BUCKET_NAME, Key=SUBFOLDER)
    except Exception as e:
        logger.error(f'Error creating S3 bucket: {e}')
        raise

def save_to_s3(file_path, s3_key, ti):

    logger.info('Saving data to S3....')
    params = ti.xcom_pull(key='params', task_ids='define_parameters')
    if not params:
        logger.error('Parameters not found, ensure that the define_parameters task completed successfully')
        raise ValueError('Missing parameters in XCom')
    AWS_ACCESS_KEY = params['AWS_ACCESS_KEY']
    AWS_SECRET_KEY = params['AWS_SECRET_KEY']
    BUCKET_NAME = params['BUCKET_NAME']
    SUBFOLDER = params['SUBFOLDER']

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    
    try:
        s3.uploadfileobj(open(file_path, 'rb'), BUCKET_NAME, s3_key)
        logger.info(f'Data saved to S3 bucket {BUCKET_NAME} successfully')
    except Exception as e:
        logger.error(f'Error saving data to S3: {e}')
        raise
def ingest_data(ti):
    logger.info('Ingesting data....')
    params = ti.xcom_pull(key='params', task_ids='define_parameters')
    if not params:
        logger.error('Parameters not found, ensure that the define_parameters task completed successfully')
        raise ValueError('Missing parameters in XCom')
    LOCAL_STORAGE = params['LOCAL_STORAGE']
    BUCKET_NAME = params['BUCKET_NAME']
    SUBFOLDER = params['SUBFOLDER']
    REGION_NAME = params['REGION_NAME']
    schemas = {"products":"product_schema", 
               "customers":"customer_schema", 
               "orders":"order_schema", 
               "order_items":"order_item_schema",
               "product_reviews":"review_schema",}
    
    os.makedirs(LOCAL_STORAGE, exist_ok=True)
    for schema_name, schema_details in schemas.items():
        data = fetch_mockaroo_data(schema_details, ti)
        filename = f"{schema_name}.json"
        file_path = os.path.join(LOCAL_STORAGE, filename)
        #save locally
        with open(file_path, 'w') as f:
            json.dump(data, f)
            f.close()
        logger.info(f'Data saved to {file_path}')

        save_to_s3(file_path,ti, f"{SUBFOLDER}/{filename}")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner':'airflow',
    'email_on_failure':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
}

dag = DAG('INGESTION_PIPELINE',
          default_args=default_args,
          description='A pipeline to ingest data from Mockaroo and save to S3',
          schedule_interval=timedelta(minutes=5),
          start_date=datetime(2024, 1, 1),
          catchup=False,
)
with dag:
    defilne_parameters = PythonOperator(
        task_id='define_parameters',
        python_callable=define_parameters,
        provide_context=True,
        dag=dag,
    )
    create_s3_bucket = PythonOperator(
        task_id='create_s3_bucket',
        python_callable=create_s3_bucket,
        provide_context=True,
        dag=dag,

    )
    ingest_data = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data,
        provide_context=True,
        dag=dag,
    )

    #set dependencies
    defilne_parameters >> create_s3_bucket >> ingest_data