#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  3 20:43:44 2023

@author: mohitkataria
"""

import awswrangler as wr
import pandas as pd 
import os
import urllib.parse

#Temporary hard-coded AWS settings; i.e to be set as OS variable in lambda
os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']

def lambda_handler(event, context):
    #Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        
        #Creating df from content
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))
        
        #Extract required columns:
        df_Step_1 = pd.json_normalize(df_raw['items'])
        
        #Write to s3
        wr_response = wr.s3.to_parquet(
            df=df_Step_1,
            path=os_input_s3_cleansed_layer,
            dataset = True,
            database = os_input_glue_catalog_db_name,
            table = os_input_glue_catalog_table_name,
            mode = os_input_write_data_operation
            )
        return wr_response
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exists and your bucket is in the same region as this function.'.format(key, bucket))
        raise e