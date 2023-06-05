#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  4 22:27:09 2023

@author: mohitkataria
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from twitter_etl import run_twitter_etl

default_args = {
    'owner': 'airflow',
    'depends_pn_past': True,
    'start_date': datetime(2023, 5, 5),
    'email': 'mokataria@gmail.com',
    'email_on_faliure':False,
    'email_on_success': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description = 'twitter ETL dag'
    )

run_etl = PythonOperator(
    task_id = 'omplete_twitter_etl',
    python_callable = run_twitter_etl,
    dag = DAG,
    
    )

run_etl