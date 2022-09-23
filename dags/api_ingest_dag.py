"""API ingest DAG 2022-09-23"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests, psycopg2, json

def fetch_page(url, page=1, size=500):
    r = requests.get(url, params={"page":page,"size":size}, timeout=30)
    r.raise_for_status(); return r.json()

def ingest_api(**ctx):
    base_url = "https://api.example.com/events"
    page = 1
    while True:
        data = fetch_page(base_url, page)
        if not data["results"]: break
        # write to staging table
        page += 1

with DAG("api_ingest", start_date=datetime(2022,10,1), schedule_interval="@hourly",
         default_args={"owner":"de","retries":3,"retry_delay":timedelta(minutes=2)},
         catchup=False) as dag:
    PythonOperator(task_id="fetch_events", python_callable=ingest_api)
