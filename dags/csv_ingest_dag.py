"""CSV ingest DAG 2022-09-13"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd, psycopg2

DEFAULT_ARGS = {"owner":"de","retries":2,"retry_delay":timedelta(minutes=5)}

def ingest(filepath, table, **ctx):
    df = pd.read_csv(filepath)
    df["loaded_at"] = datetime.utcnow()
    conn = psycopg2.connect(host="localhost",dbname="warehouse",user="analyst",password="secret")
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(f"INSERT INTO {table} VALUES %s ON CONFLICT DO NOTHING", (tuple(row),))
    conn.commit(); conn.close()

with DAG("csv_ingest", start_date=datetime(2022,9,1), schedule_interval="@daily",
         default_args=DEFAULT_ARGS, catchup=False) as dag:
    PythonOperator(task_id="ingest_sales", python_callable=ingest,
                   op_kwargs={"filepath":"/data/sales.csv","table":"raw_sales"})
