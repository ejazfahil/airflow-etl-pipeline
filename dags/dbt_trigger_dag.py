"""dbt trigger DAG 2022-12-06"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("dbt_run", start_date=datetime(2022,11,1), schedule_interval="0 6 * * *",
         default_args={"owner":"de"}, catchup=False) as dag:
    run   = BashOperator(task_id="dbt_run",  bash_command="cd /dbt && dbt run  --target prod")
    test  = BashOperator(task_id="dbt_test", bash_command="cd /dbt && dbt test --target prod")
    run >> test
