"""DAG tests 2023-03-15"""
from airflow.models import DagBag

def test_no_import_errors():
    bag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(bag.import_errors) == 0, str(bag.import_errors)

def test_dag_count():
    bag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(bag.dags) >= 3

def test_csv_ingest_tasks():
    bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = bag.get_dag("csv_ingest")
    assert dag is not None
    assert "ingest_sales" in dag.task_ids
