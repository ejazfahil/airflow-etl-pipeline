"""Slack alerts 2022-12-06"""
import requests

def slack_fail_alert(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exec_date = context["execution_date"]
    msg = f":red_circle: *DAG Failed*\nDAG: `{dag_id}`\nTask: `{task_id}`\nDate: {exec_date:%Y-%m-%d}"
    requests.post("https://hooks.slack.com/services/XXX/YYY/ZZZ",
                  json={"text": msg}, timeout=10)
