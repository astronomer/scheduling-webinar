"""
## This DAG is scheduled on a CRON string
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 6, 1),
    schedule="* * * * *",
    catchup=False,
    tags=["CRON", "webinar"],
)
def simple_cron_string_example():
    @task
    def print_true_start_date(**context):
        return context[
            "dag_run"
        ].start_date  # This is the moment the DAG run actually started

    print_true_start_date()


simple_cron_string_example()
