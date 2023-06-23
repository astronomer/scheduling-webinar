"""
## This DAG is scheduled on a timedelta object

After starting this DAG it will run every 30 seconds.
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=timedelta(seconds=30),
    catchup=False,
    tags=["timedelta"],
)
def timedelta_example():
    @task
    def say_hi():
        return "hi"

    say_hi()


timedelta_example()
