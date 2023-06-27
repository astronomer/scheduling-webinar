"""
## This DAG is scheduled on a CRON string

9 4/2 6,7,8 1-11 2-5 = "At minute 9 past every 2nd hour from 4 through 23 
on day-of-month 6, 7, and 8 and on every day-of-week from Tuesday through 
Friday in every month from January through November."

See https://crontab.guru/
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 6, 1),
    schedule="9 4/2 6,7,8 1-11 2-5",
    catchup=False,
    tags=["CRON"],
)
def complex_cron_string_example():
    @task
    def say_hi():
        return "hi"

    say_hi()


complex_cron_string_example()
