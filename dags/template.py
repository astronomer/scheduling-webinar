from airflow.decorators import dag, task
from pendulum import datetime


@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False, tags=[""])
def example_1():
    @task
    def my_task():
        return "hi"

    my_task()


example_1()
