"""
## Use Datasets - upstream DAG

This DAG runs as soon as the Dataset file://include/my_data.csv has received an update
and returns the count of happy butterflies.
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd

@dag(
    start_date=datetime(2023, 6, 1),
    schedule=[Dataset("file://include/my_data.csv")],
    catchup=False,
    tags=["Dataset", "webinar"],
)
def dataset_schedule_downstream():
    @task
    def get_total_happy_butterflies():
        file_path = "include/my_data.csv"
        df = pd.read_csv(file_path)
        print(df)

        total_happy_count = df.loc[:, "Count"].sum()

        return total_happy_count

    get_total_happy_butterflies()


dataset_schedule_downstream()
