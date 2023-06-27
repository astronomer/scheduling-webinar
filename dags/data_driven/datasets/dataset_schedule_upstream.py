"""
## Use Datasets - upstream DAG

This DAG produces to a Dataset called "file://include/my_data.csv".
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd
import os


@dag(
    start_date=datetime(2023, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["Dataset", "webinar"],
)
def dataset_schedule_upstream():
    @task
    def retrieve_butterfly_counts_data():
        return {
            "Butterfly": ["Danaus plexippus", "Papilio machaon", "Vanessa cardui"],
            "Count": [2, 4, 5],
        }

    @task(outlets=Dataset("file://include/my_data.csv"))
    def update_butterfly_counts(new_data):
        df = pd.DataFrame(new_data)
        file_path = "include/my_data.csv"

        if os.path.exists(file_path):
            # Append dataframe to existing CSV
            existing_df = pd.read_csv(file_path)
            updated_df = pd.concat([existing_df, df], ignore_index=True)
            updated_df.to_csv(file_path, index=False)
        else:
            # Create new CSV from the dataframe
            df.to_csv(file_path, index=False)

    update_butterfly_counts(retrieve_butterfly_counts_data())


dataset_schedule_upstream()
