from airflow.decorators import dag, task
from pendulum import datetime
from astronomer.providers.core.sensors.filesystem import FileSensor
import os


@dag(
    start_date=datetime(2023, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["FileSensor"],
)
def filesensor_example():
    wait_for_file_async = FileSensor(
        task_id="wait_for_file_async",
        filepath="include/my_text_files/*.txt",
        timeout=60 * 30,
        poke_interval=10,
        fs_conn_id="local_file_default",
    )

    @task
    def read_text_files():
        directory = "include/my_text_files/"
        files = os.listdir(directory)

        for file in files:
            file_path = os.path.join(directory, file)
            if file.endswith(".txt"):
                with open(file_path, "r") as f:
                    content = f.read()
                    print(f"Contents of {file}:")
                    print(content)

    wait_for_file_async >> read_text_files()


filesensor_example()
