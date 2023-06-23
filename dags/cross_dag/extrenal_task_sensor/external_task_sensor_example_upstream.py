"""
## Use the ExternalTaskSensor and ExternalTaskSensorAsync - upstream DAG

This is a helper DAG. The external_task_sensor_example_downstream DAG contains two
tasks waiting for tasks in this DAG to complete. NOTE: by default the
ExternalTaskSensor(Async) operator needs parity of the execution date! 
You can modify this behavior via the `execution_delta` and `execution_date_fn` parameters.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import time

@dag(
    start_date=datetime(2023, 6, 1),
    schedule="@daily",  # the DAG itself can be run on any type of schedule
    catchup=False,
    tags=["ExternalTaskSensor"],
)
def external_task_sensor_example_upstream():
    @task
    def wait_10_s():
        time.sleep(10)
        return "Good morning!"

    @task
    def start_branch_one():
        return "Hi"

    @task
    def wait_30_s():
        time.sleep(30)
        return "Good morning!"

    @task
    def start_branch_two():
        return "Hi"

    wait_10_s() >> start_branch_one()
    wait_30_s() >> start_branch_two()


external_task_sensor_example_upstream()