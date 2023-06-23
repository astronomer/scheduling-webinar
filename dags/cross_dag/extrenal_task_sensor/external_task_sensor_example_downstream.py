"""
## Use the ExternalTaskSensor and ExternalTaskSensorAsync - downstream DAG

This DAG waits for two tasks in the `external_task_sensor_example_upstream` DAG to
complete on two different branches. NOTE: by default both operators are waiting for 
tasks in the upstream DAG run with an IDENTICAL logical date!
You can modify this behavior via the `execution_delta` and `execution_date_fn` parameters.
"""

from airflow.decorators import dag
from pendulum import datetime
from airflow.sensors.external_task import ExternalTaskSensor
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from airflow.operators.empty import EmptyOperator


@dag(
    start_date=datetime(2023, 6, 1),
    schedule="@daily",  # the DAG itself can be run on any type of schedule
    catchup=False,
    tags=["ExternalTaskSensor"],
)
def external_task_sensor_example_downstream():
    waiting_task_branch_one = ExternalTaskSensor(
        task_id="waiting_task_branch_one",
        poke_interval=10,  # Time in seconds that the job should wait in between each try
        timeout=60 * 30,  # Time, in seconds before the task times out and fails.
        external_dag_id="external_task_sensor_example_upstream",
        external_task_id="start_branch_one",
    )

    waiting_task_branch_two_async = ExternalTaskSensorAsync(
        task_id="waiting_task_branch_two_async",
        poke_interval=10,  # Time in seconds that the job should wait in between each try
        timeout=60 * 30,  # Time, in seconds before the task times out and fails.
        external_dag_id="external_task_sensor_example_upstream",
        external_task_id="start_branch_two",
    )

    branch_one_1 = EmptyOperator(task_id="branch_one_1")
    branch_one_2 = EmptyOperator(task_id="branch_one_2")
    branch_one_3 = EmptyOperator(task_id="branch_one_3")

    branch_two_1 = EmptyOperator(task_id="branch_two_1")
    branch_two_2 = EmptyOperator(task_id="branch_two_2")
    branch_two_3 = EmptyOperator(task_id="branch_two_3")

    waiting_task_branch_one >> branch_one_1 >> branch_one_2 >> branch_one_3
    waiting_task_branch_two_async >> branch_two_1 >> branch_two_2 >> branch_two_3

    final_task = EmptyOperator(task_id="final_task")

    [branch_one_3, branch_two_3] >> final_task


external_task_sensor_example_downstream()
