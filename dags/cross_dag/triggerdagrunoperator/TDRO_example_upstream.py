"""
## Use the TriggerDagRunOperator - upstream DAG

This DAG uses the TriggerDagRunOperator to start a downstream DAG with conf and
then waits for its completion.
"""

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=None,
    catchup=False,
    tags=["TriggerDagRunOperator"],
)
def TDRO_example_upstream():
    
    tdro = TriggerDagRunOperator(
        task_id="tdro",
        trigger_dag_id="TDRO_example_downstream",
        conf={"dog_sound": "woof"},
        wait_for_completion=True,
        poke_interval=5,
        deferrable=True,
    )

    @task
    def celebrate():
        print("woof has been successful!")

    tdro >> celebrate()


TDRO_example_upstream()
