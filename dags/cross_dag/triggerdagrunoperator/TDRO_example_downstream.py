"""
## Use the TriggerDagRunOperator - downstream DAG

This DAG is a helper and supposed to be started by running the 
TDRO_example_upstream DAG. It will wait 20s and then print the param `dog_sound`.
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
import time


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=None,
    catchup=False,
    params={"dog_sound": Param("silence", type="string")},
    tags=["TriggerDagRunOperator"],
)
def TDRO_example_downstream():
    @task
    def wait_20s(**context):
        time.sleep(20)
        print(context["params"]["dog_sound"])

    wait_20s()


TDRO_example_downstream()
