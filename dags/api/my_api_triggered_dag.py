"""
## Empty DAG to be triggered via the Airflow REST API.

You can trigger DAGs via the API (https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)
with the `/api/v1/dags/{dag_id}/dagRuns` endpoint. 

Trigger this DAG using (make sure your DAG is unpaused!):

curl -X POST 'http://localhost:8080/api/v1/dags/my_api_triggered_dag/dagRuns' \
-H 'Content-Type: application/json' \
--user "admin:admin" \
-d '{
    "conf": {"my_name":"Jani"}
}'

"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=None,
    catchup=False,
    params={"my_name": Param("Unknown", type="string")},
    tags=["API"],
)
def my_api_triggered_dag():
    @task
    def say_hi(**context):
        return f"hi {context['params']['my_name']}"

    say_hi()


my_api_triggered_dag()
