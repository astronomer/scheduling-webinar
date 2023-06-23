from airflow.decorators import dag, task
from pendulum import datetime
from custom_timetable import UnevenIntervalsTimetable
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=UnevenIntervalsTimetable(),
    catchup=False,
    tags=["custom timetable"],
)
def custom_timetable_example():
    @task
    def print_runs_in_july(**context):
        return context["dag"].get_run_dates(
            start_date=datetime(2023, 7, 1), end_date=datetime(2023, 7, 10)
        )

    print_runs_in_july()


custom_timetable_example()
