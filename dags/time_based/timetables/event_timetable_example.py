"""
## DAG running on specific dates

'restrict_to_events' flag can be used to force manual runs of the DAG to 
use the time of the most recent (or very first) event for the data interval, 
otherwise manual runs will run with a data_interval_start and data_interval_end 
equal to the time at which the manual run was begun. 
"""


from airflow.decorators import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable
from pendulum import datetime


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=EventsTimetable(
        event_dates=[
            datetime(2023, 8, 1),
            datetime(2022, 12, 24, 12),
            datetime(2022, 12, 25),
            datetime(2022, 12, 26),
        ],
        description="Upcoming Swiss holidays",
        restrict_to_events=False,
    ),
    catchup=False,
    tags=["EventTimetable"],
)
def event_timetable_example():
    @task
    def relax():
        return "Relax!"

    relax()


event_timetable_example()
