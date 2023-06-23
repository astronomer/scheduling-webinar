from airflow.decorators import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 6, 1),
    # Runs every Friday at 18:00 to cover the work week (9:00 Monday to 18:00 Friday).
    schedule=CronTriggerTimetable(
        "0 18 * * 5",
        timezone="UTC",
        interval=timedelta(days=4, hours=9),
    ),
    catchup=False,
    tags=["CronTriggerTimetable"],
)
def cron_trigger_timetable_example():
    @task
    def say_hi():
        return "hi!"

    say_hi()


cron_trigger_timetable_example()
