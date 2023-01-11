# Create app celery to start satellite_weather_downloader

import json
from pathlib import Path
from celery import Celery
from datetime import timedelta
from celery.signals import worker_ready

app = Celery('satellite_weather_downloader')

app.config_from_object('satellite_weather_downloader.celery_app.celeryconfig')

delay_file = Path(__file__).parent / 'delay_controller.json'


"""
Delay Controllers
-----------------
Responsible for communicating with `delay_controller.json`,
 setting or getting the task delay in it. Ideally will be used
 to give control to the task define its own delay, depending if 
 rather there is data to fetch or not. 
 @Warning All delays in the file must be in minutes. 
"""


def get_task_delay(task):
    # WARNING: Every delay in `delay_controller.json` must be in minutes
    with open(delay_file, 'r') as d:
        delays = json.load(d)

    delay = [d['delay'] for d in delays if d['task'] == task].pop()
    return timedelta(minutes=int(delay))


def update_task_delay(task, minutes):
    with open(delay_file, 'r') as d:
        delays = json.load(d)

    for tsk in delays:
        if tsk['task'] == task:
            tsk['delay'] = minutes

    with open(delay_file, 'w') as d:
        json.dump(delays, d)


"""
-----------------
"""


app.conf.beat_schedule = {
    'fetch-brasil-weather': {
        'task': 'fetch_brasil_weather',
        'schedule': get_task_delay('fetch_brasil_weather'),
    },
    'fetch-foz-weather': {
        'task': 'fetch_foz_weather',
        'schedule': get_task_delay('fetch_foz_weather'),
    },
    'add-date-to-fetch-br': {
        'task': 'update_brasil_fetch_date',
        'schedule': timedelta(days=1),
    },
    'add-date-to-fetch-foz': {
        'task': 'update_foz_fetch_date',
        'schedule': timedelta(days=14),
    },
}


# Send signal to run at worker startup
@worker_ready.connect
def at_start(sender, **kwargs):
    """Run tasks at startup"""
    with sender.app.connection() as conn:
        sender.app.send_task('initialize_backfill_db', connection=conn)
