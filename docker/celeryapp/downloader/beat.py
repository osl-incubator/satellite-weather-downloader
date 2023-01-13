# Create app celery to start satellite_downloader

import json
from datetime import timedelta
from pathlib import Path
from celery.schedules import crontab
from loguru import logger

from celery import Celery
from celery.signals import worker_ready

app = Celery('beat_downloader')

app.config_from_object('satellite.celeryapp.downloader.config')

delay_file = Path(__file__).parent.parent / 'delay_controller.json'


"""
Delay Controllers
-----------------
Responsible for communicating with `delay_controller.json`,
 setting or getting the task delay in it. Ideally will be used
 to give control to the task define its own delay, depending if 
 rather there is data to fetch or not. 
"""


def get_task_delay(task: str) -> crontab:
    with open(delay_file, 'r') as d:
        tasks = json.load(d)

    for t in tasks:
        if t['task'] == task:
            cron = t['crontab']

    return crontab(
        minute = cron['minute'],
        hour = cron['hour'],
        day_of_week = cron['day_of_week'],
        day_of_month = cron['day_of_month'],
        month_of_year = cron['month_of_year']
    )


def update_task_delay(task, **kwargs) -> None:
    cron = dict(
        minute='*', 
        hour='*', 
        day_of_week='*',
        day_of_month='*', 
        month_of_year='*'
    )
    
    for kw in kwargs:
        if kw in cron.keys():
            cron[kw] = kwargs[kw]

    with open(delay_file, 'r') as d:
        tasks = json.load(d)

    for t in tasks:
        if t['task'] == task:
            t['crontab'] = cron

    logger.info(f'Task {task} has a new delay: {cron}')

    with open(delay_file, 'w') as d:
        json.dump(tasks, d, indent=2)


# """
# -----------------
# """


# app.conf.beat_schedule = {
#     'fetch-brasil-weather': {
#         'task': 'fetch_brasil_weather',
#         'schedule': get_task_delay('fetch_brasil_weather'),
#     },
#     'fetch-foz-weather': {
#         'task': 'fetch_foz_weather',
#         'schedule': get_task_delay('fetch_foz_weather'),
#     },
#     'add-date-to-fetch-br': {
#         'task': 'update_brasil_fetch_date',
#         'schedule': timedelta(days=1),
#     },
#     'add-date-to-fetch-foz': {
#         'task': 'update_foz_fetch_date',
#         'schedule': timedelta(days=14),
#     },
# }


# # Send signal to run at worker startup
# @worker_ready.connect
# def at_start(sender, **kwargs):
#     """Run tasks at startup"""
#     with sender.app.connection() as conn:
#         sender.app.send_task('initialize_backfill_db', connection=conn)
