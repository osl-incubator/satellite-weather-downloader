import json
from pathlib import Path

from celery.schedules import crontab
from loguru import logger

delay_file = Path(__file__).parent / 'delays.json'

"""
Delay Controllers
-----------------
Responsible for communicating with `delays.json`,
 setting or getting the task delay in it. Ideally will be used
 to give control to the task define its own delay, depending if 
 rather there is data to fetch or not. The controllers use the
 cron format: https://crontab.guru/
"""


def get_task_schedule(task: str) -> crontab:
    with open(delay_file, 'r') as d:
        tasks = json.load(d)

    for t in tasks:
        if t['task'] == task:
            cron = t['crontab']

    return crontab(
        minute=cron['minute'],
        hour=cron['hour'],
        day_of_week=cron['day_of_week'],
        day_of_month=cron['day_of_month'],
        month_of_year=cron['month_of_year'],
    )


def update_task_schedule(task, **kwargs) -> None:
    cron = dict(
        minute='*',
        hour='*',
        day_of_week='*',
        day_of_month='*',
        month_of_year='*',
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
