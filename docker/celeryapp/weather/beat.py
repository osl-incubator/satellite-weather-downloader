# Create app celery to start satellite_weather

import json
from pathlib import Path
from celery import Celery
from datetime import timedelta
from celery.signals import worker_ready

app = Celery('beat_weather')

app.config_from_object('satellite.celeryapp.weather.config')


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
