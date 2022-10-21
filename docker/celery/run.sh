#!/bin/bash

exec celery -A satellite_weather_downloader.celery_app.celeryapp beat -l DEBUG
