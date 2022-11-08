#!/bin/bash

exec celery --workdir satellite_weather_downloader/celery_app --config celeryconfig -A tasks worker -B --loglevel=DEBUG
