#!/bin/bash

exec celery --workdir /opt/services/satellite_weather_downloader/celery_app --config celeryconfig -A tasks worker -B --loglevel=INFO
