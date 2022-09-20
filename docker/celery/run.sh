#!/bin/bash

exec celery -A satellite_weather_downloader.celeryapp worker -l INFO
