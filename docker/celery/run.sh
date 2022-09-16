#!/bin/bash

# celery -A downloader_app worker -l INFO
exec celery -A downloader_app.celeryapp worker -l INFO

# python /opt/services/setup.py develop
# exec celery -A downloader_app.celeryapp worker -l info --concurrency=4
