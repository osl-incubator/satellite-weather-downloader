#!/bin/bash

exec celery \
    --workdir /opt/services/satellite/celeryapp/weather \
    --config beat \
    -A tasks worker -Q downloader.extract -B \
    -s /tmp/celerybeat-schedule \
    --pidfile /tmp/celerybeat.pid \
    --loglevel=INFO
