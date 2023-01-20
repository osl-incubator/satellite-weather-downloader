#!/bin/bash

exec celery \
    --workdir /opt/services/satellite/celeryapp/downloader \
    --config beat \
    -A tasks worker -B \
    -s /tmp/celerybeat-schedule \
    --pidfile /tmp/celerybeat.pid
    --loglevel=INFO \

chown 
