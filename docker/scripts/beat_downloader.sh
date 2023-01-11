#!/bin/bash

exec celery --workdir /opt/services/satellite_celeryapp --config celeryconfig -A tasks worker -B --loglevel=INFO -s /tmp/celerybeat-schedule --pidfile /tmp/celerybeat.pid
