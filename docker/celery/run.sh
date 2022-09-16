#!/bin/bash

exec celery -A downloader_app.celeryapp worker -l INFO
