# Create app celery to start Downloader_app

from celery import Celery


app = Celery('downloader_app')

app.config_from_object('downloader_app.celeryconfig')

if __name__ == '__main__':
    app.start()

'''
import os
from celery import Celery
from dotenv import load_dotenv

app = Celery(
    "downloader_app",
    broker=os.getenv("CELERY_BROKER_URL"),
    backend=os.getenv("CELERY_BACKEND"),  #
    include=["tasks"],
)

# app.config_from_object('downloader_app.celeryconfig')
app.autodiscover_tasks()


if __name__ == '__main__':
    app.start()
'''
