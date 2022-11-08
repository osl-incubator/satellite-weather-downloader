# Create app celery to start satellite_weather_downloader

from celery import Celery

app = Celery("satellite_weather_downloader")

app.config_from_object("satellite_weather_downloader.celery_app.celeryconfig")
app.autodiscover_tasks()


@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')


if __name__ == "__main__":
    worker = app.Worker(
        include=['tasks']
    )
    worker.start()
