# Create app celery to start satellite_weather_downloader

from celery import Celery


app = Celery('satellite_weather_downloader')

app.config_from_object('satellite_weather_downloader.celeryconfig')

if __name__ == '__main__':
    app.start()
