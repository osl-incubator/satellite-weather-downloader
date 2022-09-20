from satellite_weather_downloader.celeryapp import app


@app.task
def add(x, y):
    return x + y
