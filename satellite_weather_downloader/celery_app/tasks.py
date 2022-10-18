from turtle import update
from datetime import datetime, timedelta
from satellite_weather_downloader.celery_app.celeryapp import app
from satellite_weather_downloader.extract_reanalysis import download_netcdf, netcdf_to_dataframe
from satellite_weather_downloader.utils.extract_latlons import municipios
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine
import logging
import os

load_dotenv(find_dotenv())

PSQL_USER=os.getenv('POSTGRES_USER')
PSQL_PASSWORD=os.getenv('POSTGRES_PASSWORD')
HOST=os.getenv('PSQL_HOST')
PORT=os.getenv('PSQL_PORT')
DBASE=os.getenv('POSTGRES_DATABASE')


engine = create_engine(
        f"postgresql://{PSQL_USER}:{PSQL_PASSWORD}@{HOST}:{PORT}/{DBASE}"
    )

geocodes = [mun['geocodigo'] for mun in municipios]


# runs daily
@app.task
def download_data_reanalysis(date):
    
    data_file = download_netcdf(
        date = date
        # date_end = daily? weekly? monthly?
        # data_dir = '/tmp'
    )

    return data_file


@app.task
def insert_reanalysis_into_db(data, geocode):

    df = netcdf_to_dataframe(
        data,
        geocode
    )

    df.to_sql(
        'weather_copernicus',
        engine,
        schema = 'Municipio',
        if_exists= 'append',
    )


@app.task
def fetch_reanalysis_data_daily():

    today = datetime.now()
    update_delay = timedelta(days=7)
    last_update = (today - update_delay).strftime("%Y-%m-%d")

    data = download_data_reanalysis(last_update)

    for geocode in geocodes:
        insert_reanalysis_into_db(data, geocode)
        logging.info(f'{geocode} updated {last_update}')

fetch_reanalysis_data_daily()