from __future__ import absolute_import

import pandas as pd
import subprocess
import logging
import tqdm
import os

from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import find_dotenv, load_dotenv
from satellite_weather_downloader.celery_app.celeryapp import app
from satellite_weather_downloader.utils import extract_latlons, connection
from satellite_weather_downloader.extract_reanalysis import (
    download_netcdf,
    netcdf_to_dataframe,
)

load_dotenv(find_dotenv())

PSQL_USER = os.getenv("POSTGRES_USER")
PSQL_PASSWORD = os.getenv("POSTGRES_PASSWORD")
HOST = os.getenv("PSQL_HOST")
PORT = os.getenv("POSTGRES_PORT")
DBASE = os.getenv("POSTGRES_DATABASE")
UUID = os.getenv("API_UUID")
KEY = os.getenv("API_KEY")

engine = create_engine(
    f"postgresql://{PSQL_USER}:{PSQL_PASSWORD}@{HOST}:{PORT}/{DBASE}"
)

geocodes = [mun["geocodigo"] for mun in extract_latlons.municipios]


@app.task
def reanalysis_download_data(date):

    connection.connect(UUID, KEY)

    data_file = download_netcdf(
        date=date
        # date_end = daily? weekly? monthly?
        # data_dir = '/tmp'
    )

    return data_file


@app.task
def reanalysis_insert_into_db(df: pd.DataFrame):

    df.to_sql(
        "weather_copernicus",
        engine,
        schema="Municipio",
        if_exists="append",
    )

    logging.info(f'{len(df)} rows updated on "Municipios".weather_copernicus')

@app.task
def reanalysis_delete_netcdf(file: str):

    subprocess.run([
        'rm',
        '-rf',
        file
    ])

    logging.info(f'{file.split("/")[-1]} removed.')


@app.task(name='fetch_copernicus_weather')
def reanalysis_fetch_data_daily():

    today = datetime.now()
    update_delay = timedelta(days=7)
    last_update = (today - update_delay).strftime("%Y-%m-%d")

    data = reanalysis_download_data(last_update)

    cope_df = pd.DataFrame(columns=[
        'date',
        'geocodigo',
        'temp_min',
        'temp_med',
        'temp_max',
        'precip_min',
        'precip_med',
        'precip_max',
        'pressao_min',
        'pressao_med',
        'pressao_max',
        'umid_min',
        'umid_med',
        'umid_max',  
    ])

    total_cities = 5570
    with tqdm.tqdm(total=total_cities) as pbar:
        for geocode in geocodes:
            row = netcdf_to_dataframe(data, geocode)
            cope_df = cope_df.merge(row, on=list(cope_df.columns), how='outer')
            pbar.update(1)

    cope_df = cope_df.set_index('date')

    reanalysis_insert_into_db(cope_df)

    reanalysis_delete_netcdf(data)
