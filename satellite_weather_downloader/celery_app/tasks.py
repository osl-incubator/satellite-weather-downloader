from __future__ import absolute_import
from pathlib import Path

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

BACKFILL_FILE = os.getenv("BACKFILL_FILE")

engine = create_engine(
    f"postgresql://{PSQL_USER}:{PSQL_PASSWORD}@{HOST}:{PORT}/{DBASE}"
)

geocodes = [mun["geocodigo"] for mun in extract_latlons.municipios]

COPE_DF = pd.DataFrame(columns=[
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

@app.task
def reanalysis_download_data(date, date_end=None) -> str:

    connection.connect(UUID, KEY)

    data_file = download_netcdf(
        date=date,
        date_end=date_end
        # data_dir = '/tmp'
    )

    return data_file


@app.task
def reanalysis_create_dataframe(data: str) -> pd.DataFrame:


    total_cities = 5570
    with tqdm.tqdm(total=total_cities, disable=None) as pbar:
        for geocode in geocodes:
            row = netcdf_to_dataframe(data, geocode)
            tmp_df = COPE_DF.merge(row, on=list(COPE_DF.columns), how='outer')
            pbar.update(1)

    df = tmp_df.set_index('date')

    return df


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
    """
    This task is triggered once a day, fetch the last update available
    in Copernicus API and load the data into database.
    """

    today = datetime.now()
    update_delay = timedelta(days=9)
    last_update = (today - update_delay).strftime("%Y-%m-%d")

    data = reanalysis_download_data(last_update)

    cope_df = reanalysis_create_dataframe(data)

    reanalysis_insert_into_db(cope_df)

    reanalysis_delete_netcdf(data)


# Backfill
# --------

@app.task(name='backfill_copernicus_weather')
def backfill_analysis_data():
    """
    This task will read the file `backfill_cope_dates.txt`
    searching for dates to be uploaded into database, where
    each line represents a day to be uploaded with its flag.
    The flag is set to true if the date has been already
    processed into postgres. Celery will check for dates
    every 1 hour.
    Format: "%Y-%m-%d" bool
    Example: 2020-01-01 false
    """

    def gen_next_date() -> str:
        """
        Open backfill file and search for next available date.
        Returns the next date set as False and change it to True.
        """
        file = Path(BACKFILL_FILE)

        with open(file, 'r', encoding='utf-8') as f:
            data = f.readlines()

        for i, line in enumerate(data):
            flag = 'false' #False = not uploaded

            if flag in line:
                date, flag = line.split(' ')
                data[i] = line.replace('false', 'true')

                with open(file, 'w', encoding='utf-8') as f:
                    for line in data:
                        f.write(line)
                
                yield date
    
    try:
        date = next(gen_next_date())

    except StopIteration:
        logging.warning('No date available in `backfill_cope_dates.txt`\nEnding task.')
        return

    data = reanalysis_download_data(date)

    cope_df = reanalysis_create_dataframe(data)

    cope_df = reanalysis_create_dataframe(data)

    reanalysis_insert_into_db(cope_df)

    reanalysis_delete_netcdf(data)

# --------

#Iguazu Falls
