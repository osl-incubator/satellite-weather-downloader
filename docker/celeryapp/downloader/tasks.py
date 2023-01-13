from __future__ import absolute_import

import calendar
import os
from datetime import datetime, timedelta
from types import NoneType

from dotenv import find_dotenv, load_dotenv
from satellite_downloader import extract_reanalysis as ex

# import tqdm
# import calendar
# import subprocess
# import pandas as pd
# from loguru import logger
from sqlalchemy import create_engine

from .beat import app, update_task_delay

load_dotenv(find_dotenv())

PSQL_USER = os.getenv('POSTGRES_USER')
PSQL_PASSWORD = os.getenv('POSTGRES_PASSWORD')
HOST = os.getenv('PSQL_HOST')
PORT = os.getenv('POSTGRES_PORT')
DBASE = os.getenv('POSTGRES_DATABASE')
UUID = os.getenv('API_UUID')
KEY = os.getenv('API_KEY')


engine = create_engine(
    f'postgresql://{PSQL_USER}:{PSQL_PASSWORD}@{HOST}:{PORT}/{DBASE}'
)


def _get_last_available_date(conn, schema: str, table: str) -> datetime:
    sql = f'SELECT MIN(date) FROM {schema}.{table}'
    with conn:
        cur = conn.execute(sql)
        res = cur.fetchone()
        if not res:  # Will run only at system initialization
            current = datetime.now()
            return datetime(current.year, current.month, 1)
        return res[0]


def _calc_last_month_range(date: datetime) -> tuple(datetime, datetime):
    if date.month == 1:
        ini_date = datetime(date.year - 1, 12, 1)
    else:
        ini_date = datetime(date.year, date.month - 1, 1)

    end_date = datetime(
        ini_date.year,
        ini_date.month,
        calendar.monthrange(ini_date.year, ini_date.month)[1],
    )

    return ini_date, end_date


def _download_monthly_file(ini_date: datetime, end_date: datetime) -> str:
    return ex.download_br_netcdf(ini_date, end_date)


def _create_file_query(
    schema: str, table: str, date: datetime, file_path: str
) -> str:
    sql = f'INSERT INTO {schema}.{table} (date, path, in_progress, done) VALUES ({date}, {file_path}, false, false)'
    return sql


@app.task
def download_netcdf_monthly():
    schema = ''  # TODO: define a psql table and schema
    table = ''
    with engine.connect() as conn:
        next_date = _get_last_available_date(conn, schema, table)

    ini_date, end_date = _calc_last_month_range(next_date)

    try:
        file = _download_monthly_file(ini_date, end_date)
    except Exception as e:
        ...  # Retry task & log

    if file:
        sql = _create_file_query(schema, table, ini_date, file)
        with engine.connect() as conn:
            conn.execute(sql)
            ...  # log
    else:
        ...  # Retry task & log


@app.task
def initialize_db() -> None:
    ...  # Runs at startup
    # CREATE TABLE IF NOT EXISTS (
    # date: date (primeiro dia de cada mes)
    # path: str (path do arquivo netcdf)
    # in progress: bool
    # done: bool)


# @app.task
# def update_db_with_task_dates(task):
#     today = datetime.now()
#     db = BackfillDB()

#     match task:
#         case 'fetch_brasil_weather':
#             next_update_date = today - timedelta(days=9)   # safety margin
#             date = next_update_date.strftime('%Y-%m-%d')
#             db.update_table(date=date, table='brasil')

#         case 'fetch_foz_weather':
#             cur = db.conn.cursor()
#             cur.execute('SELECT MAX(date) FROM foz')
#             last_update = cur.fetchone()[0]
#             last_update = datetime.fromisoformat(last_update).strftime(
#                 '%Y-%m-%d'
#             )

#             first_monthday = today - timedelta(days=today.day)
#             last_day = first_monthday.replace(
#                 day=calendar.monthrange(
#                     first_monthday.year, first_monthday.month
#                 )[1]
#             ).strftime('%Y-%m-%d')

#             if last_update != last_day:
#                 db.update_table(date=last_day, table='foz')


# @app.task
# def reanalysis_download_data(date, date_end=None) -> str:

#     connection.connect(UUID, KEY)

#     data_file = download_br_netcdf(
#         date=date,
#         date_end=date_end
#         # data_dir = '/tmp'
#     )

#     return data_file


# @app.task
# def reanalysis_create_dataframe(data: str, task: str) -> pd.DataFrame:
#     match task:
#         case 'fetch_brasil_weather':
#             total_cities = 5570
#             with tqdm.tqdm(total=total_cities, disable=None) as pbar:
#                 for geocode in geocodes:
#                     row = netcdf_to_dataframe(data, geocode)
#                     tmp_df = COPE_DF.merge(
#                         row, on=list(COPE_DF.columns), how='outer'
#                     )
#                     pbar.update(1)
#                     df = tmp_df.set_index('date')

#         case 'fetch_foz_weather':
#             df = netcdf_to_dataframe(data, 4108304)

#     return df


# @app.task
# def reanalysis_insert_into_db(df: pd.DataFrame, tablename: str):

#     df.to_sql(
#         tablename,
#         engine,
#         schema='Municipio',
#         if_exists='append',
#     )

#     logger.info(f'{len(df)} rows updated on "Municipios".{tablename}')


# @app.task
# def reanalysis_delete_netcdf(file: str):

#     subprocess.run(['rm', '-rf', file])

#     logger.info(f'{file.split("/")[-1]} removed.')


# @app.task
# def fetch_weather(task: str, tablename: str):
#     match task:
#         case 'fetch_brasil_weather':
#             db = BackfillHandler('brasil')
#             date = db.next_date()
#             ini_date = datetime.fromisoformat(date).strftime('%Y-%m-%d')
#             end_date = None

#             if not date:
#                 logger.warning(
#                     'None task was found to fetch.'
#                     ' Setting task delay to a day'
#                 )
#                 update_task_delay('fetch_brasil_weather', 1440)
#                 return None

#         case 'fetch_foz_weather':
#             db = BackfillHandler('foz')
#             date = db.next_date()
#             e_date = datetime.fromisoformat(date)
#             end_date = e_date.strftime('%Y-%m-%d')
#             ini_date = datetime(
#                 year=e_date.year, month=e_date.month, day=1
#             ).strftime('%Y-%m-%d')

#             if not date:
#                 logger.warning(
#                     'None task was found to fetch.'
#                     ' Setting task delay to a month'
#                 )
#                 update_task_delay('fetch_foz_weather', 43830)
#                 return None

#     try:
#         data = reanalysis_download_data(ini_date, end_date)
#         cope_df = reanalysis_create_dataframe(data, task)
#         reanalysis_insert_into_db(cope_df, tablename)
#         reanalysis_delete_netcdf(data)
#         db.set_task_done(date)

#     except Exception as e:
#         db.set_task_unfinished(date)
#         logger.error(e)


# @app.task(name='initialize_backfill_db')
# def initialize_backfill_db():
#     db = BackfillDB()
#     db.populate_tables()


# @app.task(
#     name='fetch_brasil_weather',
# )
# def reanalysis_fetch_brasil_data():
#     fetch_weather('fetch_brasil_weather', 'cope_brasil_weather')


# @app.task(name='fetch_foz_weather')
# def backfill_analysis_data():
#     fetch_weather('fetch_foz_weather', 'cope_foz_weather')


# @app.task(name='update_brasil_fetch_date')
# def update_br_date():
#     update_db_with_task_dates('fetch_brasil_weather')


# @app.task(name='update_foz_fetch_date')
# def update_foz_date():
#     update_db_with_task_dates('fetch_foz_weather')
