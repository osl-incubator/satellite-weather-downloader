from __future__ import absolute_import
from .beat import app

import os
import calendar
from loguru import logger
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import find_dotenv, load_dotenv
from satellite_downloader import extract_reanalysis as ex
from celeryapp.delay_controller import update_task_schedule

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


@app.task(name='extract_br_netcdf_monthly', retry_kwargs={'max_retries': 5})
def download_netcdf_monthly() -> None:
    """
    This task will be responsible for downloading every data in copernicus
    API for Brasil. It will runs continously until there is no more months
    to fetch, then self-update the delay to run monthly.
    """
    schema = 'weather'
    table = 'copernicus_downloader'
    with engine.connect() as conn:
        try:
            next_date = _produce_next_date(conn, schema, table)
        except ValueError as e:
            # When finish fetching previously months, self update
            # delay to run only at first day of each month
            update_task_schedule(
                task = 'extract_br_netcdf_monthly',
                minute = 1,
                hour = 1,
                day_of_month = 1
            )
            raise e

    with engine.connect() as conn:
        conn.execute(
            f'INSERT INTO {schema}.{table} (date, downloading)'
            f' VALUES ({next_date}, true)'
        )
    
    ini_date, end_date = calc_last_month_range(next_date)

    try:
        file_path = ex.download_br_netcdf(ini_date, end_date)
        with engine.connect() as conn:
            conn.execute(
                f'UPDATE {schema}.{table}'
                 ' SET (path, downloading)'
                f' VALUES ({file_path}, false)'
                f" WHERE date = '{ini_date}'"
            )

    except Exception as e:
        logger.error(e)
        raise e

    finally:
        with engine.connect() as conn:
            conn.execute(
                f'UPDATE {schema}.{table}'
                 ' SET (downloading)'
                 ' VALUES (false)'
                f" WHERE date = '{ini_date}'"
            )


@app.task(name='initialize_satellite_download_db')
def initialize_db() -> None:
    schema = ''
    table = ''
    sql = _sql_create_table(schema, table)
    with engine.connect() as conn:
        conn.execute(sql)
    logger.info(f'{table} on {schema} created.')


# ---
# initialize_satellite_db task:
def _sql_create_table(schema:str, table:str) -> str:
    sql = (
        f'CREATE TABLE IF NOT EXISTS {schema}.{table} ('
        ' date DATETIME PRIMARY KEY UNIQUE NOT NULL,'
        ' path TEXT DEFAULT NULL,'
        ' downloading BOOLEAN NOT NULL DEFAULT false,'
    ')')
    return sql


# ---
# extract_br_netcdf_monthly task:
def _produce_next_date(conn, schema: str, table: str) -> datetime:
    """
    Rules for next available date to download:
    1. If last update isn't last month, return last month
    2. If there is a null `path` and is not `downloading`, return this date
    3. If the `path` is null, but it's `downloading`, return the previously month
    4. Has to be lesser than the current month and bigger than 1999/12/01
    5. Cannot, in any ways, have a `path`
    6. Has to return the first day of the month
    """
    cur_date = datetime.now()
    cur_date_to_update, _ = calc_last_month_range(cur_date)
    _table = f'{schema}.{table}'

    # Rule 1
    with conn:
        cur = conn.execute(
            f'SELECT MAX(date) FROM {_table}'
        )
        last_update_date = cur.fetchone()
    
    # DB is empty; Runs only at first initialization
    if not last_update_date:
        return cur_date_to_update

    elif last_update_date[0] != cur_date_to_update:
        return cur_date_to_update

    # Rule 2
    # This case will ensure there are no dates left to update,
    # prioritizing the most recent date
    with conn:
        cur = conn.execute(
            f'SELECT MAX(date) FROM {_table} WHERE'
            ' path IS NULL AND'
            ' NOT downloading'
        )
        next_avail_date = cur.fetchone()
    
    if next_avail_date:
        return next_avail_date[0]

    # Rule 3
    with conn:
        cur = conn.execute(
            f'SELECT MIN(date) FROM {_table} WHERE'
            ' path IS NULL AND'
            ' downloading IS true'
        )
        is_downloading = cur.fetchone()

    if is_downloading:
        previous_date, _ = calc_last_month_range(is_downloading[0])
        # Rule 4
        if previous_date < datetime(2000, 1, 1):
            raise ValueError('Date limit reached')
        return previous_date

    else:
        raise ValueError('Could not generate next date to download')


def calc_last_month_range(date: datetime) -> tuple:
    """
    This returns the date range for the last month of
    the datetime provided.
    Usage:
        from datetime import datetime; import calendar
        date = datetime(2023, 1, 1)
        calc_last_month_range(date)
    Output:
        (datetime(2022, 12, 1, 0, 0), datetime(2022, 12, 31, 0, 0))
    """
    if date.month == 1:
        ini_date = datetime(date.year - 1, 12, 1)
    else:
        ini_date = datetime(date.year, date.month - 1, 1)
    
    end_date = datetime(
        ini_date.year,
        ini_date.month,
        calendar.monthrange(
            ini_date.year, 
            ini_date.month
        )[1]
    )

    return ini_date, end_date
