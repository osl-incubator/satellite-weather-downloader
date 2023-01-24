from __future__ import absolute_import

from beat import app
from celery import Celery, states
from celery.exceptions import Ignore
from celeryapp.delay_controller import update_task_schedule

import os
import calendar
import pandas as pd
from pathlib import Path
from loguru import logger
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import find_dotenv, load_dotenv

from satellite_downloader.utils import connection
from satellite_downloader import extract_reanalysis as ex

load_dotenv(find_dotenv())

PSQL_USER = os.getenv('POSTGRES_USER')
PSQL_PASSWORD = os.getenv('POSTGRES_PASSWORD')
HOST = os.getenv('POSTGRES_HOST')
PORT = os.getenv('POSTGRES_PORT')
DBASE = os.getenv('POSTGRES_DATABASE')
UUID = os.getenv('API_UUID')
KEY = os.getenv('API_KEY')
DATA_DIR = os.getenv('COPER_DATA_DIR_CONT')
STATUS_TABLE = 'cope_download_status'
SCHEMA = 'weather'

engine = create_engine(
    f'postgresql://{PSQL_USER}:{PSQL_PASSWORD}@{HOST}:{PORT}/{DBASE}'
)


@app.task(bind=True, name='extract_br_netcdf_monthly', retry_kwargs={'max_retries': 5})
def download_br_netcdf_monthly(self) -> None:
    """
    This task will be responsible for downloading every data in copernicus
    API for Brasil. It will runs continuously until there is no more months
    to fetch, then self-update the delay to run monthly.
    """
    with engine.connect() as conn:
        try:
            ini_date, end_date = _produce_next_month_to_update(
                conn, SCHEMA, STATUS_TABLE
            )
        except ValueError as e:
            # When finish fetching previously months, self update
            # delay to run only at first day of each month
            update_task_schedule(
                task='extract_br_netcdf_monthly',
                minute=0,
                hour=0,
                day_of_month=15,
            )
            logger.error(e)
            logger.warning(
                'Task `extract_br_netcdf_monthly` delay updated'
                ' to run every day 15 of month'
            )
            self.update_state(
                state = states.FAILURE,
                meta = 'No date found to fetch in status table.'
            )
            raise Ignore()

    try:
        with engine.connect() as conn:
            conn.execute(
                f'UPDATE {SCHEMA}.{STATUS_TABLE}'
                ' SET downloading = true'
                f" WHERE date = '{end_date}'"
            )

        file_path = ex.download_br_netcdf(
            date=ini_date, date_end=end_date, data_dir=DATA_DIR
        )

        if not file_path:
            raise Exception('File was not downloaded, exiting task.')

        with engine.connect() as conn:
            conn.execute(
                f'UPDATE {SCHEMA}.{STATUS_TABLE}'
                f" SET path = '{file_path}'"
                f" WHERE date = '{end_date}'"
            )

        logger.info(
            f'Data for {ini_date} until {end_date} downloaded at {file_path}'
        )

    except Exception as e:
        logger.error(e)
        raise e

    finally:
        with engine.connect() as conn:
            conn.execute(
                f'UPDATE {SCHEMA}.{STATUS_TABLE}'
                ' SET downloading = false'
                f" WHERE date = '{end_date}'"
            )


@app.task(name='scan_for_missing_dates')
def insert_missing_dates() -> None:
    """
    This task will look up for missing dates in database,
    if any missing date is find, update table with them.
    """
    table = 'cope_download_status'
    schema = 'weather'

    db_dates = pd.read_sql(
        sql=('SELECT date' f' FROM {schema}.{table}'),
        con=engine,
    )

    cur_date = datetime.now().date()
    cur_date_range = pd.date_range(
        start=datetime(2000, 1, 1).date(), end=cur_date, freq='M'
    )

    df = pd.DataFrame()
    df['date'] = pd.DataFrame(cur_date_range, columns=['date'])['date'].apply(
        lambda d: d.date()
    )

    missing_dates = df[~df['date'].isin(db_dates['date'])]

    if not missing_dates.empty:
        logger.warning(f'Missing dates found, inserting:\n{missing_dates}')
        missing_dates.to_sql(
            name=table, con=engine, schema=schema, if_exists='append'
        )
    else:
        logger.info(f'Table {table} up-to-date')


@app.task(name='initialize_satellite_download_db')
def initialize_db() -> None:
    """
    This task runs at every container startup. Will create the table
    structure if it doesn't exist and populate it with dates if no data
    is present. Will do nothing if there is any data in the table.
    """
    # Store cdsapi credentials:
    connection.connect(uid=UUID, key=KEY)
    # Runs at container Startup
    schema = 'weather'
    table = 'cope_download_status'
    sql = _sql_create_table(schema, table)
    with engine.connect() as conn:
        conn.execute(sql)
        cur = conn.execute(f'SELECT MAX(date) FROM {schema}.{table}')
        last_update_date = cur.fetchone()[0]

    if not last_update_date:
        _populate_table(engine, schema, table)
        logger.warning(f'{table} not found, populating table with dates.')
    else:
        logger.info(
            f'{table} up and running. Last update on: {last_update_date}'
        )


@app.task(name='create_copernicus_data_tables')
def initialize_db() -> None:
    """
    Runs at Beat startup. Create tables if they don't exist.
    Municipal tables will have `copernicus_` prefix added before
    it's city name.
    Tables:
        copernicus_brasil: Nacional level data table.
        copernicus_foz_do_iguacu: Foz do Iguaçu data.
    """
    municipal_tables = [
        'foz_do_iguacu',
    ]

    with engine.connect() as conn:
        conn.execute(_create_national_table_sql())
        logger.info('Table copernicus_brasil initialized')
        for mun_name in municipal_tables:
            conn.execute(_create_municipal_table_sql(mun_name))
            logger.info(f'Table copernicus_{mun_name} initialized')


# ---
# create_copernicus_data_tables task:
def _create_national_table_sql() -> str:
    sql = (
        'CREATE TABLE IF NOT EXISTS weather.copernicus_brasil ('
        'index SERIAL UNIQUE PRIMARY KEY,'
        'time DATE NOT NULL,'
        'geocodigo BIGINT NOT NULL,'
        'temp_min FLOAT(23) NOT NULL,'
        'temp_med FLOAT(23) NOT NULL,'
        'temp_max FLOAT(23) NOT NULL,'
        'precip_min FLOAT(23) NOT NULL,'
        'precip_med FLOAT(23) NOT NULL,'
        'precip_max FLOAT(23) NOT NULL,'
        'pressao_min FLOAT(23) NOT NULL,'
        'pressao_med FLOAT(23) NOT NULL,'
        'pressao_max FLOAT(23) NOT NULL,'
        'umid_min FLOAT(23) NOT NULL,'
        'umid_med FLOAT(23) NOT NULL,'
        'umid_max FLOAT(23) NOT NULL);'
    )
    return sql


def _create_municipal_table_sql(name: str) -> str:
    sql = (
        f'CREATE TABLE IF NOT EXISTS weather.copernicus_{name} ('
        'index SERIAL UNIQUE PRIMARY KEY,'
        'time TIMESTAMP WITHOUT TIME ZONE NOT NULL,'
        'geocodigo BIGINT NOT NULL,'
        'temp FLOAT(23) NOT NULL,'
        'precip FLOAT(23) NOT NULL,'
        'pressao FLOAT(23) NOT NULL,'
        'umid FLOAT(23) NOT NULL);'
    )
    return sql


# ---
# initialize_satellite_db task:
def _sql_create_table(schema: str, table: str) -> str:
    sql = (
        f'CREATE TABLE IF NOT EXISTS {schema}.{table} ('
        ' index SERIAL PRIMARY KEY UNIQUE,'
        ' date DATE NOT NULL,'
        ' path TEXT DEFAULT NULL,'
        ' downloading BOOLEAN NOT NULL DEFAULT false,'
        ' task_brasil_status TEXT DEFAULT NULL,'
        ' task_foz_status TEXT DEFAULT NULL'
        ')'
    )
    return sql


def _populate_table(conn, schema: str, table: str) -> None:
    today = datetime.now().date()
    months = pd.date_range(start='1/1/2000', end=today, freq='M')
    df = pd.DataFrame()
    df['date'] = pd.DataFrame(months, columns=['date'])['date'].apply(
        lambda d: d.date()
    )

    df.to_sql(name=table, schema=schema, con=conn, if_exists='append')


# ---
# extract_br_netcdf_monthly task:
def _produce_next_month_to_update(conn, schema: str, table: str) -> tuple:
    """
    This method will produce the next day to be updated.
    Rules for next available month to download:
    1. If last update isn't last month, return last month range to be
       inserted in the table
    2. If there is a null `path` and is not `downloading`,
       return the max date
    3. Has to be lesser than the current month and bigger than 1999/12/01
    Returns:
        tuple (datetime.date, datetime.date): First and last day of month.
    """
    cur_date = datetime.now().date()
    _, date_to_update = _last_month_range(cur_date)
    _table = f'{schema}.{table}'

    # Rule 1
    cur = conn.execute(f'SELECT MAX(date) FROM {_table}')
    last_update_date = cur.fetchone()[0]

    # DB is empty; Runs only at first initialization
    if not last_update_date:
        raise RuntimeError('No data found on DB')

    elif last_update_date != date_to_update:
        return _month_range(date_to_update)

    # Rule 2
    # This case will ensure there are no dates left to update,
    # prioritizing the most recent date
    cur = conn.execute(
        f'SELECT MAX(date) FROM {_table} WHERE'
        ' path IS NULL AND'
        ' NOT downloading'
    )
    next_avail_date = cur.fetchone()[0]

    if next_avail_date:
        month_first, month_last = _month_range(next_avail_date)
        # Rule 3
        if month_first < datetime(2000, 1, 1).date():
            raise ValueError('Date limit reached')
        else:
            return month_first, month_last
    else:
        raise ValueError('Could not generate next date to download')


def _month_range(date: datetime.date) -> tuple:
    """
    This returns the date range for the current month of
    the datetime provided.
    Usage:
        from datetime import datetime; import calendar
        date = datetime.date(2023, 1, 14)
        last_month_range(date)
    Output:
        (datetime.date(2023, 1, 1), datetime.date(2023, 1, 31))
    """
    first = datetime(date.year, date.month, 1).date()
    last = datetime(
        date.year, date.month, calendar.monthrange(date.year, date.month)[1]
    ).date()
    return first, last


def _last_month_range(date: datetime.date) -> tuple:
    """
    This returns the date range for the last month of
    the datetime provided.
    Usage:
        from datetime import datetime; import calendar
        date = datetime.date(2023, 1, 1)
        last_month_range(date)
    Output:
        (datetime.date(2022, 12, 1), datetime.date(2022, 12, 31))
    """
    if date.month == 1:
        ini_date = datetime(date.year - 1, 12, 1)
    else:
        ini_date = datetime(date.year, date.month - 1, 1)

    end_date = datetime(
        ini_date.year,
        ini_date.month,
        calendar.monthrange(ini_date.year, ini_date.month)[1],
    )

    return ini_date.date(), end_date.date()


# ---
#
def scan_and_remove_inconsistent_data() -> None:
    # each month has 5570 values per day (copernicus_brasil)
    # each month has 8 values per day (copernicus_foz_do_iguacu)
    # if month in incomplete:
    # - drop all rows within this month range
    # - delete local file
    # - clean path from table

    with engine.connect() as conn:
        date_ranges = _get_inconsistent_months(conn)
        if any(date_ranges):
            try:
                _delete_entries_for(date_ranges, conn)
            except Exception as e:
                logger.error(e)
                raise e
        else:
            logger.info('[SCAN] No inconsistent date were found')


def _delete_entries_for(date_ranges: list[tuple], conn) -> None:
    for date_range in date_ranges:
        ini_m, end_m = date_range
        cur = conn.execute(
            'SELECT path, task_brasil_status, task_foz_status'
            f' FROM weather.{STATUS_TABLE}'
            f" WHERE date = '{end_m}'"
        )
        path = cur.fetchone()[0]
        conn.execute(
            'DELETE FROM weather.copernicus_brasil'
            f' WHERE time BETWEEN {ini_m} AND {end_m}'
        )
        conn.execute(
            'DELETE FROM weather.copernicus_foz_do_iguacu'
            f' WHERE time BETWEEN {ini_m} AND {end_m}'
        )
        conn.execute(
            f'UPDATE weather.{STATUS_TABLE} SET'
            ' path = NULL,'
            ' task_brasil_status = NULL'
            f" WHERE date = '{end_m}'"
        )
        Path(path).unlink()
        logger.warning(f'[SCAN] All data entries for {path} was deleted.')


def _get_inconsistent_months(conn) -> list:
    date_ranges = []

    cur = conn.execute(
        ' SELECT'
        '  res.ini_m,'
        '  res.end_m'
        ' FROM ('
        '    SELECT'
        "    DATE_TRUNC('month', time) AS ini_m,"
        "    (DATE_TRUNC('month', time) + interval '1 month - 1 day')::date "
        'AS end_m,'
        '    count(*) AS tot'
        '  FROM weather.copernicus_brasil'
        "  GROUP BY DATE_TRUNC('month', time)) AS res"
        " WHERE to_char(((res.end_m - res.ini_m) * 5570), 'DD')::integer != res.tot;"
    )
    br_dates = cur.fetchall()
    date_ranges.extend(br_dates)

    cur = conn.execute(
        ' SELECT'
        '  res.ini_m,'
        '  res.end_m'
        ' FROM ('
        '    SELECT'
        "    DATE_TRUNC('month', time) AS ini_m,"
        "    (DATE_TRUNC('month', time) + interval '1 month - 1 day')::date "
        'AS end_m,'
        '    count(*) AS tot'
        '  FROM weather.copernicus_foz_do_iguacu'
        "  GROUP BY DATE_TRUNC('month', time)) AS res"
        " WHERE to_char(((res.end_m - res.ini_m) * 8), 'DD')::integer != res.tot;"
    )
    foz_dates = cur.fetchall()
    for date in foz_dates:
        if date not in date_ranges:
            date_ranges.append(date)

    return date_ranges
