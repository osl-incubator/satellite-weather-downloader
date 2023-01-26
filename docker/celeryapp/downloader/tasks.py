from __future__ import absolute_import

from beat import app
from celery import states
from celery.exceptions import Ignore
from celeryapp.delay_controller import update_task_schedule

import os
import calendar
import pandas as pd
from loguru import logger
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import find_dotenv, load_dotenv

from satellite_downloader import connection
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


@app.task(bind=True, name='extract_br_netcdf_monthly', retry_kwargs={'max_retries': 0})
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
            date=ini_date.strftime('%Y-%m-%d'), 
            date_end=end_date.strftime('%Y-%m-%d'), 
            data_dir=DATA_DIR
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
def initialize_data_tables() -> None:
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


@app.task(name='remove_inconsistent_data')
def scan_and_remove_inconsistent_data() -> None:
    # each month has 5570 values per day (copernicus_brasil)
    # each month has 8 values per day (copernicus_foz_do_iguacu)
    # if month in incomplete:
    # - drop all rows within this month range
    # - clean status from status table
    with engine.connect() as conn:
        _scan_and_delete_entries_for('fetch_copernicus_brasil', conn)
        _scan_and_delete_entries_for('fetch_copernicus_foz', conn)


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

    # DB is empty
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
# remove_inconsistent_data task
def _scan_and_delete_entries_for(task: str, conn) -> None:
    match task:
        case 'fetch_copernicus_brasil':
            status = 'task_brasil_status'
            data = 'copernicus_brasil'
            day_entries = 5570
        
        case 'fetch_copernicus_foz':
            status = 'task_foz_status'
            data = 'copernicus_foz_do_iguacu'
            day_entries = 8

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
        f'  FROM weather.{data}'
        "  GROUP BY DATE_TRUNC('month', time)) AS res"
        '  WHERE to_char(('
        f"    (res.end_m - res.ini_m + '1 day') * {day_entries}), 'DD'"
        '  )::integer != res.tot;'
    )

    date_ranges = cur.fetchall()

    if any(date_ranges):
        for date_range in date_ranges:
            ini_m, end_m = date_range

            logger.warning(
                '[SCAN] Inconcistency found for '
                f'date {end_m} on {data}, removing entries'
            )
            
            conn.execute(
                f'DELETE FROM weather.{data}'
                f" WHERE time >= '{ini_m}'" 
                f" AND time <= '{end_m} 23:59:00'"
            )

            conn.execute(
                f'UPDATE weather.{STATUS_TABLE} SET'
                f' {status} = NULL'
                f" WHERE date = '{end_m}'"
            )
            
    else:
        logger.info('[SCAN] No inconsistent date were found')
