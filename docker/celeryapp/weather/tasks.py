from __future__ import absolute_import
from celery import states
from celery.exceptions import Ignore

import os
from datetime import datetime
from pathlib import Path

import satellite_weather as sat
from beat import app
from celeryapp.delay_controller import update_task_schedule
from dotenv import find_dotenv, load_dotenv
from loguru import logger
from sqlalchemy import create_engine

load_dotenv(find_dotenv())

PSQL_USER = os.getenv('POSTGRES_USER')
PSQL_PASSWORD = os.getenv('POSTGRES_PASSWORD')
HOST = os.getenv('POSTGRES_HOST')
PORT = os.getenv('POSTGRES_PORT')
DBASE = os.getenv('POSTGRES_DATABASE')
STATUS_TABLE = 'cope_download_status'

engine = create_engine(
    f'postgresql://{PSQL_USER}:{PSQL_PASSWORD}@{HOST}:{PORT}/{DBASE}'
)


@app.task(bind=True, name='fetch_copernicus_brasil', retry_kwargs={'max_retries': 1})
def brasil_monthly_data(self):
    fetch_cope_monthly_data(self=self, task='fetch_copernicus_brasil')


@app.task(bind=True, name='fetch_copernicus_foz', retry_kwargs={'max_retries': 1})
def foz_do_iguacu_montly_data(self):
    fetch_cope_monthly_data(self=self, task='fetch_copernicus_foz')


def fetch_cope_monthly_data(self, task: str) -> None:
    """
    This is the main method for the fetching tasks. It will look up 
    for the next available data to fetch in the status task table. 
    If a path is found, it will load the data with xarray and insert 
    transformed data into its corresponding data table. If no file is
    found to fetch, self-update task delay to run every day 20 of month.
    If any errors occur when inserting data into data table, delete entry
    for this data file, break the task and set its task status to NULL. 
    """
    match task:
        case 'fetch_copernicus_brasil':
            data_table = 'copernicus_brasil'
            task_status = 'task_brasil_status'

        case 'fetch_copernicus_foz':
            data_table = 'copernicus_foz_do_iguacu'
            task_status = 'task_foz_status'

    try:
        # Get next monthly data file to fetch
        with engine.connect() as conn:
            path = _produce_next_path(conn, task_status)

    except ValueError as err:
        # No data found to fetch, self-update delay schedule
        update_task_schedule(
            task=task,
            minute=0,
            hour=0,
            day_of_month=20,
        )
        logger.warning(
            f'Task {task} delay updated'
            ' to run every day 20 of month'
        )
        logger.error(err)

        self.update_state(
            state = states.FAILURE,
            meta = 'No date found to fetch.'
        )

        raise Ignore()

    def change_status_to(status: str) -> str:
        sql = (
            f'UPDATE weather.{STATUS_TABLE}'
            f" SET {task_status} = '{status}'"
            f" WHERE path = '{path}'"
        )
        return sql

    try:
        with engine.connect() as conn:
            conn.execute(change_status_to('in_progress'))
            # Xarray data extraction
            _insert_data_into_table(
                data = path,
                table = data_table,
                conn = conn
            )
            conn.execute(change_status_to('done'))
            logger.info(f'Task {task} has finished.')

    except Exception as e:
        # TODO: Find more specifics exceptions for table insertion error
        # If any error occur when inserting data into db, remove data
        # entry for this path
        logger.error(f'{e}\nDeleting data entry for {path}')
        with engine.connect() as conn:
            cur = conn.execute(
                f'SELECT date FROM weather.{STATUS_TABLE}'
                f" WHERE path = '{path}'"
            )
            date = cur.fetchone()[0]
            conn.execute(
                f'UPDATE weather.{STATUS_TABLE} SET'
                f' path = NULL,'
                f' {task_status} = NULL'
                f" WHERE path = '{path}'"
            )
            conn.execute(
                f'DELETE FROM weather.{data_table}'
                 ' WHERE time BETWEEN'
                f" '{datetime(date.year, date.month, 1)}' AND '{date}'"
            )

        Path(path).unlink(missing_ok=True)

    finally:

        self.update_state(
            state = states.FAILURE,
            meta = 'An error occurred when inserting data into DB.'
        )

        raise Ignore()

    
def _insert_data_into_table(data: str, table: str, conn) -> None:
    """
    Here the dataset will be loaded with xarray, data results will
    be calculate and insert into data table according to its task.
    This process will fail if there is any NULL values in the data,
    indicating the file is corrupted and its entry needs to be deleted. 
    """
    municipalities = sat.extract_latlons.municipios
    ds = sat.load_dataset(data)

    match table:
        case 'copernicus_brasil':
            for mun in municipalities:
                df = ds.copebr.to_dataframe(mun['geocodigo'])
                df.to_sql(
                    name = 'copernicus_brasil',
                    schema = 'weather',
                    con = conn,
                    if_exists = 'append',
                )
                logger.debug(
                    f"Data for {mun['municipio']}"
                    f' inserted into `copernicus_brasil`'
                    f'\nFile: {data}'
                )
            logger.info(
                f'All data from {data} inserted into `copernicus_brasil`'
            )

        case 'copernicus_foz_do_iguacu':
            df = ds.copebr.to_dataframe(4108304, raw=True)
            df.to_sql(
                name = 'copernicus_foz_do_iguacu',
                schema = 'weather',
                con = conn,
                if_exists = 'append'
            )
            logger.info(
                f'Data for Foz do Iguaçu'
                f' inserted into `copernicus_foz_do_iguacu`'
                f'\nFile: {data}'
            )


def _produce_next_path(conn, task_status) -> str:
    """
    This method will produce the next day to be updated.
    Raises ValueError if there is no next date.

    Rules for next month to fetch:
        1. Needs to have a path
        2. Its status can't be `done` nor `in_progress`
        3. Returns the max available date to fetch
    Returns:
        file_path (str): Next data file to fetch
    """
    sql = (
        f'SELECT MAX(date) FROM weather.{STATUS_TABLE} WHERE'
         ' path IS NOT NULL AND'
        f' {task_status} IS NULL OR ('
          f" {task_status} != 'done' AND"
          f" {task_status} != 'in_progress')"
    )
    cur = conn.execute(sql)
    to_update = cur.fetchone()[0]

    if not to_update:
        raise ValueError('No date available to fetch')

    path_query = (
        f'SELECT path FROM weather.{STATUS_TABLE}'
        f" WHERE date = '{to_update}'"
    )

    cur = conn.execute(path_query)
    path = cur.fetchone()[0]

    logger.info(f'Next date to fetch: {to_update}\nFile: {path}')

    return path
