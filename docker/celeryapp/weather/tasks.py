from __future__ import absolute_import

import os

from celeryapp.delay_controller import update_task_schedule
from dotenv import find_dotenv, load_dotenv
from loguru import logger
from sqlalchemy import create_engine

from .beat import app

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


def _sql_create_table(schema: str, table: str) -> str:
    sql = (
        f'CREATE TABLE IF NOT EXISTS {schema}.{table} ('
        ' path TEXT DEFAULT NULL,'
        ' in_progess BOOLEAN NOT NULL DEFAULT false,'
        ' done BOOLEAN NOT NULL DEFAULT false'
        ')'
    )
    return sql
