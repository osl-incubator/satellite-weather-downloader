from loguru import logger
from dotenv import find_dotenv, load_dotenv
from pathlib import Path
import sqlite3
import os
import pandas as pd

load_dotenv(find_dotenv())

DB = os.getenv('BACKFILL_FILE')


class BackfillDB():
    
    def __init__(self) -> None:
        self.conn = self.sql_conn()
        self.initialize_db(self.conn)


    def sql_conn(self) -> sqlite3.Connection:
        #Creates the db file if not exists, returns SQLite connection
        filename = Path(DB).expanduser()
        filename.touch(exist_ok=True)
        return sqlite3.connect(filename)


    def get_tables(self) -> set:
        #Get tables from DB file
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = set(row[0] for row in cur)
        return tables


    def initialize_db(self, conn) -> None:
        """
        Creates the table for the backfilling process. Each table consists
        in 3 variables: 
         - date: datetime
         - done: bool = false 
         - in_progress: bool = false
        """
        cur = conn.cursor()
        tables = self.get_tables()

        def create_table(tablename: str):
            cur.execute(f"CREATE TABLE {tablename} ("
                        " date DATETIME PRIMARY KEY UNIQUE NOT NULL,"
                        " done BOOLEAN NOT NULL DEFAULT false,"
                        " in_progress BOOLEAN NOT NULL DEFAULT false)")
        
        with conn:
            if not 'brasil' in tables:
                create_table('brasil')
            if not 'foz' in tables:
                create_table('foz')
        
        conn.commit()


    def populate_tables(self):
        #Count the rows quantity for ea table, populate with daterange if
        #empty table.
        cur = self.conn.cursor()
        tables = self.get_tables()

        def create_df(dates: pd.bdate_range) -> pd.DataFrame:
            df = pd.DataFrame()
            dones = [0 for n in range(len(dates))]
            in_progress = [0 for n in range(len(dates))]
            df.insert(0, 'date', dates)
            df.insert(1, 'done', dones)
            df.insert(2, 'in_progress', in_progress)
            df = df.set_index('date')
            return df

        for table in tables:
            with self.conn:
                cur.execute(f"SELECT COUNT(*) FROM {table}") 
                rows = cur.fetchone()[0]
                
            if not rows:
                match table:
                    case 'brasil':
                        dates = pd.bdate_range(
                            start='1/1/2000', 
                            end='12/1/2022', 
                            freq='D'
                            )[::-1]
                        df = create_df(dates)
                        df.to_sql(table, self.conn, if_exists='append')

                    case 'foz':
                        dates = pd.bdate_range(
                            start='1/1/2000', 
                            end='12/1/2022', 
                            freq='M'
                            )[::-1]
                        df = create_df(dates)
                        df.to_sql(table, self.conn, if_exists='append')
            

if __name__ == "__main__":
    db = BackfillDB()
    db.populate_tables()
    logger.info(f'⚙️  Backfill Database configured.')
