import os
import sqlite3
import pandas as pd
from pathlib import Path
from loguru import logger
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

DB = os.getenv('BACKFILL_FILE')


class BackfillDB():
    """ 
    When initiated, touches the backfill db file. Also able to populate
    the date with a date range and a frequency.
    """
    def __init__(self) -> None:
        self.conn = self._sql_conn()
        self._initialize_db(self.conn)


    def _sql_conn(self) -> sqlite3.Connection:
        #Creates the db file if not exists, returns SQLite connection
        filename = Path(DB).expanduser()
        filename.touch(exist_ok=True)
        return sqlite3.connect(filename)


    def _get_tables(self) -> set:
        #Get tables from DB file
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = set(row[0] for row in cur)
        return tables


    def _initialize_db(self, conn) -> None:
        """
        Creates the table for the backfilling process. Each table consists
        in 3 variables: 
         - date: datetime
         - done: bool = false 
         - in_progress: bool = false
        """
        cur = conn.cursor()
        tables = self._get_tables()

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
        tables = self._get_tables()

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
            

class BackfillHandler():
    """
    This class is responsible for controlling the dates of the Backfill DB,
    it returns the next available date to be fetched and updates the rows
    when needed.
    """
    def __init__(self, tablename) -> None:
        self.conn = BackfillDB().conn
        self.tablename = tablename
    

    def next_date(self) -> str:
        #Reads the next date and update `in_progress` to True
        cur = self.conn.cursor()

        cur.execute(f"SELECT MAX(date) FROM {self.tablename} WHERE"
                     " done = false AND"
                     " in_progress = false;")

        date = cur.fetchone()[0]
        
        cur.execute(f'UPDATE {self.tablename}'
                     ' SET in_progress = true' 
                    f' WHERE date = "{date}";')
        
        self.conn.commit()
        return date
    

    def set_done(self, date):
        #Updates done and in_progress for a specific date
        cur = self.conn.cursor()

        try:
            cur.execute(f'UPDATE {self.tablename} SET'
                         ' done = CASE'
	                     '  WHEN done = false'
		                 '   THEN true END,'
	                     ' in_progress = CASE'
	                     '  WHEN in_progress = true'
		                 '   THEN in_progress = false END'
	                    f' WHERE date = "{date}";'
            )

            self.conn.commit()
            logger.debug(f"☑️  backfill task for {date} done on {self.tablename}")

        except sqlite3.IntegrityError as e:
            logger.error(f"❌ task for {date} on table {self.tablename} "
                         f"has been fetched already!")

    
    def set_unfinished(self, date):
        #In case some error occur after retrieving the date,
        # this method changes in_progress back to false 
        cur = self.conn.cursor()

        cur.execute(f'UPDATE {self.tablename} SET'
                     ' in_progress = false'
                    f' WHERE date = "{date}";')

        self.conn.commit()
        logger.warning(f"❗ task for {date} on table {self.tablename} was left "
                        "unfinished. Changing `in_progress` back to `false`")


if __name__ == "__main__":
    db = BackfillDB()
    db.populate_tables()
    logger.info(f'⚙️  Backfill Database configured.')
