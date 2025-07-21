from psycopg2.errors import OperationalError

from jobnlp.utils.logger import get_logger
from jobnlp.db.connection import conn

log = get_logger(__name__)

def create_schemas() -> None:
    with conn.cursor() as cur:
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS adds_lakehouse;
        """)
        # log.info("Schema 'adds_lakehouse' created.")
        conn.commit()

def create_bronze() -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS adds_lakehouse.adds_bronze (
                id SERIAL PRIMARY KEY,
                scrap_date DATE NOT NULL, 
                source_url TEXT, 
                norm_text TEXT,
                hash TEXT,
                CONSTRAINT unique_hash UNIQUE (hash)
            );
            """)
        conn.commit()
        # log.info("Table 'adds_bronze' created.")
    except OperationalError:
        log.error("Unable to create 'adds_bronze' table.")
        raise

def create_silver() -> None:
    print(NotImplemented)
    # try:
    #     with conn.cursor() as cur:
    #         cur.execute("""
    #         CREATE TABLE IF NOT EXISTS adds_lakehouse.adds_silver (
    #             id SERIAL PRIMARY KEY,
        
    #         );
    #         """)
    #     conn.commit()
    #     log.info("Table 'adds_silver' created.")
    # except OperationalError:
    #     log.error("Unable to create 'adds_silver' table.")
    #     raise


def create_tables() -> None:
    create_bronze()
    # create_silver()

def db_init() -> None:
    '''
    Ensure the existence of schemas.
    '''
    try:
        create_schemas()
    except:
        log.error("Error when trying to create schemas.")

    try:
        create_tables()
    except:
        log.error("Error when trying to create tables.")