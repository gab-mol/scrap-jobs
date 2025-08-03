from psycopg2.errors import OperationalError

from jobnlp.utils.logger import get_logger
from jobnlp.db.connection import conn

log = get_logger(__name__)

ALLOWED_SCHEMES = {"adds_lakehouse"}
ALLOWED_TABLES = {"adds_bronze", "adds_silver"}

def validate_db_identifiers(scheme: str, table: str) -> None:
    if scheme not in ALLOWED_SCHEMES:
        raise ValueError(f"Scheme '{scheme}' is not allowed.")
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Table '{table}' is not allowed.")


def create_schemas() -> None:
    with conn.cursor() as cur:
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS adds_lakehouse;
        """)
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
    except OperationalError:
        log.error("Unable to create 'adds_bronze' table.")
        raise

def create_silver() -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS adds_lakehouse.adds_silver (
                id SERIAL PRIMARY KEY,
                scrap_date DATE NOT NULL, 
                entity_text TEXT,
                label TEXT,
                start_pos INT,
                end_pos INT,
                hash TEXT,
                CONSTRAINT unique_hash_silver UNIQUE (hash)
            );
            """)
        conn.commit()
        log.info("Table 'adds_silver' created.")
    except OperationalError:
        log.error("Unable to create 'adds_silver' table.")
        raise

def create_tables() -> None:
    create_bronze()
    create_silver()

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


create_silver()