from psycopg2.errors import OperationalError

from jobnlp.utils.logger import get_logger

log = get_logger(__name__)

ALLOWED_SCHEMES = {"ads_lakehouse"}
ALLOWED_TABLES = {"ads_bronze", "ads_silver"}

def validate_db_identifiers(scheme: str, table: str) -> None:
    if scheme not in ALLOWED_SCHEMES:
        raise ValueError(f"Scheme '{scheme}' is not allowed.")
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Table '{table}' is not allowed.")

def schema_exists(conn, schema: str) -> bool:
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.schemata
            WHERE schema_name = %s
        );
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema,))
        result = cur.fetchone()[0]
        return result


def table_exists(conn, schema: str, table: str) -> bool:
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        );
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        return cur.fetchone()[0]

def create_schemas(conn) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE SCHEMA IF NOT EXISTS ads_lakehouse;
            """)
            conn.commit()
    except Exception as e:
        log.error("Error when trying to create schemas.")
        raise OperationalError from e

def create_bronze(conn) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS ads_lakehouse.ads_bronze (
                id SERIAL PRIMARY KEY,
                scrap_date DATE NOT NULL, 
                source_url TEXT, 
                norm_text TEXT,
                hash TEXT,
                CONSTRAINT unique_hash UNIQUE (hash)
            );
            """)
        conn.commit()
        log.info("Table 'ads_bronze' created.")
    except Exception as e:
        log.error("Unable to create 'ads_bronze' table.")
        raise OperationalError from e

def create_silver(conn) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS ads_lakehouse.ads_silver (
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
        log.info("Table 'ads_silver' created.")
    except Exception as e:
        log.error("Unable to create 'ads_silver' table.")
        raise OperationalError from e

def db_init(conn) -> None:
    '''
    Ensure the existence of schemas and tables.
    '''
    if not schema_exists(conn, "ads_lakehouse"):
        create_schemas(conn)

    if not table_exists(conn, "ads_lakehouse", "ads_bronze"):
        create_bronze(conn)
    else:
        log.info("ads_bronze table exist.")

    if not table_exists(conn, "ads_lakehouse", "ads_silver"):
        create_silver(conn)
    else:
        log.info("ads_silver table exist.")