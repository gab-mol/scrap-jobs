from psycopg2.errors import OperationalError

from jobnlp.db.connection import conn
from jobnlp.utils.logger import get_logger

log = get_logger(__name__)

def insert_bronze(add: dict) -> None:
    '''
    Insert row into table `adds_bronze`.

    :Parameter:
    add: `dict` (Mandatory keys: colnames)   
        - scrap_date  
        - source_url  
        - norm_text  
        - hash  
    '''
    query = """INSERT INTO adds_lakehouse.adds_bronze (scrap_date, source_url, norm_text, hash)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (hash) DO NOTHING;
                """
    try:
        with conn.cursor() as cur:
                cur.execute(query, (
                    add["scrap_date"],
                    add["source_url"],
                    add["norm_text"],
                    add["hash"]
                ))

        conn.commit()
    except OperationalError:
        log.error(f"Error inserting record with hash: {add['hash']}")
        raise