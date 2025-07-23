from psycopg2.errors import OperationalError
from typing import Literal
from datetime import datetime

from jobnlp.db.connection import conn
from jobnlp.db.schemas import validate_db_identifiers
from jobnlp.utils.logger import get_logger

log = get_logger(__name__)


def insert_bronze(add: dict) -> Literal[0, 1]:
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
                    ON CONFLICT (hash) DO NOTHING
                    RETURNING id;
                """
    try:
        cur = conn.cursor()
        cur.execute(query, (
            add["scrap_date"],
            add["source_url"],
            add["norm_text"],
            add["hash"]
        ))
        inserted = cur.fetchone()
        cur.close()
        conn.commit()
        return 1 if inserted else 0
    except OperationalError:
        log.error(f"Error inserting record with hash: {add['hash']}")
        raise

def insert_silver():
    ...


def fetchall_layer(table: str, date: str|None=None, since: str|None=None, 
                   to: str|None=None, scheme="adds_lakehouse"):
    '''
    Fetch data from the lakehouse.

    Parameters:
        table: layer table name (see jobnlp.db.schemas).
        date: date format %Y/%m/%d
        since: date format %Y/%m/%d
        to: date format %Y/%m/%d
        scheme: defoult = jobnlp.db.schemas.MAIN_SCHEME
    '''
    
    validate_db_identifiers(scheme, table)

    def validate_date(d):
        try:
            return datetime.strptime(d, "%Y-%m-%d").strftime("%Y-%m-%d")
        except Exception:
            raise ValueError(f"Date '{d}' must be in YYYY-MM-DD format")
    
    d_col = "scrap_date"
    today = datetime.now().strftime("%Y-%m-%d")

    if date:
        log.info(f"exec fetchall_layer | date: {date} | {scheme}.{table}")
        where = f"WHERE {d_col}='{date}'"

    elif to or since:
        if since:
            since = validate_date(since)

        if to:
            to = validate_date(to)

        if since and not to:
            log.info(f"fetchall_layer: {since} -> {today} (defoult)")
            where = f"WHERE {d_col} BETWEEN '{since}' AND '{today}'"

    if to and not since:
        log.warning(("Since `to` was provided but not `since`, only "
                        "records corresponding to `to` are returned."))
        where = f"WHERE {d_col}='{to}'"

    elif since and to:
        log.info(f"fetchall_layer: {since} -> {to}")
        where = f"WHERE {d_col} BETWEEN '{since}' AND '{to}'"

    else:
        msj="A date must be specified (`date`, or `since` and/or `to`)"
        log.error("fetchall_layer: "+msj)
        raise ValueError(msj)
    
    query = f"""
    SELECT * FROM {scheme}.{table}
    {where};
    """

    with conn.cursor() as cur:
        try:
            cur.execute(query)
        except OperationalError:
            log.error(f"Failed to execute: {query}")
            raise
        
        res = cur.fetchall()
        
    return res if res else None