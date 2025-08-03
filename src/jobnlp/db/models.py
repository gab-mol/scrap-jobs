from psycopg2.errors import OperationalError
from typing import Literal
from datetime import datetime

from jobnlp.db.schemas import validate_db_identifiers
from jobnlp.utils.logger import Logger

class BronzeQueryError(Exception):
    """Raised when querying the bronze layer fails."""
    pass


class SilverQueryError(Exception):
    """Raised when querying the silver layer fails."""
    pass


def insert_bronze(conn, add: dict, log: Logger|None = None) -> Literal[0, 1]:
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
    except Exception as e:
        if log: log.error(("Error inserting record with hash: "
                          f"{add.get('hash', '?')}. {type(e).__name__}: {e}"))
        raise BronzeQueryError from e

def insert_silver(conn, add: dict, log: Logger|None = None):
    '''
    Insert row into table `adds_silver`.

    :Parameter:
    add: `dict` (Mandatory keys: colnames)   
        - scrap_date  
        - source_url  
        - norm_text  
        - hash  
    '''
    query = """INSERT INTO adds_lakehouse.adds_silver (scrap_date, entity_text, 
                        label, start_pos, end_pos, hash)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (hash) DO NOTHING
                RETURNING id;
                """
    try:
        cur = conn.cursor()
        cur.execute(query, (
            add["scrap_date"],
            add["entity_text"],
            add["label"],
            add["start_pos"],
            add["end_pos"],
            add["hash"]
        ))
        inserted = cur.fetchone()
        cur.close()
        conn.commit()
        return 1 if inserted else 0
    except Exception as e:
        if log: log.error(("Error inserting record with hash: "
                          f"{add.get('hash', '?')}. {type(e).__name__}: {e}"))
        raise SilverQueryError from e

def fetchall_layer(conn, table: str, date: str|None=None, since: str|None=None, 
                   to: str|None=None, cols: list[str]|None = None, 
                   scheme="adds_lakehouse", log: Logger|None = None):
    '''
    Fetch data from the lakehouse.

    Parameters:
        table: layer table name (see jobnlp.db.schemas).
        date: date format %Y-%m-%d
        since: date format %Y-%m-%d
        to: date format %Y-%m-%d
        cols: select columns. All selected by defoult.
        scheme: name. See jobnlp.db.schemas.ALLOWED_SCHEMES
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
        if log: log.info(f"exec fetchall_layer | date: {date} | {scheme}.{table}")
        where = f"WHERE {d_col}='{date}'"

    elif to or since:
        if since:
            since = validate_date(since)

        if to:
            to = validate_date(to)

        if since and not to:
            if log: log.info(f"fetchall_layer: {since} -> {today} (defoult)")
            where = f"WHERE {d_col} BETWEEN '{since}' AND '{today}'"

        if to and not since:
            if log: log.warning(("Since `to` was provided but not `since`, only "
                            "records corresponding to `to` are returned."))
            where = f"WHERE {d_col}='{to}'"

        elif since and to:
            if log: log.info(f"fetchall_layer: {since} -> {to}")
            where = f"WHERE {d_col} BETWEEN '{since}' AND '{to}'"

    else:
        msj="A date must be specified (`date`, or `since` and/or `to`)"
        if log: log.error("fetchall_layer: "+msj)
        raise ValueError(msj)
    
    col_sel = "*" if not cols else ", ".join(cols)

    query = f"""
    SELECT {col_sel} FROM {scheme}.{table}
    {where};
    """

    with conn.cursor() as cur:
        try:
            cur.execute(query)
        except Exception as e:
            if log: log.error(f"Failed to execute: {query}")
            raise OperationalError from e
        
        res = cur.fetchall()
        
    return res if res else None