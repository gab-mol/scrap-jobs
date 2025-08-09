from psycopg2.errors import OperationalError
from typing import Literal, Any, Optional
from datetime import datetime, date

from jobnlp.db.schemas import validate_db_identifiers
from jobnlp.utils.logger import Logger

COLS_WHITE_LIST = {"scrap_date", "source_url", "norm_text", "hash",
                   "entity_text", "label", "start_pos", "end_pos"}

def validate_cols(cols: list[str]) -> None:
    invalid = [c for c in cols if c not in COLS_WHITE_LIST]
    if invalid:
        raise ValueError(f"Invalid column(s): {', '.join(invalid)}")
    
def validate_filters(filters: dict[str, Any]) -> None:
    invalid = [k for k in filters if k not in COLS_WHITE_LIST]
    if invalid:
        raise ValueError(f"Invalid filter column(s): {', '.join(invalid)}")

def validate_date(d):
    try:
        return datetime.strptime(d, "%Y-%m-%d").strftime("%Y-%m-%d")
    except Exception:
        raise ValueError(f"Date '{d}' must be in YYYY-MM-DD format")

class BronzeQueryError(Exception):
    """Raised when querying the bronze layer fails."""
    pass


class SilverQueryError(Exception):
    """Raised when querying the silver layer fails."""
    pass


class GoldQueryError(Exception):
    """Raised when querying the gold layer fails."""
    pass


def insert_bronze(conn, add: dict, log: Logger|None = None) -> Literal[0, 1]:
    '''
    Insert row into table `ads_bronze`.

    :Parameter:
    conn: psycopg2 connection object.   

    add: `dict` (Mandatory keys: colnames)   
        - scrap_date  
        - source_url  
        - norm_text  
        - hash  
    
    log: logging object.
    '''
    query = """INSERT INTO ads_lakehouse.ads_bronze (scrap_date, source_url, norm_text, hash)
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
    Insert row into table `ads_silver`.

    ### Parameters
    conn: psycopg2 connection object.   
    
    add: `dict` (Mandatory keys: colnames)   
        - scrap_date  
        - source_url  
        - norm_text  
        - hash  
    
    log: logging object.
    '''
    query = """INSERT INTO ads_lakehouse.ads_silver (scrap_date, entity_text, 
                        label, start_pos, end_pos, hash)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (hash, entity_text) DO NOTHING
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

def insert_gold_disc(conn, table_name: str,
                add: dict, log: Logger|None = None):
    '''
    ## Discarded idea,
    of segregating by entity label in different tables.
    -- insert

    ### Parameters
    conn: psycopg2 connection object.  

    table_name: Name of the gold layer table to insert into.  

    add: `dict` (Mandatory keys: colnames)  
        - hash  
        - scrap_date  
        - entity_text

    log: logging object.
    '''
    query = f"""INSERT INTO ads_lakehouse.{table_name} (hash, scrap_date, 
                        entity_text)
                VALUES (%s, %s, %s)
                ON CONFLICT (hash) DO NOTHING
                RETURNING id;
                """
    try:
        cur = conn.cursor()
        cur.execute(query, (
            add["hash"],
            add["scrap_date"],
            add["entity_text"]
        ))
        inserted = cur.fetchone()
        cur.close()
        conn.commit()
        return 1 if inserted else 0
    except Exception as e:
        if log: log.error(("Error inserting record with hash: "
                          f"{add.get('hash', '?')}. {type(e).__name__}: {e}"))
        raise GoldQueryError from e

def fetchall_layer(
    conn,
    table: str,
    date: str | None = None,
    since: str | None = None,
    to: str | None = None,
    filters: dict[str, Any] | None = None,
    cols: list[str] | None = None,
    schema: str = "ads_lakehouse",
    log: Logger | None = None
):
    '''
    Fetch data from the lakehouse with optional filters.

    Parameters:
        conn: psycopg2 connection object.  
        table: table name.
        date: fetch specific date (YYYY-MM-DD).
        since: fetch records from this date onward.
        to: fetch records up to this date.
        filters: dict of filters e.g. {"col": "value"}
        cols: list of column names to select.
        schema: schema name.
        log: logger.
    '''
    validate_db_identifiers(schema, table)

    if cols:
        validate_cols(cols)

    if filters:
        validate_filters(filters)

    d_col = "scrap_date"
    today = datetime.now().strftime("%Y-%m-%d")

    where_clauses = []
    values = []

    if date:
        date = validate_date(date)
        where_clauses.append(f"{d_col} = %s")
        values.append(date)

    elif since or to:
        if since:
            since = validate_date(since)
        if to:
            to = validate_date(to)

        if since and to:
            where_clauses.append(f"{d_col} BETWEEN %s AND %s")
            values.extend([since, to])
        elif since:
            where_clauses.append(f"{d_col} BETWEEN %s AND %s")
            values.extend([since, today])
        elif to:
            where_clauses.append(f"{d_col} = %s")
            values.append(to)
    else:
        raise ValueError("Must specify `date` or `since`/`to`.")

    if filters:
        for col, val in filters.items():
            if not isinstance(col, str):
                raise ValueError("Filter keys must be column names (str)")
            where_clauses.append(f"{col} = %s")
            values.append(val)

    col_sel = "*"
    if cols:
        col_sel = ", ".join(cols)

    where_clause = " AND ".join(where_clauses)

    query = f"""
        SELECT {col_sel}
        FROM {schema}.{table}
        WHERE {where_clause};
    """

    if log:
        log.info(f"Executing query on {schema}.{table} | Filters: {filters} | Dates: {date or (since, to)}")

    with conn.cursor() as cur:
        try:
            cur.execute(query, tuple(values))
            return cur.fetchall()
        except Exception as e:
            if log:
                log.error(f"Query failed: {query.strip()} | Args: {values}")
            raise OperationalError from e

def agreg_from_silver(conn, *,
                      date_eq: Optional[date] = None,
                      since: Optional[date] = None,
                      to: Optional[date] = None,
                      label: Optional[str] = None,
                      log=None):
    """
    Returns aggregates by entity_text (+ label) for a date or range.

    #### Parameters
    date_eq: Filters by an exact date.  
    since/to: Filters by range [since,to] (if only since, use since..today).  
    label: Optional, filters by label.  
    log: logging.
    """
    if not (date_eq or since or to):
        raise ValueError("Provide date_eq or since/to")

    where_clauses = []
    params = []

    if date_eq:
        where_clauses.append("scrap_date = %s")
        params.append(date_eq)

    else:
        if since and to:
            where_clauses.append("scrap_date BETWEEN %s AND %s")
            params.extend([since, to])
        elif since and not to:
            today = datetime.date.today()
            where_clauses.append("scrap_date BETWEEN %s AND %s")
            params.extend([since, today])
        elif to and not since:
            where_clauses.append("scrap_date = %s")
            params.append(to)

    if label:
        where_clauses.append("label = %s")
        params.append(label)

    # Construir query
    where_sql = " AND ".join(where_clauses)
    query = f"""
        SELECT entity_text,
               label,
               COUNT(*) AS count,
               COUNT(DISTINCT hash) AS count_ads,
               scrap_date
        FROM ads_lakehouse.ads_silver
        WHERE {where_sql}
        GROUP BY entity_text, label, scrap_date
        ORDER BY count DESC;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        if log: log.error("Error querying silver layer: %s", e)
        raise OperationalError from e
    
def insert_gold(conn, add: dict, log: Logger):
    query = """
        INSERT INTO ads_lakehouse.ads_gold
        (entity_text, label, count, count_ads, scrap_date)
        VALUES (%s, %s, %s, %s, %s)
    """
    try:
        cur = conn.cursor()
        cur.execute(query, (
            add["entity_text"],
            add["label"],
            add["count"],
            add["count_ads"],
            add["scrap_date"].strftime("%Y-%m-%d")
        ))
        conn.commit()
        cur.close()
        return 1
    except Exception as e:
        if log:
            log.error(("Error inserting into gold layer for: "
                      f"{add.get('scrap_date')}"))
        raise GoldQueryError from e
