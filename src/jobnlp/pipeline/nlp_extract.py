from datetime import date
from pathlib import Path
from spacy.language import Language
from typing import Iterator

import jobnlp
from jobnlp.db.connection import get_connection
from jobnlp.db.schemas import db_init, validate_db_identifiers
from jobnlp.db.models import (fetchall_layer, insert_silver, 
                              BronzeQueryError, SilverQueryError)
from jobnlp.nlp.nlp_custom import NLPRules
from jobnlp.utils.logger import setup_logging, get_logger
from jobnlp.utils.date_arg import get_exec_date

DIR = Path(jobnlp.__file__).parent
MOD_RUL_PATH = DIR / "nlp" / "models" / "rules_es"
LOG_PATH = Path("log/nlp_extract.log")

setup_logging(logfile=LOG_PATH)
log = get_logger(__name__)


def load_bronze_adds(conn, date: date, 
            table="ads_bronze", schema="ads_lakehouse"):

    validate_db_identifiers(schema, table)
    date_f = date.strftime("%Y-%m-%d")
    try:
        log.info(f"Querying ads scraped on: {date_f}")
        return fetchall_layer(
            conn,
            table, 
            date=date_f, schema=schema,
            cols=["scrap_date, norm_text, hash"],
            log=log)
    except Exception:
        log.error("Error querying data from the bronze layer")
        raise

def extract_ents(nlp: Language, data:list[tuple]) -> Iterator:
    '''
    Iterates returning a list of dict representing 
    the rows corresponding to each entity found per ad.
    '''
    for row in data:

        ents = nlp(row[1]).ents

        if not ents:
            continue

        ent_rows = list()
        for ent in ents:
            ent_rows.append({
                "scrap_date": row[0],
                "entity_text": ent.text,
                "label": ent.label_, 
                "start_pos": ent.start_char, 
                "end_pos": ent.end_char,
                "hash": row[2]
            }
        )
            
        yield ent_rows
    
def main():
    nlp_rul = NLPRules(log)
    nlp_rul.load_model(MOD_RUL_PATH)

    # date parameter
    run_date = get_exec_date(log)

    conn = get_connection()
    db_init(conn)

    try:
        adds_brz = load_bronze_adds(conn, date=run_date)
    except Exception as e:
        log.critical("Missing data. Aborting.")
        raise BronzeQueryError from e
        
    extr_gen = extract_ents(nlp_rul.nlp, adds_brz)

    inserted_count = 0
    try:
        for rs in extr_gen:
            res = insert_silver(conn, rs[0], log)
            inserted_count += res
    except Exception as e:
        log.critical("Abort insertion to silver layer.")
        raise SilverQueryError from e
    finally:
        conn.close()
    
    if inserted_count < 1:
        log.warning(f"No new ads were inserted to silver")
    else:
        log.info(f"{inserted_count} new ads were inserted to silver")

if __name__ == "__main__":

    main()
