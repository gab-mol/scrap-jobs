from datetime import date
from pathlib import Path
from spacy.language import Language
from typing import Iterator

import jobnlp
from jobnlp.db.schemas import validate_db_identifiers
from jobnlp.db.models import (fetchall_layer, insert_silver, 
                              BronzeQueryError, SilverQueryError)
from jobnlp.nlp.nlp_custom import NLPRules
from jobnlp.utils.date_arg import get_exec_date, today
from jobnlp.pipeline.base import PipeInit

DIR = Path(jobnlp.__file__).parent
MOD_RUL_PATH = DIR / "nlp" / "models" / "rules_es"

def load_bronze_adds(conn, date: date, log,
            table="ads_bronze", schema="ads_lakehouse"):

    validate_db_identifiers(schema, table)
    date_f = date.strftime("%Y-%m-%d")
    try:
        log.info(f"Querying ads scraped on: {date_f}")
        return fetchall_layer(
            conn,
            table, 
            date=date_f, schema=schema,
            cols=["scrap_date", "norm_text", "hash"],
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

        for ent in ents:
            yield {
                "scrap_date": row[0],
                "entity_text": ent.text,
                "label": ent.label_, 
                "start_pos": ent.start_char, 
                "end_pos": ent.end_char,
                "hash": row[2]
            }

def tasks(init: PipeInit, nlp_rul: NLPRules, run_date):

    try:
        adds_brz = load_bronze_adds(init.conn, run_date,
                                    init.log)
    except Exception as e:
        init.log.critical("Missing data. Aborting.")
        raise BronzeQueryError from e
        
    extr_gen = extract_ents(nlp_rul.nlp, adds_brz)

    inserted_count = 0
    try:
        for rs in extr_gen:
            res = insert_silver(init.conn, rs, init.log)
            inserted_count += res
    except Exception as e:
        init.log.critical("Abort insertion to silver layer.")
        raise SilverQueryError from e
    finally:
        init.conn.close()
    
    if inserted_count < 1:
        init.log.warning(f"No new ads were inserted to silver")
    else:
        init.log.info(f"{inserted_count} new ads were inserted to silver")

def air_schedule():
    init = PipeInit()
    nlp_rul = NLPRules(init.log)
    nlp_rul.load_model(MOD_RUL_PATH)

    tasks(init, nlp_rul, today())

def main():
    init = PipeInit()
    nlp_rul = NLPRules(init.log)
    nlp_rul.load_model(MOD_RUL_PATH)

    # date parameter
    run_date = get_exec_date(init.log)

    tasks(init, nlp_rul, run_date)

if __name__ == "__main__":

    main()
