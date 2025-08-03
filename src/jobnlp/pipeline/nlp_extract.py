from datetime import datetime, date
from pathlib import Path
from spacy.language import Language
from typing import Iterator
import jobnlp
from jobnlp.db.schemas import db_init
from jobnlp.db.models import fetchall_layer, insert_silver
from jobnlp.nlp.nlp_custom import NLPRules
from jobnlp.utils.logger import setup_logging, get_logger

DIR = Path(jobnlp.__file__).parent
MOD_RUL_PATH = DIR / "nlp" / "models" / "rules_es"
LOG_PATH = Path("log/nlp_extract.log")

setup_logging(logfile=LOG_PATH)
log = get_logger(__name__)


def load_bronze_adds(date: str|None = None):
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")

    try:
        return fetchall_layer(
            "adds_bronze", 
            since="2025-07-16", # date
            cols=["scrap_date, norm_text, hash"])
    except:
        log.error("Error querying data from the bronze layer")

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
    db_init()
    adds_brz = load_bronze_adds()
    if not adds_brz:
        log.critical("Missing data. Aborting.")
        return
        
    extr_gen = extract_ents(nlp_rul.nlp, adds_brz)

    inserted_count = 0
    try:
        for rs in extr_gen:
            res = insert_silver(rs[0], log)
            inserted_count += res
    except:
        log.critical("Abort insertion to silver layer.")
        return
    
    if inserted_count < 1:
        log.warning(f"No new ads were inserted to silver")
    else:
        log.info(f"{inserted_count} new ads were inserted to silver")

if __name__ == "__main__":

    main()
