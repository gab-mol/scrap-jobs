import pathlib, datetime

import jobnlp
from jobnlp.db.connection import get_connection
from jobnlp.db.schemas import db_init
from jobnlp.db.models import (SilverQueryError,
                              agreg_from_silver, insert_gold)
from jobnlp.utils import logger, date_arg

DIR = pathlib.Path(jobnlp.__file__).parent
LOG_PATH = pathlib.Path("log/entity_exporter.log")

logger.setup_logging(logfile=LOG_PATH)
log = logger.get_logger(__name__)

def main():

    run_date = date_arg.get_exec_date(log)

    conn = get_connection()

    db_init(conn)
    log.info("Reading from the silver layer: %s", 
             run_date.strftime("%d/%m/%Y"))
    try:
        silver_ents = agreg_from_silver(conn,
                                        date_eq=run_date,
                                        log=log)
    except Exception as e:
        log.error(("It was not possible to read and group data"
                  f"from the silver layer. For date: {run_date}"))
        raise SilverQueryError from e
    
    count = 0
    for r in silver_ents:
        insert_gold(conn, r, log)
        count += 1
    if count > 0:
        log.info("Inserted counts in gold layer for %i entities:", count)
    else:
        log.warning("No new entity counts were saved.")
if __name__ == "__main__":

    main()