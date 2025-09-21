import pathlib

import jobnlp
from jobnlp.db.models import (SilverQueryError,
                              agreg_from_silver, insert_gold)
from jobnlp.utils import date_arg
from jobnlp.pipeline.base import PipeInit
from jobnlp.utils.date_arg import today

DIR = pathlib.Path(jobnlp.__file__).parent
LOG_PATH = pathlib.Path("log/entity_count.log")

def tasks(init: PipeInit, run_date):
    
    init.log.info("Reading from the silver layer: %s", 
             run_date.strftime("%d/%m/%Y"))
    
    try:
        silver_ents = agreg_from_silver(init.conn,
                                        date_eq=run_date,
                                        log=init.log)
    except Exception as e:
        init.log.error(("It was not possible to read and group data"
                  f"from the silver layer. For date: {run_date}"))
        raise SilverQueryError from e
    
    count = 0
    for r in silver_ents:
        insert_gold(init.conn, r, init.log)
        count += 1
    if count > 0:
        init.log.info("Inserted counts in gold layer for %i entities:", count)
    else:
        init.log.warning("No new entity counts were saved.")

def air_schedule():
    init = PipeInit()
    tasks(init, today())

def main():
    init = PipeInit()
    run_date = date_arg.get_exec_date(init.log)
    tasks(init, run_date)

if __name__ == "__main__":

    main()