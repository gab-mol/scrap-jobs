import pathlib, datetime

import jobnlp
from jobnlp.db.connection import get_connection
from jobnlp.db.schemas import db_init
from jobnlp.db.models import (validate_db_identifiers, SilverQueryError,
                              agreg_from_silver, insert_gold)
from jobnlp.utils import logger, date_arg, read_labels

DIR = pathlib.Path(jobnlp.__file__).parent
LOG_PATH = pathlib.Path("log/entity_exporter.log")

logger.setup_logging(logfile=LOG_PATH)
log = logger.get_logger(__name__)


# def load_silver_ents(conn, 
#                     date: datetime.date,
#                     table="ads_silver", 
#                     schema="ads_lakehouse") -> dict[str:list[tuple]]:

#     date_f = date.strftime("%Y-%m-%d")

#     validate_db_identifiers(schema, table)

#     ent_labels = read_labels.get_labels_from_rules_json()
#     try:
#         log.info(f"Querying ads scraped on: {date_f}")

#         ents = dict()
#         for lab in ent_labels:
#             ents[lab] = fetchall_layer(
#                 conn,
#                 table=table, 
#                 since=date_f, schema=schema,
#                 cols=["scrap_date", "entity_text", "hash"],
#                 filters={"label":lab},
#                 log=log)
#         return ents
#     except Exception as e:
#         log.error("Error querying data from the silver layer")
#         raise SilverQueryError from e


def main():

    # date parameter
    run_date = date_arg.get_exec_date(log)

    conn = get_connection()

    db_init(conn)

    silver_ents = agreg_from_silver(conn, 
                                    # date_eq=run_date,
                                    since=datetime.date(2025,7,17),
                                    to=datetime.date(2025,8,9),
                                    log=log)
    count = 0
    for r in silver_ents:
        insert_gold(conn, r, log)
        count += 1
    print("Inserted:", count)

if __name__ == "__main__":

    main()