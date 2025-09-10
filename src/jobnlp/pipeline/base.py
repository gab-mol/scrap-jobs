import pathlib

from jobnlp.utils import logger
from jobnlp.db.connection import get_connection
from jobnlp.db.schemas import db_init

class PipeInit:

    def __init__(self):
        
        LOG_PATH = pathlib.Path("log/clean_text.log")
        logger.setup_logging(logfile=LOG_PATH)
        self.log = logger.get_logger(__name__)
        
        self.conn = get_connection()
        db_init(self.conn)