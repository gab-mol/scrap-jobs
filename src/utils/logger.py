import logging
from pathlib import Path

LOG_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

def setup_logging(level="INFO", logfile: Path | None = None):
    handlers = [logging.StreamHandler()]
    if logfile:
        handlers.append(logging.FileHandler(logfile, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format=LOG_FMT,
        handlers=handlers,
        force=True,
    )

