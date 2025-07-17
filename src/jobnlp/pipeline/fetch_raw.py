import yaml, pathlib

import jobnlp
from jobnlp.scraper.sites.classif_ads import NewsPapAds
from jobnlp.utils import logger 


def load_config():
    CFG_PATH = (
        pathlib.Path(jobnlp.__file__)
        .resolve()
        .parent / "scraper" / "config" / "scraper.yml"
    )
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)     

    return cfg

if __name__ == "__main__":
    LOG_PATH = pathlib.Path("log/fetch_raw.log")
    
    logger.setup_logging(logfile=LOG_PATH)

    classif = NewsPapAds(config=load_config())
    classif.run_and_store()