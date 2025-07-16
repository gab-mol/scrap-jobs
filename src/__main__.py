import os, yaml

from scraper.sites.classif_ads import NewsPapAds
from utils import logger 


def load_config():
    with open(os.path.join("config", "scraper.yml"), "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)     

    return cfg

if __name__ == "__main__":

    logger.setup_logging()

    classif = NewsPapAds(config=load_config())
    classif.run_and_store()