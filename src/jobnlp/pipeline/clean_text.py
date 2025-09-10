import jsonlines, gzip
import pathlib
import re, hashlib
from bs4 import BeautifulSoup
import json

import jobnlp
from jobnlp.utils import logger, date_arg
from jobnlp.db.models import insert_bronze, BronzeQueryError
from jobnlp.pipeline.base import PipeInit

RAW_DIR = pathlib.Path("data/raw")
BRONZE_DIR = pathlib.Path("data/processed/bronze")
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
PATTERNS_PATH = pathlib.Path(jobnlp.__file__).parent / "utils" / "clean_patterns.json"

try:
    with open(PATTERNS_PATH, "r", encoding="utf-8") as f:
        PATTERNS: dict = json.load(f)
except FileNotFoundError as e:
    print(f"Missing {PATTERNS_PATH.name} file.")
    raise e

def remove_pattern(pattern_key: str, pattern_file: dict, text: str):
    combined = "|".join(pattern_file[pattern_key])
    return re.sub(combined, "", text, flags=re.IGNORECASE)

def remove_addresses(text: str) -> str:
    return remove_pattern("address_patterns", PATTERNS, text)

def remove_phone_numbers(text: str) -> str:
    return remove_pattern("phone_patterns", PATTERNS, text)

def remove_emails(text: str) -> str:
    return remove_pattern("email_patterns", PATTERNS, text)

def remove_urls(text: str) -> str:
    return remove_pattern("url_patterns", PATTERNS, text)

def remove_residual_phrases(text: str) -> str:
    return remove_pattern("residual_phrases", PATTERNS, text)

def gen_hash(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def normalize_text(text: str) -> str:
    text = text.lower()
    text = remove_addresses(text)
    text = remove_phone_numbers(text)
    text = remove_emails(text)
    text = remove_urls(text)
    text = remove_residual_phrases(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def clean_html(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(separator=" ", strip=True)

def process_file(file_path: pathlib.Path) -> list[dict]:

    adds_list = []

    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        reader = jsonlines.Reader(f)
        for obj in reader:
            
            raw_html = obj["raw"]
            
            clean = clean_html(raw_html)
            clean = normalize_text(clean)

            if clean:
               adds_list.append({
                    "norm_text": clean,
                    "scrap_date": obj.get("scraped_at"),
                    "source_url": obj.get("source_url"),
                    "hash": gen_hash(clean)
                })

        reader.close()
    return adds_list

def load_to_bronze(conn, add_list: list[dict], 
                   log: logger.Logger, raw_path: pathlib.Path):
    inserted_count = 0
    for add in add_list:
        res = insert_bronze(conn, add)
        inserted_count += res
    if inserted_count < 1:
        log.warning(f"No new ads were inserted from: {raw_path.name}")
    else:
        log.info((f"{inserted_count} new ads were inserted from:"
                  f"{raw_path.name}"))


def tranf_load(init: PipeInit, raw_path: pathlib.Path):
    """
    Preliminary cleaning transformations and loading to bronze layer.
    """
    if raw_path.exists():
        init.log.info(f"Processing file: {raw_path}")
        add_list = process_file(raw_path)
        
        try: 
            load_to_bronze(init.conn, add_list, init.log, raw_path)
            init.log.info((f"Processed: {raw_path.name} -> "
                      "DB: bronze layer"))
        except Exception as e:
            init.log.error((f"Could not save {raw_path.name} to DB: "
                        "bronze layer"))
            raise BronzeQueryError from e
        finally:
            init.conn.close()
    else:
        init.log.error(f"{raw_path} not found.")

def air_schedule():
    """
    Entry point for Airflow's DAG.
    The pipeline must be fully executed by each execution date.
    """
    init = PipeInit()
    run_date = date_arg.today()
    run_date_f = run_date.strftime("%Y%m%d")
    raw_path = pathlib.Path(f"data/raw/NewsPapAds_{run_date_f}.jsonl.gz")

    tranf_load(init, raw_path)

def main():
    """
    Entry point for `console_scripts` in `setup.py`. 
    Allows you to enter the date of the file in `/raw` to be read.
    """
    init = PipeInit()

    # date parameter
    run_date = date_arg.get_exec_date(init.log)
    run_date_f = run_date.strftime("%Y%m%d")
    raw_path = pathlib.Path(f"data/raw/NewsPapAds_{run_date_f}.jsonl.gz")

    tranf_load(init, raw_path)

if __name__ == "__main__":

    main()
