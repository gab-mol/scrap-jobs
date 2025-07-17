import jsonlines, gzip
import pandas as pd
import pathlib
import re
from bs4 import BeautifulSoup
from datetime import datetime
import argparse

import jobnlp
from jobnlp.utils import logger

RAW_DIR = pathlib.Path("data/raw")
BRONZE_DIR = pathlib.Path("data/processed/bronze")
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

EMAIL_PATTERN = r"\b[\w\.-]+@[\w\.-]+\.\w+\b"

def clean_html(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(separator=" ", strip=True)

def normalize_text(text: str) -> str:
    text = text.lower()
    text = re.sub(EMAIL_PATTERN, "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def process_file(file_path: pathlib.Path) -> pd.DataFrame:
    rows = []

    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        reader = jsonlines.Reader(f)
        i = 0
        for obj in reader:

            raw_html = obj["raw"]
            clean = clean_html(raw_html)
            clean = normalize_text(clean)
            rows.append({
                "text_clean": clean,
                "scraped_at": obj.get("scraped_at"),
                "source_url": obj.get("source_url"),
            })
            i += 1
        reader.close()
    return pd.DataFrame(rows)

def main():

    LOG_PATH = pathlib.Path("log/clean_text.log")
    logger.setup_logging(logfile=LOG_PATH)
    log = logger.get_logger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=str,
        help="Fecha de ejecución en formato YYYY-MM-DD (default: hoy)",
    )
    
    args = parser.parse_args()

    if args.date:
        try:
            run_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            log.info(f"Ejecusión de clean_text para archivo de: {run_date}")
        except ValueError:
            raise ValueError("Fecha inválida. Usar formato YYYY-MM-DD.")
    else:
        log.info("Ejecusión de clean_text en defoult (hoy)")
        run_date = datetime.today().date()

    ds_nodash = run_date.strftime("%Y%m%d")

    raw_path = pathlib.Path(f"data/raw/NewsPapAds_{ds_nodash}.jsonl.gz")

    if raw_path.exists():
        log.info(f"Procesando archivo: {raw_path}")
        df = process_file(raw_path)
        timestp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_name = f"bronze_{timestp}.parquet"
        out_path = BRONZE_DIR / out_name
        df.to_parquet(out_path, index=False)
        log.info(f"Procesado: {raw_path.name} -> {out_name}")
    else:
        log.error(f"{raw_path} no encontrado.")

if __name__ == "__main__":

    main()
