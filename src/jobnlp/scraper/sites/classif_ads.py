import jsonlines, gzip
from pathlib import Path
from datetime import datetime, timezone
from bs4 import NavigableString

from jobnlp.scraper.base import BaseScraper
from jobnlp.utils.logger import get_logger

log = get_logger(__name__)

class NewsPapAds(BaseScraper):

    def __init__(self, config):
        self.url: str = config["scraping_url"]["newsp"]["classif_ads_s1"]
        self.RAW_STORAGE_DIR = Path("data/raw")

    @staticmethod
    def _dump_jsonl(records: list[dict], dest: Path) -> None:
        dest = dest.with_suffix(dest.suffix + ".gz")
        dest.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(dest, "wt", encoding="utf-8") as gz:
            writer = jsonlines.Writer(gz)
            writer.write_all(records)

    def extract(self, dom):

        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")

        paid = dom.select("p.pago")

        if not paid:
            log.warning("0 avisos pagos encontrados en %s", self.url)

        for p in paid:
            yield {
                "raw_html": str(p),
                "selector": "css_class=pago",
                "scraped_at": ts,
                "source_url": self.url,
            }

        normal = dom.select_one(".avisos.normal")
        if normal:
            for nodo in normal.find_all(string=True, recursive=True):
                if isinstance(nodo, NavigableString) and nodo.strip():
                    yield {
                        "raw_text": str(nodo),
                        "selector": "css_class=avisos normal",
                        "scraped_at": ts,
                        "source_url": self.url,
                    }
        else:
            log.warning("Bloque .avisos.normal no encontrado en %s", self.url)

    def run_and_store(self) -> Path:
        records = list(self.run())
        if not records: log.warning("0 registros – no se graba."); return None
        ts = datetime.now().strftime("%Y%m%d")
        out = self.RAW_STORAGE_DIR / f"NewsPapAds_{ts}.jsonl"
        self._dump_jsonl(records, out)
        log.info("Grabados %s anuncios en %s", len(records), out)
        return out