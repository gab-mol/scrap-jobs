from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from .session import build_session
import logging, requests

log = logging.getLogger(__name__)

class BaseScraper(ABC):

    url: str
    headers: dict = {}
    session: requests.Session | None = None
    parser: str = "html.parser"
    timeout: int | float = 10

    def run(self):
        html = self.fetch()
        dom: BeautifulSoup  = self.parse(html)
        
        return list(self.extract(dom))

    def fetch(self) -> str:
        s = self.session if self.session is not None else build_session()
        try:
            r = s.get(self.url, headers=self.headers, timeout=self.timeout)
            r.raise_for_status()
            log.info("GET a: %s", self.url)
            return r.text
        except requests.RequestException as exc:
            log.error("GET Fail %s → %s", self.url, exc)
            raise

    def parse(self, html: str):
        return BeautifulSoup(html, self.parser)

    @abstractmethod
    def extract(self, dom: BeautifulSoup):
        pass
