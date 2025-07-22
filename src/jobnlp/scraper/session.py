import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

def build_session(
    retries=3, backoff=0.5,
    ua="job-scraper/1.0 (+https://github.com/gab-mol/scrap-jobs.git)",
):
    retry = Retry(
        total=retries, backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=10)

    s = requests.Session()
    s.headers.update({"User-Agent": ua})
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s
