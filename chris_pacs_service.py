import requests
from chrisclient import request
from loguru import logger
import sys
from requests.exceptions import RequestException, Timeout, HTTPError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from urllib.parse import urlencode

LOG = logger.debug

logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> │ "
    "<level>{level: <5}</level> │ "
    "<yellow>{name: >28}</yellow>::"
    "<cyan>{function: <30}</cyan> @"
    "<cyan>{line: <4}</cyan> ║ "
    "<level>{message}</level>"
)
logger.remove()
logger.add(sys.stderr, format=logger_format)


class PACSClient(object):
    def __init__(self, url: str, token: str):
        self.api_base = url.rstrip('/')
        self.auth = token
        self.headers = {"Content-Type": "application/json"}
        self.pacs_series_search_url = f"{url}search/"

    # --------------------------
    # Retryable request handler
    # --------------------------
    @retry(
        retry=retry_if_exception_type((RequestException, Timeout, HTTPError)),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        reraise=True
    )
    def make_request(self, method, endpoint, **kwargs):
        response = requests.request(method, endpoint, headers=self.headers, auth=self.auth, timeout=30, **kwargs)
        response.raise_for_status()

        try:
            return response.json()
        except ValueError:
            return response.text


    def get_pacs_files(self, params: dict):
        """
        Get PACS folder path
        """
        l_dir_path = set()
        query_string = urlencode(params)
        response = self.make_request("GET", f"{self.pacs_series_search_url}?{query_string}")
        for item in response.get("collection", {}).get("items", []):
            for link in item.get("links", []):
                folder = self.make_request("GET",link.get("href"))
                for item_folder in folder.get("collection", {}).get("items", []):
                    for field in item_folder.get("data", []):
                        if field.get("name") == "path":
                            l_dir_path.add(field.get("value"))

        return ','.join(l_dir_path)