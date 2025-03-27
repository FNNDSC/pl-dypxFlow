import requests
from chrisclient import request
from loguru import logger
import sys

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
    def __init__(self, url: str, username: str, password: str):
        self.cl = request.Request(username, password)
        self.pacs_series_search_url = f"{url}search/"


    def get_pacs_files(self, params: dict):
        l_dir_path = set()
        resp = self.cl.get(self.pacs_series_search_url,params)
        LOG(resp)
        for item in resp.items:
            for link in item.links:
                folder = self.cl.get(link.href)
                for item_folder in folder.items:
                    path = item_folder.data.path.value
                    l_dir_path.add(path)
        return ','.join(l_dir_path)