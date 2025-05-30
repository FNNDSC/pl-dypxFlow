### Python Chris Client Implementation ###

from base_client import BaseClient
from chrisclient import client
from chris_pacs_service import PACSClient
import json
import time
from loguru import logger
import sys
from pipeline import Pipeline
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

class ChrisClient(BaseClient):
    def __init__(self, url: str, username: str, password: str):
        self.cl = client.Client(url, username, password)
        self.cl.pacs_series_url = f"{url}pacs/series/"
        self.req = PACSClient(self.cl.pacs_series_url,username,password)

    def create_con(self,params:dict):
        return self.cl

    def health_check(self):
        return self.cl.get_chris_instance()

    def pacs_pull(self):
        pass
    def pacs_push(self):
        pass
    def anonymize(self, params: dict, pv_id: int):
        pipe = Pipeline(self.cl)
        plugin_params = {
            'PACS-query': {
                "PACSurl": params["pull"]["url"],
                "PACSname": params["pull"]["pacs"],
                "PACSdirective": json.dumps(params["search"])
            },
            'PACS-retrieve': {
                "PACSurl": params["pull"]["url"],
                "PACSname": params["pull"]["pacs"],
                "inputJSONfile": "search_results.json",
                "copyInputFile": True
            },
            'verify-registration': {
                "CUBEurl": self.cl.url,
                "CUBEuser": self.cl.username,
                "CUBEpassword": self.cl.password,
                "inputJSONfile": "search_results.json",
                "folderName": params["push"]["Folder name"],
                "neuroDicomLocation": params["push"]["Dicom path"],
                "neuroAnonLocation": params["push"]["Dicom anonymized path"],
                "neuroNiftiLocation": params["push"]["Nifti path"],
                "PACSurl": params["pull"]["url"],
                "PACSname": params["pull"]["pacs"],
            }
        }
        d_ret = pipe.flow_executeAndBlockUntilNodeComplete(
            attachToNodeID = pv_id,
            workflowTitle = "PACS query, retrieve, registration verification, and run pipeline in CUBE 20250331",
            waitForNodeWithTitle = "verify-registration",
            totalPolls = 100,
            pluginParameters = plugin_params )
        return d_ret