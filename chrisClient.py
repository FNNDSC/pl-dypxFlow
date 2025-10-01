### Python Chris Client Implementation ###

from base_client import BaseClient
import json
import requests
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
    def __init__(self, url: str, token: str):
        self.api_base = url.rstrip('/')
        self.auth = token
        self.headers = {"Content-Type": "application/json", "Authorization": f"Token {token}"}
        self.pacs_series_url = f"{url}/pacs/series/"

    def health_check(self):
        endpoint = f"{self.api_base}/"
        response = requests.request("GET", endpoint, headers=self.headers, timeout=30)

        response.raise_for_status()

        try:
            return response.json()
        except ValueError:
            return response.text

    def pacs_pull(self):
        pass
    def pacs_push(self):
        pass
    async def anonymize(self, params: dict, pv_id: int):
        pipe = Pipeline(self.api_base, self.auth)
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
                "CUBEurl": self.api_base,
                "inputJSONfile": "search_results.json",
                "folderName": params["push"]["Folder name"],
                "neuroDicomLocation": params["push"]["Dicom path"],
                "neuroAnonLocation": params["push"]["Dicom anonymized path"],
                "neuroNiftiLocation": params["push"]["Nifti path"],
                "PACSurl": params["pull"]["url"],
                "PACSname": params["pull"]["pacs"],
                "SMTPServer": params["notify"]["smtp_server"],
                "recipients": params["notify"]["recipients"]
            }
        }
        d_ret = await pipe.run_pipeline(
            previous_inst = pv_id,
            pipeline_name = "PACS query, retrieve, registration verification, and run pipeline in CUBE 20250806",
            pipeline_params = plugin_params )
        return d_ret