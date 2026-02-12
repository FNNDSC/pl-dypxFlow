### Python Chris Client Implementation ###

from base_client import BaseClient
import json
import requests
from loguru import logger
import sys
from pipeline import Pipeline
from notification import Notification
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
                "recipients": params["notify"]["recipients"],
                "largeSequenceSize": params["relay"]["largeSequenceSize"],
                "largeSequencePollInterval": params["relay"]["largeSequencePollInterval"],
            }
        }
        d_ret = await pipe.run_pipeline(
            previous_inst = pv_id,
            pipeline_name = "PACS query, retrieve, registration verification, and run pipeline in CUBE 20250806",
            pipeline_params = plugin_params )
        return d_ret

    async def neuro_pull(self, neuro_location: str, sequence_filter: str, job_params: dict):
        """
        1. Pull data from the neuro tree
        2. Run anonymization pipeline to the root node
        """
        send_params: dict = job_params["push"]

        ntf = Notification(self.api_base, self.auth)
        neuro_plugin_id = ntf.get_plugin_id({"name": "pl-dircopy"})

        # Run pl-neuro_pull using filters
        neuro_inst_id = ntf.create_plugin_instance(neuro_plugin_id,
                                                   {"previous_id": 10,
                                                    "dir":"home/chris/uploads/data_upload-upload-38",
                                                    "title": sequence_filter}
                                                   )

        # Run anonymization pipeline
        pipe = Pipeline(self.api_base, self.auth)
        plugin_params = {
            'PACS-query': {
                "PACSurl": job_params["pull"]["url"],
                "PACSname": job_params["pull"]["pacs"],
                "PACSdirective": json.dumps(job_params["search"])
            },
            'send-dicoms-to-neuro-FS': {
                "path": f"{send_params['Dicom path']}/{send_params['Folder name']}/",
                "include": "*.dcm",
                "min_size": "0",
                "timeout": "0",
                "max_size": "1G",
                "max_depth": "3"
            },
            'send-anon-dicoms-to-neuro-FS': {
                "path": f"{send_params['Dicom anonymized path']}/{send_params['Folder name']}/",
                "include": "*.dcm",
                "min_size": "0",
                "timeout": "0",
                "max_size": "1G",
                "max_depth": "3"
            },
            'send-niftii-to-neuro-FS': {
                "path": f"{send_params['Nifti path']}/{send_params['Folder name']}/",
                "include": "*",
                "min_size": "0",
                "timeout": "0",
                "max_size": "1G",
                "max_depth": "3"
            },
            # additional parameters for pipeline
            'verify-registration': {
                "PACSname": job_params["pull"]["pacs"],
                "SMTPServer": job_params["notify"]["smtp_server"],
                "recipients": job_params["notify"]["recipients"]
            }
        }
        d_ret = await pipe.run_pipeline(
            previous_inst=neuro_inst_id,
            pipeline_name="DICOM anonymization, niftii conversion, and push to neuro tree v20250326",
            pipeline_params=plugin_params)
        return d_ret


def run_neuro_plugin(self, params: dict):
        pass
