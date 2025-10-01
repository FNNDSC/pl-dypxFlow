import json
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException, Timeout, HTTPError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from loguru import logger
import time
import asyncio
from urllib.parse import urlencode


def transform_plugin_data(nested_data_list: list[dict]) -> list[dict]:
    """Flatten nested plugin data into a list of dictionaries."""
    flat_data = []
    for item in nested_data_list:
        entry = {pair['name']: pair['value'] for pair in item.get("data", [])}
        flat_data.append(entry)
    return flat_data


def update_plugin_parameters(d_piping: list[dict], plugin_params: dict) -> list[dict]:
    """
    Override default parameters in the pipeline with user-provided values.
    """
    for plugin_title, new_params in plugin_params.items():
        for piping in d_piping:
            if plugin_title in piping.get('title', ''):
                for param in piping.get('plugin_parameter_defaults', []):
                    if param['name'] in new_params:
                        param['default'] = new_params[param['name']]
    return d_piping


def compute_workflow_nodes_info(pipeline_default_parameters: list[dict], include_all_defaults=False) -> list[dict]:
    """
    Build nodes_info structure from flattened default parameters.
    """
    pipings_dict = {}
    for param in pipeline_default_parameters:
        piping_id = param['plugin_piping_id']

        if piping_id not in pipings_dict:
            pipings_dict[piping_id] = {
                'piping_id': piping_id,
                'previous_piping_id': param['previous_plugin_piping_id'],
                'title': param['plugin_piping_title'],
                'plugin_parameter_defaults': []
            }

        if param['value'] is None or include_all_defaults:
            pipings_dict[piping_id]['plugin_parameter_defaults'].append({
                'name': param['param_name'],
                'default': param['value']
            })

    # Clean up unused keys and prepare final list
    nodes_info = []
    for piping in pipings_dict.values():
        if not piping['plugin_parameter_defaults']:
            piping.pop('plugin_parameter_defaults', None)
        nodes_info.append(piping)

    return nodes_info


class Pipeline:
    def __init__(self, url: str, token: str):
        self.api_base = url.rstrip('/')
        self.headers = {"Content-Type": "application/json", "Authorization": f"Token {token}"}

    # --------------------------
    # Retryable request handler
    # --------------------------
    @retry(
        retry=retry_if_exception_type((RequestException, Timeout, HTTPError)),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        reraise=True
    )
    def make_request(self, method: str, endpoint: str, **kwargs):
        url = f"{self.api_base}{endpoint}"
        response = requests.request(method, url, headers=self.headers, timeout=30, **kwargs)
        response.raise_for_status()

        try:
            return response.json().get("collection", {}).get("items", [])
        except ValueError:
            return response.text

    def post_request(self, endpoint: str, **kwargs):
        url = f"{self.api_base}{endpoint}"
        response = requests.request("POST", url, headers=self.headers, timeout=30, **kwargs)
        response.raise_for_status()

        try:
            return response.json().get("collection", {}).get("items", [])
        except ValueError:
            return response.text

    # --------------------------
    # Pipeline helpers
    # --------------------------
    def get_pipeline_id(self, name: str) -> int:
        """Fetch pipeline ID by name."""
        logger.info(f"Fetching ID for pipeline: {name}")
        response = self.make_request("GET", f"/pipelines/search/?name={name}")

        for item in response:
            for field in item.get("data", []):
                if field.get("name") == "id":
                    return field.get("value")
        return -1

    def get_pipeline_total_pipings(self, pipeline_id: int) -> int:
        """Get the total number of plugin pipings in the given pipeline."""
        logger.info(f"Fetching pipeline plugin piping list.")
        response = self.make_request("GET", f"/pipelines/{pipeline_id}/pipings/?limit=100")
        return len(response)

    def get_pipeline_parameters(self, pipeline_id: int) -> list[dict]:
        """Get default parameters for a pipeline."""
        logger.info(f"Fetching default parameters for pipeline with ID: {pipeline_id}")
        response = self.make_request("GET", f"/pipelines/{pipeline_id}/parameters/?limit=1000")
        return transform_plugin_data(response)

    def post_workflow(self, pipeline_id: int, previous_id: int, params: list[dict]) -> int:
        """
        Trigger a pipeline workflow in CUBE.
        """
        payload = {
            "previous_plugin_inst_id": previous_id,
            "nodes_info": json.dumps(params)
        }
        response = self.post_request(f"/pipelines/{pipeline_id}/workflows/", json=payload)
        for item in response:
            for field in item.get("data", []):
                if field.get("name") == "id":
                    return field.get("value")
        return -1

    def _get_workflow_status(self, workflow_id: int) -> dict:
        """
        1. Get workflow details for a given workflow id.
        2. Check for errored jobs
        3. return total jobs (finished + errored + cancelled)
        """
        finished_jobs = 0
        errored_jobs = 0
        cancelled_jobs = 0
        created_jobs = 0
        waiting_jobs = 0
        scheduled_jobs = 0
        started_jobs = 0
        registering_jobs = 0

        logger.info(f"Fetching workflow details for ID: {workflow_id}")
        response = self.make_request("GET", f"/pipelines/workflows/{workflow_id}/")
        for item in response:
            for field in item.get("data", []):
                if field.get("name") == "finished_jobs":
                    finished_jobs = field.get("value")
                if field.get("name") == "errored_jobs":
                    errored_jobs = field.get("value")
                if field.get("name") == "cancelled_jobs":
                    cancelled_jobs = field.get("value")
                if field.get("name") == "created_jobs":
                    created_jobs = field.get("value")
                if field.get("name") == "waiting_jobs":
                    waiting_jobs = field.get("value")
                if field.get("name") == "scheduled_jobs":
                    scheduled_jobs = field.get("value")
                if field.get("name") == "started_jobs":
                    started_jobs = field.get("value")
                if field.get("name") == "registering_jobs":
                    registering_jobs = field.get("value")

        return {
            "finished_jobs": finished_jobs,
            "total_jobs": finished_jobs + errored_jobs + cancelled_jobs + created_jobs + waiting_jobs + scheduled_jobs + started_jobs + registering_jobs,
            "workflow_failed": (errored_jobs > 0)
        }

    async def monitor_pipeline(self, workflow_id, total_jobs, pv_inst, rcpts, smtp, search_data):
        d_search_data = json.loads(search_data)
        while True:
            status = self._get_workflow_status(workflow_id)
            if status["workflow_failed"]:
                logger.error("Pipeline failed.")
                self.run_notification_plugin(pv_inst, "Pipeline failed with errors", rcpts, smtp, d_search_data)
                break
            if status["finished_jobs"] >= total_jobs:
                logger.info("Pipeline complete.")
                break
            if status["total_jobs"] < total_jobs:
                self.run_notification_plugin(pv_inst, "Nodes deleted in pipeline", rcpts, smtp, d_search_data)
                break
            time.sleep(20)

    def run_notification_plugin(self, pv_id: int, msg: str, rcpts: str, smtp: str, search_data) -> int:
        """
        Run the pl-notification plugin.
        """
        search_data = json.loads(search_data)
        email_content = (f"An error occurred while running pacs-pull pipeline on the following data: "
                         f"\nMRN: {search_data['PatientID']} "
                         f"\nStudyDate: {search_data['StudyDate']}"
                         f"\nModality: {search_data['Modality']}"
                         f"\n\nKindly login to ChRIS to access the logs for more details.")

        try:
            plugin_id = self._get_plugin_id({"name": "pl-notification", "version": "0.1.0"})
            instance_id = self._create_plugin_instance(plugin_id, {
                "previous_id": pv_id,
                "content": email_content,
                "title": "pipeline-error",
                "rcpt": rcpts,
                "sender": "noreply@fnndsc.org",
                "mail_server": smtp
            })
            return int(instance_id)
        except Exception as ex:
            logger.error(f"Error occurred while creating notification instance {ex}")
            return -1

    def run_error_plugin(self, pv_id: int):
        """
        Run pl-error plugin
        """
        pass


    def _create_plugin_instance(self, plugin_id: str, params: dict):
        """
        Create a plugin instance and return its ID.
        """
        response = self.post_request(f"/plugins/{plugin_id}/instances/", json=params)

        for item in response:
            for field in item.get("data", []):
                if field.get("name") == "id":
                    return field.get("value")

        raise RuntimeError("Plugin instance could not be scheduled.")

    def _get_plugin_id(self, params: dict):
        """
        Fetch plugin ID by search parameters.
        """
        query_string = urlencode(params)
        response = self.make_request("GET", f"/plugins/search/?{query_string}")

        for item in response:
            for field in item.get("data", []):
                if field.get("name") == "id":
                    return field.get("value")

        raise RuntimeError(f"No plugin found with matching criteria: {params}")

    async def run_pipeline(self, pipeline_name: str, previous_inst: int, pipeline_params: dict):
        """
        Full workflow to:
        1. Fetch pipeline ID
        2. Get default parameters
        3. Update them
        4. Trigger the pipeline
        """
        smtp_server = pipeline_params["verify-registration"]["SMTPServer"]
        recipients = pipeline_params["verify-registration"]["recipients"]
        search_data = pipeline_params["PACS-query"]["PACSdirective"]
        search_data = json.dumps(search_data)
        try:
            pipeline_id = self.get_pipeline_id(pipeline_name)
            total_jobs = self.get_pipeline_total_pipings(pipeline_id)
            default_params = self.get_pipeline_parameters(pipeline_id)
            nodes_info = compute_workflow_nodes_info(default_params, include_all_defaults=True)
            updated_params = update_plugin_parameters(nodes_info, pipeline_params)
            workflow_id = self.post_workflow(pipeline_id=pipeline_id, previous_id=previous_inst, params=updated_params)

            # Start this in the background (not awaited)
            asyncio.create_task(
                self.monitor_pipeline(workflow_id, total_jobs, previous_inst, recipients, smtp_server, search_data))

            logger.info(f"Workflow posted successfully")
            return {"status": "Pipeline running"}
        except Exception as ex:
            logger.error(f"Running pipeline failed due to: {ex}")
            return {"status": "Failed", "error": str(ex)}
