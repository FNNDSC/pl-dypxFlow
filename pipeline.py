import json
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException, Timeout, HTTPError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from loguru import logger


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

    def get_pipeline_parameters(self, pipeline_id: int) -> list[dict]:
        """Get default parameters for a pipeline."""
        logger.info(f"Fetching default parameters for pipeline with ID: {pipeline_id}")
        response = self.make_request("GET", f"/pipelines/{pipeline_id}/parameters/?limit=1000")
        return transform_plugin_data(response)

    def post_workflow(self, pipeline_id: int, previous_id: int, params: list[dict]) -> dict:
        """
        Trigger a pipeline workflow in CUBE.
        """
        payload = {
            "previous_plugin_inst_id": previous_id,
            "nodes_info": json.dumps(params)
        }
        return self.post_request( f"/pipelines/{pipeline_id}/workflows/", json=payload)

    def run_pipeline(self, pipeline_name: str, previous_inst: int, pipeline_params: dict):
        """
        Full workflow to:
        1. Fetch pipeline ID
        2. Get default parameters
        3. Update them
        4. Trigger the pipeline
        """
        try:
            pipeline_id = self.get_pipeline_id(pipeline_name)
            default_params = self.get_pipeline_parameters(pipeline_id)
            nodes_info = compute_workflow_nodes_info(default_params, include_all_defaults=True)
            updated_params = update_plugin_parameters(nodes_info, pipeline_params)
            workflow = self.post_workflow(pipeline_id=pipeline_id, previous_id=previous_inst, params=updated_params)
            logger.info(f"Workflow posted successfully")
            return {"status": "Pipeline running"}
        except Exception as ex:
            logger.error(f"Running pipeline failed due to: {ex}")
            return {"status": "Failed", "error": str(ex)}
