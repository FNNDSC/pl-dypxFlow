import json
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException, Timeout, HTTPError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from loguru import logger
import time
import asyncio
from urllib.parse import urlencode

class Notification:
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

    def get_feed_id_from_plugin_inst(self, plugin_inst: int) -> int:
        """Get feed_id from a given plugin instance"""
        logger.info(f"Fetching feed id for plugin instance with ID: {plugin_inst}")
        response = self.make_request("GET",f"/plugins/instances/{plugin_inst}/")
        for item in response:
            for field in item.get("data", []):
                if field.get("name") == "feed_id":
                    return field.get("value")
        return -1

    def get_feed_details_from_id(self, feed_id: int) -> dict:
        """Get feed details given a feed id"""
        feed_details = {}

        logger.info(f"Getting feed details for ID: {feed_id}")
        response = self.make_request("GET",f"/{feed_id}/")
        for item in response:
            for field in item.get("data", []):
                if field.get("name") == "creation_date":
                    feed_details["date"] = field.get("value")
                if field.get("name") == "name":
                    feed_details["name"] = field.get("value")
                if field.get("name") == "owner_username":
                    feed_details["owner"] = field.get("value")

        return feed_details

    def run_notification_plugin(self, pv_id: int, msg: str, rcpts: str, smtp: str, search_data: str) -> int:
        """
        Run the pl-notification plugin.
        """
        feed_id = self.get_feed_id_from_plugin_inst(pv_id)
        feed_details = self.get_feed_details_from_id(feed_id)
        email_content = (f"Your workflow is now complete."
                         f"\nFeed Name: {feed_details['name']}"
                         f"\nDate: {feed_details['date']}"
                         f"\n\nKindly login to ChRIS as *{feed_details['owner']}* to access the logs for more details.")

        try:
            plugin_id = self.get_plugin_id({"name": "pl-notification", "version": "0.1.0"})
            instance_id = self.create_plugin_instance(plugin_id, {
                "previous_id": pv_id,
                "content": email_content,
                "title": f"Analysis {feed_details['name']} is complete.",
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


    def create_plugin_instance(self, plugin_id: str, params: dict):
        """
        Create a plugin instance and return its ID.
        """
        response = self.post_request(f"/plugins/{plugin_id}/instances/", json=params)
        feed_id = -1

        for item in response:
            for field in item.get("data", []):
                if field.get("name") == "id":
                    return field.get("value")

        raise RuntimeError("Plugin instance could not be scheduled.")


    def get_plugin_id(self, params: dict):
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