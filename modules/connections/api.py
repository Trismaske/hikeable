import time
import logging
from typing import Dict

import requests
import pandas as pd

from modules.config import Config
from modules.connections.base import BaseConnection

logger = logging.getLogger(__name__)

class API(BaseConnection):
    """Handles API connections for data extraction and loading."""

    def __init__(self, role: str, config: Config):
        """Initialize the API connection."""
        super().__init__(role, config)
        if 'url' not in self.connection_config:
            raise ValueError(f"Config for {self.role} must include a 'url' key.")
        elif 'method' not in self.connection_config:
            raise ValueError(f"Config for {self.role} must include a 'method' key.")
        elif 'auth' not in self.connection_config:
            raise ValueError(f"Config for {self.role} must include an 'auth' key.")
        self.url = self.connection_config.get("url", "")
        self.method = self.connection_config.get("method", "")
        self.headers = self.connection_config.get("headers", {})
        self.query_params = self.connection_config.get("query_params", {})
        self.body = self.connection_config.get("body", {})
        self.auth = self.connection_config.get("auth", {})
        self.data_key = self.connection_config.get("data_key", "")
        self.json_orientation = self.connection_config.get("json_orientation", "")
        logger.info(f"Initialized API connection with URL: \"{self.url}\" and method: \"{self.method}\"")

    def _make_request(self, url: str, headers: Dict, params: Dict, data: Dict) -> requests.Response:
        """Make a request to the API."""
        try:
            if self.auth.get("type") == "basic":
                headers['Authorization'] = f"Basic {self.auth['username']}:{self.auth['password']}"
            if self.method == "GET":
                response = requests.get(url, headers=headers, params=params)
            elif self.method == "POST":
                response = requests.post(url, headers=headers, params=params, json=data)
            elif self.method == "PUT":
                response = requests.put(url, headers=headers, params=params, json=data)
            elif self.method == "PATCH":
                response = requests.patch(url, headers=headers, params=params, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {self.method}")
            response.raise_for_status()
            logger.info("API request successful.")
            return response
        except requests.RequestException as e:
            logger.error(f"Error occurred while making API request: {e}")
            raise

    def _extract_data_from_response(self, response: requests.Response) -> pd.DataFrame:
        """Extract data from the API response."""
        try:
            data = response.json()

            if self.data_key and self.data_key in data:
                for level in self.data_key.split('.'):
                    data = data.get(level, {})

            if isinstance(data, list):
                return pd.DataFrame(data)
            elif isinstance(data, dict):
                if not self.json_orientation:
                    return pd.DataFrame([data])
                else:
                    return pd.DataFrame.from_dict(data, orient=self.json_orientation)
            else:
                raise ValueError("Unexpected data format in API response.")            
        except ValueError as e:
            logger.error(f"Error parsing JSON response: {e}")
            raise

    def extract(self) -> pd.DataFrame:
        """Extract data from the API."""
        logger.info(f"Extracting data from API at \"{self.url}\" with method \"{self.method}\"")
        # To Do: enable injection of values into headers, query parameters, and body
        # To Do: implement pagination handling
        response = self._make_request(self.url, headers=self.headers, params=self.query_params, data=self.body)
        logger.info("API request successful, processing response data.")
        data = self._extract_data_from_response(response)
        df = pd.DataFrame(data)
        self._process_dataframe(df)
        return df
    
    def load(self, df: pd.DataFrame):
        """Load data to the API."""
        logger.info(f"Loading data to API at \"{self.url}\" with method \"{self.method}\"")
        # To Do: enable injection of values into headers, query parameters, and body
        data_list = df.to_dict(orient='records')
        logger.info(f"WARNING: Unable to deduplicate data before loading to API. Ensure the API handles deduplication if necessary.")
        for record in data_list:
            self._make_request(self.url, headers=self.headers, params=self.query_params, data=record)
            time.sleep(0.1)
        logger.info("Data loaded successfully.")
