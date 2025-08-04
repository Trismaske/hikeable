import json
import logging
from typing import Dict, Any, List

from jsonschema import validate, ValidationError

logger = logging.getLogger(__name__)

class Config:
    """Loads, validates, and provides access to the pipeline configuration."""

    _config_schema = {
        "type": "object",
        "properties": {
            "source": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["api", "local_file"]},
                },
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "enum": ["api"]},
                            "url": {"type": "string", "format": "uri"},
                            "method": {"type": "string", "enum": ["GET", "POST"]},
                            "headers": {"type": "object"},
                            "query_params": {"type": "object"},
                            "auth": {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string", "enum": ["disabled", "api_key", "basic"]},
                                },
                                "required": ["type"],
                                "oneOf": [
                                    {"properties": {"type": {"const": "disabled"}}},
                                    {
                                        "properties": {
                                            "type": {"const": "api_key"},
                                            "api_key": {"type": "string"}
                                        },
                                        "required": ["api_key"]
                                    },
                                    {
                                        "properties": {
                                            "type": {"const": "basic"},
                                            "username": {"type": "string"},
                                            "password": {"type": "string"}
                                        },
                                        "required": ["username", "password"]
                                    }
                                ]
                            }
                        },
                        "required": ["type", "url", "method", "auth"]
                    },
                    {
                        "type": "object",
                        "properties": {
                            "type": {"const": "local_file"},
                            "path": {"type": "string"},
                            "file_type": {"type": "string", "enum": ["csv", "json", "parquet", "xlsx", "xml"]}
                        },
                        "required": ["type", "path", "file_type"]
                    }
                ],
                "required": ["type"]
            },
            "schema": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "field_name_input": {"type": "string"},
                        "field_name_output": {"type": "string"},
                        "type_input": {"type": "string"},
                        "type_output": {"type": "string"}
                    },
                    "required": ["field_name_input", "field_name_output", "type_input", "type_output"]
                }
            },
            "destination": {
                "type": "object",
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "enum": ["bigquery"]},
                            "project": {"type": "string"},
                            "dataset": {"type": "string"},
                            "table": {"type": "string"},
                            "service_account_key_path": {"type": "string"}
                        },
                        "required": ["type", "project", "dataset", "table"]
                    },
                    {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "enum": ["gcs"]},
                            "bucket": {"type": "string"},
                            "path": {"type": "string"},
                        },
                        "required": ["type", "bucket", "path"]
                    },
                    {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "enum": ["local_file"]},
                            "path": {"type": "string"},
                            "file_type": {"type": "string", "enum": ["csv", "json", "parquet", "xlsx", "xml"]}
                        },
                        "required": ["type", "path", "file_type"]
                    }
                ]
            },
        },
        "required": ["source", "schema", "destination"]
    }

    def __init__(self, config_file: str):
        """
        Initializes the Config object by loading and validating the config file.

        Args:
            config_file: The path to the JSON configuration file.
        """
        self.config_file = config_file
        self._config_data = self._load_and_validate_config()
        
        self.source: Dict[str, Any] = self._config_data['source']
        self.schema: List[Dict[str, str]] = self._config_data['schema']
        self.destination: Dict[str, Any] = self._config_data['destination']

    def _load_and_validate_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file and validate."""
        try:
            with open(self.config_file, 'r') as f:
                config_data = json.load(f)
                validate(instance=config_data, schema=self._config_schema)
                logger.info("Configuration loaded and validated successfully.")
                return config_data
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_file}")
            raise
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {self.config_file}.")
            raise
        except ValidationError as e:
            logger.error(f"Validation error in {self.config_file}: {e.message}")
            raise
