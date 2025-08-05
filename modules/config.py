import json
import logging
from typing import Dict, Any, List

from jsonschema import validate, ValidationError

logger = logging.getLogger(__name__)

class Config:
    """Loads, validates, and provides access to the pipeline configuration."""

    _connection_definitions = {
        "api": {
            "type": "object",
            "properties": {
                "type": {"const": "api"},
                "url": {"type": "string", "format": "uri"},
                "method": {"type": "string", "enum": ["GET", "POST", "PUT", "PATCH"]},
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
                                "type": {"const": "basic"},
                                "username": {"type": "string"},
                                "password": {"type": "string"}
                            },
                            "required": ["username", "password"]
                        }
                    ]
                },
                "data_key": {"type": "string"},
            },
            "required": ["type", "url", "method", "auth"]
        },
        "local_file": {
            "type": "object",
            "properties": {
                "type": {"const": "local_file"},
                "file_path": {"type": "string"},
                "file_type": {"type": "string", "enum": ["csv", "json", "parquet", "xlsx", "xml"]},
                "file_type_options": {"type": "object"}
            },
            "required": ["type", "file_path", "file_type"]
        },
        "bigquery": {
            "type": "object",
            "properties": {
                "type": {"const": "bigquery"},
                "project": {"type": "string"},
                "dataset": {"type": "string"},
                "table": {"type": "string"},
                "service_account_key_path": {"type": "string"}
            },
            "required": ["type", "project", "dataset", "table"]
        },
        "gcs": {
            "type": "object",
            "properties": {
                "type": {"const": "gcs"},
                "file_path": {"type": "string"},
                "file_type": {"type": "string", "enum": ["csv", "json", "parquet", "xlsx", "xml"]},
                "file_type_options": {"type": "object"},
                "bucket": {"type": "string"},
                "gcs_path": {"type": "string"},
            },
            "required": ["type", "bucket", "gcs_path"]
        }
    }

    _config_schema = {
        "type": "object",
        "properties": {
            "source": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["api", "local_file", "bigquery", "gcs"]},
                },
                "oneOf": list(_connection_definitions.values()),
                "required": ["type"]
            },
            "schema": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "type": {"type": "string"},
                    },
                    "required": ["name", "type"]
                }
            },
            "destination": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["api", "local_file", "bigquery", "gcs"]},
                },
                "oneOf": list(_connection_definitions.values()),
                "required": ["type"]
            },
        },
        "required": ["source", "destination"]
    }

    def __init__(self, config_file_path: str):
        """
        Initializes the Config object by loading and validating the config file.

        Args:
            config_file_path: The path to the JSON configuration file.
        """
        self.config_file_path = config_file_path
        self.config_data = self._load_and_validate_config()
        logger.info("Configuration loaded and validated successfully.")

    def _load_and_validate_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file and validate."""
        try:
            with open(self.config_file_path, 'r') as f:
                config_data = json.load(f)
                validate(instance=config_data, schema=self._config_schema)
                return config_data
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_file_path}")
            raise
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {self.config_file_path}.")
            raise
        except ValidationError as e:
            logger.error(f"Validation error in {self.config_file_path}: {e.message}")
            raise

    def update_schema(self, new_schema: List[Dict[str, str]]):
        """Update the schema in the config file."""
        try:
            validate(instance=new_schema, schema=self._config_schema)
            self.config_data['schema'] = new_schema
            with open(self.config_file_path, 'w') as f:
                json.dump(self.config_data, f, indent=4)
            logger.info("Schema updated and saved to config file.")
        except ValidationError as e:
            logger.error(f"Validation error while updating schema: {e.message}")
            raise
        except Exception as e:
            logger.error(f"Error updating schema: {e}")
            raise
