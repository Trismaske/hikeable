import argparse
import json
import sys
import time
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs
import logging
from jsonschema import validate, ValidationError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config(config_file: str) -> Dict[str, Any]:
    """Load configuration from JSON file and validate."""
    config_schema = {
        "type": "object",
        "properties": {
            "source": {
                "type": "object",
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "enum": ["api"]},
                            "url": {"type": "string"},
                            "method": {"type": "string", "enum": ["GET", "POST"]},
                            "headers": {"type": "object"},
                            "query_params": {"type": "object"},
                            "auth": {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string", "enum": ["disabled", "api_key", "basic"]},
                                    "api_key": {"type": "string"},
                                    "username": {"type": "string"},
                                    "password": {"type": "string"}
                                },
                                "required": ["type"]
                            },
                            "required": ["type", "url", "method", "headers", "query_params", "auth"]
                        }
                    },
                    {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "enum": ["local_file"]},
                            "path": {"type": "string"},
                            "file_type": {"type": "string", "enum": ["csv", "json", "parquet", "xlsx", "xml"]},
                            "required": ["type", "path", "file_type"]
                        }
                    }
                ],
            },
            "schema": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "field_name_input": {"type": "string"},
                        "field_name_output": {"type": "string"},
                        "type_input": {"type": "string"},
                        "type_output": {"type": "string"},
                        "required": ["field_name_input", "field_name_output", "type_input", "type_output"]
                    }
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
                            "service_account_key_path": {"type": "string"},
                        },
                        "required": ["type", "project", "dataset", "table", "service_account_key_path"]
                    },
                    {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "enum": ["gcs"]},
                            "bucket": {"type": "string"},
                            "path": {"type": "string"},
                            "service_account_key_path": {"type": "string"},
                        },
                        "required": ["type", "bucket", "path", "service_account_key_path"]
                    },
                    {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "enum": ["local_file"]},
                            "path": {"type": "string"},
                            "file_type": {"type": "string", "enum": ["csv", "json", "parquet", "xlsx", "xml"]},
                            "required": ["type", "path", "file_type"]
                        }
                    }
                ]
            },
        }
    }
    try:
        with open(config_file, 'r') as f:
            config_data = json.load(f)
            validate(instance=config_data, schema=config_schema)
            return config_data

    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_file}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {config_file}.")
        raise
    except ValidationError as e:
        logger.error(f"Validation error in {config_file}: {e.message}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Retrieve data from a source and write it to a destination according to the passed config file."
    )
    parser.add_argument(
        'config_file',
        help='Path to JSON configuration file'
    )
    args = parser.parse_args()
    
    try:
        config = load_config(args.config_file)
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())