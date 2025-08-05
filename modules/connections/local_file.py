import os
import logging
import pandas
from typing import Dict

from modules.config import Config
from modules.connections.base import BaseConnection

logger = logging.getLogger(__name__)

class LocalFile(BaseConnection):
    """Handles local file connections for data extraction and loading."""

    def __init__(self, role: str, config: Config):
        """Initialize the LocalFile connection."""
        super().__init__(role, config)
        self.file_path: str = self.connection_config.get("file_path", "")
        if not self.file_path:
            raise ValueError(f"Config for {self.role} must include a 'file_path' key.")
        self.file_type: str = self.connection_config.get("file_type", "")
        if self.file_type not in ["csv", "json", "parquet", "xlsx", "xml"]:
            raise ValueError(f"Unsupported file type: \"{self.file_type}\"")
        self.file_type_options: Dict = self.connection_config.get("file_type_options", {})
        logger.info(f"Initialized local file connection with path: \"{self.file_path}\" and type: \"{self.file_type}\"")
        
    def _path_to_dataframe(self, path: str) -> pandas.DataFrame:
        """Convert a file path to a DataFrame based on the file type."""
        if self.file_type == "csv":
            return pandas.read_csv(path, **self.file_type_options)
        elif self.file_type == "json":
            return pandas.read_json(path, **self.file_type_options)
        elif self.file_type == "parquet":
            return pandas.read_parquet(path, **self.file_type_options)
        elif self.file_type == "xlsx":
            return pandas.read_excel(path, **self.file_type_options)
        elif self.file_type == "xml":
            return pandas.read_xml(path, **self.file_type_options)
        else:
            raise ValueError(f"Unsupported file type: {self.file_type}")

    def extract(self) -> pandas.DataFrame:
        """Extract data from a local file."""
        logger.info(f"Extracting data from \"{self.file_type}\" at \"{self.file_path}\"")
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found at \"{self.file_path}\"")
        df = self._path_to_dataframe(self.file_path)
        self._process_dataframe(df)
        return df

    def load(self, df: pandas.DataFrame):
        """Load data to a local file."""
        logger.info(f"Loading data to a \"{self.file_type}\" file at \"{self.file_path}\"")
        if self.primary_key and os.path.exists(self.file_path):
            logger.info(f"File exists at \"{self.file_path}\". Deduplicating data based on primary key: {self.primary_key}")
            existing_df = self._path_to_dataframe(self.file_path)
            if not existing_df.empty and self.primary_key in existing_df.columns:
                df = self._deduplicate_df(df, existing_df)
            df = pandas.concat([existing_df, df], ignore_index=True)
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        if self.file_type == "csv":
            df.to_csv(self.file_path, index=False, **self.file_type_options)
        elif self.file_type == "json":
            df.to_json(self.file_path, orient='records', lines=True, **self.file_type_options)
        elif self.file_type == "parquet":
            df.to_parquet(self.file_path, index=False, **self.file_type_options)
        elif self.file_type == "xlsx":
            df.to_excel(self.file_path, index=False, **self.file_type_options)
        elif self.file_type == "xml":
            df.to_xml(self.file_path, index=False, **self.file_type_options)
        else:
            raise ValueError(f"Unsupported file type: {self.file_type}")