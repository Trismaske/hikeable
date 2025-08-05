import logging
import pandas

from modules.config import Config
from modules.connections.base import BaseConnection

logger = logging.getLogger(__name__)

class LocalFile(BaseConnection):
    """Handles local file connections for data extraction and loading."""

    def __init__(self, role: str, config: Config):
        """Initialize the local file connection."""
        super().__init__(role, config)
        if 'path' not in self.connection_config:
            raise ValueError("Config for \"local_file\" must include a 'path' key.")
        if 'file_type' not in self.connection_config:
            raise ValueError("Config for \"local_file\" must include a 'file_type' key.")
        self.path: str = self.connection_config.get("path", "")
        self.file_type: str = self.connection_config.get("file_type", "")
        logger.info(f"Initialized local file connection with path: \"{self.path}\" and type: \"{self.file_type}\"")

    def extract(self) -> pandas.DataFrame:
        """Extract data from the local file."""
        # To Do: Implement file type specific parameters
        logger.info(f"Extracting data from \"{self.file_type}\" at \"{self.path}\"")
        if self.file_type == "csv":
            data = pandas.read_csv(self.path)
        elif self.file_type == "json":
            data = pandas.read_json(self.path)
        elif self.file_type == "parquet":
            data = pandas.read_parquet(self.path)
        elif self.file_type == "xlsx":
            data = pandas.read_excel(self.path)
        elif self.file_type == "xml":
            data = pandas.read_xml(self.path)
        else:
            raise ValueError(f"Unsupported file type: {self.file_type}")
        self._process_dataframe(data)
        return data

    def load(self, data: pandas.DataFrame):
        """Load data to the destination."""
        logger.info(f"Loading data to a \"{self.file_type}\" file at \"{self.path}\"")
        if self.file_type == "csv":
            data.to_csv(self.path, index=False)
        elif self.file_type == "json":
            data.to_json(self.path, orient='records', lines=True)
        elif self.file_type == "parquet":
            data.to_parquet(self.path, index=False)
        elif self.file_type == "xlsx":
            data.to_excel(self.path, index=False)
        elif self.file_type == "xml":
            data.to_xml(self.path, index=False)
        else:
            raise ValueError(f"Unsupported file type: {self.file_type}")