import os
import logging
import pandas

from google.cloud import storage

from modules.config import Config
from modules.connections.local_file import LocalFile

logger = logging.getLogger(__name__)

class GCS(LocalFile):
    """Handles connections for Google Cloud Storage."""

    def __init__(self, role: str, config: Config):
        """Initialize the GCS connection."""
        super().__init__(role, config)
        self.bucket_name = self.connection_config.get("bucket")
        if not self.bucket_name:
            raise ValueError(f"Config for {self.role} must include a 'bucket' key.")
        self.gcs_path = self.connection_config.get("gcs_path")
        if not self.gcs_path:
            raise ValueError(f"Config for {self.role} must include a 'gcs_path' key.")
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(self.bucket_name)
        if not self.bucket.exists():
            self.bucket = self.storage_client.create_bucket(self.bucket_name)
        logger.info(f"Initialized GCS connection for bucket: \"{self.bucket_name}\"")

    def extract(self) -> pandas.DataFrame:
        """Download file from GCS and extract data."""
        logger.info(f"Downloading \"{self.gcs_path}\" from GCS bucket \"{self.bucket_name}\" to \"{self.file_path}\"")
        blob = self.bucket.blob(self.gcs_path)
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        blob.download_to_filename(self.file_path)
        logger.info("Download complete.")
        return super().extract()

    def load(self, df: pandas.DataFrame):
        """Load data to a local file and upload to GCS."""
        super().load(df)
        logger.info(f"Uploading \"{self.file_path}\" to GCS bucket \"{self.bucket_name}\" at \"{self.gcs_path}\"")
        blob = self.bucket.blob(self.gcs_path)
        blob.upload_from_filename(self.file_path)
        logger.info(f"Upload to \"{self.bucket_name}:{self.gcs_path}\" complete.")

    def upload_file(self, local_path: str, gcs_path: str):
        """Uploads a local file to GCS."""
        logger.info(f"Uploading \"{local_path}\" to GCS bucket \"{self.bucket_name}\" at \"{gcs_path}\"")
        blob = self.bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        logger.info(f"Upload to \"{self.bucket_name}:{gcs_path}\" complete.")