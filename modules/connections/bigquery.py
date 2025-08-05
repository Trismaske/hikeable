import logging
import pandas

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from modules.config import Config
from modules.connections.local_file import BaseConnection

logger = logging.getLogger(__name__)

class BigQuery(BaseConnection):
    """Handles connections for Google BigQuery."""
    primary_key: str | None

    def __init__(self, role: str, config: Config):
        """Initialize the BigQuery connection."""
        super().__init__(role, config)
        self.multi_region_location = self.connection_config.get("multi_region_location", "EU")
        self.project = self.connection_config.get("project")
        if not self.project:
            raise ValueError(f"Config for {self.role} must include a 'project' key.")
        dataset = self.connection_config.get("dataset")
        if not dataset:
            raise ValueError(f"Config for {self.role} must include a 'dataset' key.")
        self.table = self.connection_config.get("table")
        if not self.table:
            raise ValueError(f"Config for {self.role} must include a 'table' key.")
        self.primary_key = self.connection_config.get("primary_key")
        write_disposition = self.connection_config.get("write_disposition", "append")
        self.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if write_disposition == "truncate" else bigquery.WriteDisposition.WRITE_APPEND
        self.dataset_id = f"{self.project}.{dataset}"
        self.bq_client = bigquery.Client(project=self.project)
        try:
            self.dataset = self.bq_client.get_dataset(self.dataset_id)
        except NotFound:
            logger.info(f"Creating Dataset \"{self.dataset_id}\" in project \"{self.project}\".")
            self.dataset = bigquery.Dataset(self.dataset_id)
            self.dataset.location = self.multi_region_location
            try:
                self.dataset = self.bq_client.create_dataset(self.dataset)
                logger.info(f"Created dataset \"{self.dataset.dataset_id}\" in location \"{self.dataset.location}\"")
            except Exception as e:
                logger.error(f"Error creating dataset \"{self.dataset.dataset_id}\" in location \"{self.dataset.location}\": {e}")
        logger.info(f"Initialized BigQuery connection for project: \"{self.project}\", dataset: \"{self.dataset}\", table: \"{self.table}\"")

    def extract(self) -> pandas.DataFrame:
        """Extract data from BigQuery."""
        query = f"SELECT * FROM `{self.dataset_id}.{self.table}`"
        logger.info(f"Executing query: \"{query}\"")
        df = self.bq_client.query(query).to_dataframe()
        self._process_dataframe(df)
        return df
    
    def load(self, df: pandas.DataFrame):
        """Load data to BigQuery."""
        table_id = f"{self.dataset_id}.{self.table}"
        logger.info(f"Loading data to BigQuery table: \"{table_id}\"")

        try:
            self.bq_client.get_table(table_id)
            table_exists = True
        except NotFound:
            table_exists = False

        if self.primary_key and table_exists:
            logger.info(f"Deduplicating data based on primary key: {self.primary_key}")
            query = f"SELECT {self.primary_key} FROM `{table_id}`"
            try:
                existing_keys_df = self.bq_client.query(query).to_dataframe()
                if not existing_keys_df.empty and self.primary_key in existing_keys_df.columns:
                    existing_keys = existing_keys_df[self.primary_key].tolist()
                    original_rows = len(df)
                    df = df[~df[self.primary_key].isin(existing_keys)]
                    rows_dropped = original_rows - len(df)
                    if rows_dropped > 0:
                        logger.info(f"Dropped {rows_dropped} rows with existing primary keys.")
            except Exception as e:
                logger.warning(f"Could not query existing keys from BigQuery. Proceeding without deduplication. Error: {e}")

        if df.empty:
            logger.info("No new data to load after deduplication.")
            return

        job_config = bigquery.LoadJobConfig(
            write_disposition=self.write_disposition,
            source_format=bigquery.SourceFormat.PARQUET
        )
        job = self.bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        table = self.bq_client.get_table(table_id)
        logger.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
