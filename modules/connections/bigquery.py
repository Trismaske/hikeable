import logging
import pandas

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from modules.config import Config
from modules.connections.local_file import BaseConnection

logger = logging.getLogger(__name__)

class BigQuery(BaseConnection):
    """Handles connections for Google BigQuery."""

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
        write_disposition = self.connection_config.get("write_disposition", "append")
        self.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if write_disposition == "truncate" or self.dedupe_using_all_columns else bigquery.WriteDisposition.WRITE_APPEND
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
        self.table_id = f"{self.dataset_id}.{self.table}"
        self.table_exists = False
        try:
            self.bq_client.get_table(self.table_id)
            self.table_exists = True
        except NotFound:
            if self.role == "destination":
                logger.info(f"Table \"{self.table_id}\" does not exist. It will be created upon loading data.")
            else:
                raise ValueError(f"Table \"{self.table_id}\" does not exist in dataset \"{self.dataset_id}\". Please create it before using it as a source.")
        logger.info(f"Initialized BigQuery connection for project: \"{self.project}\", dataset: \"{self.dataset}\", table: \"{self.table}\"")

    def _query_to_dataframe(self, query: str) -> pandas.DataFrame:
        """Execute a query and return the result as a DataFrame."""
        logger.info(f"Executing query: \"{query}\"")
        try:
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"Query returned {len(df)} rows and {len(df.columns)} columns.")
            return df
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def extract(self) -> pandas.DataFrame:
        """Extract data from BigQuery."""
        query = f"SELECT * FROM `{self.table_id}`"
        df = self._query_to_dataframe(query)
        self._process_dataframe(df)
        return df
    
    def load(self, df: pandas.DataFrame):
        """Load data to BigQuery."""
        logger.info(f"Loading data to BigQuery table: \"{self.table_id}\"")
        if self.primary_key and self.table_exists:
            try:
                existing_df = self._query_to_dataframe(f"SELECT * FROM `{self.table_id}`")
                if not existing_df.empty:
                    df = self._deduplicate_df(df, existing_df)
            except Exception as e:
                logger.warning(f"Could not query existing keys from BigQuery. Proceeding without deduplication. Error: {e}")

        if df.empty:
            logger.info("No new data to load after deduplication.")
            return

        job_config = bigquery.LoadJobConfig(
            write_disposition=self.write_disposition,
            source_format=bigquery.SourceFormat.PARQUET
        )
        job = self.bq_client.load_table_from_dataframe(df, self.table_id, job_config=job_config)
        job.result()
        table = self.bq_client.get_table(self.table_id)
        logger.info(f"Loaded {len(df)} rows to table (total rows: {table.num_rows}) to {self.table_id}")
