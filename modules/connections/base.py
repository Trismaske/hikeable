import logging
import pandas
from typing import Dict, Any, List

from modules.config import Config

logger = logging.getLogger(__name__)

class BaseConnection:
    """Base class for all connections."""

    def __init__(self, role: str, config: Config):
        """Initialize the connection with configuration."""
        if role not in ['source', 'destination']:
            raise ValueError("Role must be either 'source' or 'destination'.")
        self.role: str = role
        self.config: Config = config
        self.connection_config: Dict[str, Any] = config.config_data[role]
        self.primary_key: str = self.connection_config.get("primary_key", "")
        self.dedupe_using_all_columns: bool = self.connection_config.get("dedupe_using_all_columns", False)

    def _get_schema_for_column(self, column: str) -> Dict[str, str]:
        """Get the schema for a specific column."""
        for field in self.config.config_data['schema']:
            if field['name'] == column:
                return field
        raise ValueError(f"Column '{column}' not found in schema.")

    def _generate_schema(self, df: pandas.DataFrame):
        """Generate a schema based on the dataframe and save the newly generated schema to the config file."""
        new_schema: List[Dict[str, str]] = []
        for column in df.columns:
            new_schema.append({
                "name": column,
                "type": str(df[column].dtype)
            })
        self.config.update_schema(new_schema)
        logger.info("Schema generated and updated.")

    def _check_schema(self, df: pandas.DataFrame):
        """Check if the schema exists and matches the dataframe. If the schema does not exist, generate it."""
        if not self.config.config_data.get('schema') or len(self.config.config_data['schema']) == 0:
            self._generate_schema(df)
        else:
            found_columns = []
            for column in df.columns:
                self._get_schema_for_column(column)
                found_columns.append(column)
            if set(found_columns) != set([field['name'] for field in self.config.config_data['schema']]):
                raise ValueError("Dataframe columns do not match the schema.")

    def _process_dataframe(self, df: pandas.DataFrame):
        """Process the dataframe according to the schema, changing the column names and types."""
        self._check_schema(df)
        # To Do: update the schema to allow for different input and output column names, types, transformations and data quality checks

    def _deduplicate_df(self, df: pandas.DataFrame, existing_df: pandas.DataFrame) -> pandas.DataFrame:
        """Merges a new dataframe with an existing one, handling duplicates and updates."""
        rows_new = len(df)
        rows_existing = len(existing_df)
        logger.info(f"Merging new dataframe with {rows_new} rows and existing dataframe with {rows_existing} rows (Total rows: {rows_new + rows_existing}).")
        merged_df = pandas.concat([existing_df, df], ignore_index=True)
        rows_total_before_deduplication = len(merged_df)
        if not self.primary_key and not self.dedupe_using_all_columns:
            logger.info(f"No primary key or deduplication strategy set. Returning a simple concatenation of dataframes (Final rows: {rows_total_before_deduplication}).")
            return merged_df
        deduplicated_df = merged_df
        if self.dedupe_using_all_columns:
            logger.info("Deduplicating merged dataframe based on all columns.")
            rows_before_all_cols_deduplication = len(deduplicated_df)
            deduplicated_df = deduplicated_df.drop_duplicates(keep='last')
            rows_after_all_cols_deduplication = len(deduplicated_df)
            logger.info(f"Dropped {rows_before_all_cols_deduplication - rows_after_all_cols_deduplication} duplicates based on all columns. Processed dataframe now has {rows_after_all_cols_deduplication} rows.")
        if self.primary_key:
            logger.info(f"Deduplicating processed dataframe based on primary key: \"{self.primary_key}\"")
            if self.primary_key not in df.columns:
                logger.warning(f"Primary key '{self.primary_key}' not found in the new dataframe. Skipping primary key deduplication.")
            elif self.primary_key not in existing_df.columns:
                logger.warning(f"Primary key '{self.primary_key}' not found in the existing data. Skipping primary key deduplication.")
            else:
                rows_before_pk_deduplication = len(deduplicated_df)
                deduplicated_df = deduplicated_df.drop_duplicates(subset=[self.primary_key], keep='last')
                rows_after_pk_deduplication = len(deduplicated_df)
                logger.info(f"Dropped {rows_before_pk_deduplication - rows_after_pk_deduplication} duplicates based on primary key '{self.primary_key}'. Processed dataframe now has {rows_after_pk_deduplication} rows.")
        logger.info(f"Dropped {rows_total_before_deduplication - len(deduplicated_df)} rows in total. Final rows: {len(deduplicated_df)}")
        return deduplicated_df.reset_index(drop=True)

    def extract(self) -> pandas.DataFrame:
        """Extract data from the source."""
        raise NotImplementedError("Subclasses must implement this method.")

    def load(self, df: pandas.DataFrame):
        """Load data to the destination."""
        raise NotImplementedError("Subclasses must implement this method.")