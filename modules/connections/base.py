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

    def _deduplicate_df(self, df: pandas.DataFrame, existing_df: pandas.DataFrame) -> pandas.DataFrame:
        """Removes rows from a dataframe that have primary keys in the existing_df."""
        if not self.primary_key:
            return df

        if self.primary_key not in df.columns:
            logger.warning(f"Primary key '{self.primary_key}' not found in the dataframe. Skipping deduplication.")
            return df
        
        existing_keys = existing_df[self.primary_key].tolist()
        original_rows = len(df)
        df = df[~df[self.primary_key].isin(existing_keys)] # type: ignore
        rows_dropped = original_rows - len(df)
        if rows_dropped > 0:
            logger.info(f"Dropped {rows_dropped} records from the DataFrame as they already exist.")
        return df

    def _process_dataframe(self, df: pandas.DataFrame):
        """Process the dataframe according to the schema, changing the column names and types."""
        self._check_schema(df)
        # To Do: update the schema to allow for different input and output column names, types, transformations and data quality checks

    def extract(self) -> pandas.DataFrame:
        """Extract data from the source."""
        raise NotImplementedError("Subclasses must implement this method.")

    def load(self, df: pandas.DataFrame):
        """Load data to the destination."""
        raise NotImplementedError("Subclasses must implement this method.")