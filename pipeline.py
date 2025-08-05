import argparse
import sys
import logging
from modules.config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def connection_factory(role: str, config: Config):
    """Factory function to create connection objects based on the config."""
    connection_type: str = config.config_data[role].get('type')
    if connection_type == 'api':
        from modules.connections.api import API
        return API(role, config)
    elif connection_type == 'local_file':
        from modules.connections.local_file import LocalFile
        return LocalFile(role, config)
    elif connection_type == 'gcs':
        from modules.connections.gcs import GCS
        return GCS(role, config)
    elif connection_type == 'bigquery':
        from modules.connections.bigquery import BigQuery
        return BigQuery(role, config)
    else:
        raise ValueError(f"Unsupported source connection_type: {connection_type}")


def main():
    parser = argparse.ArgumentParser(
        description="Retrieve data from a source and write it to a destination according to the passed config file."
    )
    parser.add_argument(
        'config_file_path',
        help='Path to JSON configuration file'
    )
    args = parser.parse_args()
    
    try:
        config = Config(args.config_file_path)
        source = connection_factory("source", config)
        destination = connection_factory("destination", config)
        data = source.extract()
        destination.load(data)
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())