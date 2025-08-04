import argparse
import sys
import logging
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
        config = Config(args.config_file)
        logger.info("Configuration loaded successfully.")
        # You can now access config attributes like config.source, config.schema, etc.
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())