import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_data():

    file_path = f"./data/YT_data_API{date.today()}.json"

    try:
        logger.info(f"Processing data file: YT_data_API{date.today()}")

        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)

        return data

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise

    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from file: {file_path}")
        raise