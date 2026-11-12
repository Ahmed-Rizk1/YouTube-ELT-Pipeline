import json 
import logging 

logger = logging.getLogger(__name__)

def load_data(file_path: str):
    """
    Reads and parses raw YouTube metrics JSON file.
    The file_path is injected from upstream task metadata (XCom or logical execution date)
    to guarantee idempotency during backfills.
    """
    try:
        logger.info(f"Processing file: {file_path}")

        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)

            return data 
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise

# Developed by Ahmed Rizk Gawish
