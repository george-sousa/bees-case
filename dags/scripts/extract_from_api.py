import requests
import json
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

API_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 200
MAX_PAGES = 1000 # safety limit

def fetch_brewery_page(page: int, per_page: int = PER_PAGE):
    try:
        logger.debug(f"Fetching page {page}")
        response = requests.get(API_URL, params={"page": page, "per_page": per_page}, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error while fetching page {page}: {e}")
    except requests.exceptions.ConnectionError as e:
        logger.warning(f"Connection error on page {page}: {e}")
    except requests.exceptions.Timeout as e:
        logger.warning(f"Timeout error on page {page}: {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed on page {page}: {e}")
    return {}

def extract_breweries_to_bronze(bronze_path: str, execution_date: str = None):
    if not execution_date:
        execution_date = datetime.today().strftime("%Y-%m-%d")

    output_dir = Path(bronze_path) / execution_date
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory created: {output_dir}")
    except Exception as e:
        logger.error(f"Failed to create output directory {output_dir}: {e}")
        raise

    output_file = output_dir / "breweries.json"
    all_breweries = []

    try:
        for page in range(1, MAX_PAGES + 1):
            logger.info(f"Processing page {page}...")
            breweries = fetch_brewery_page(page)
            if not breweries:
                logger.info("No more breweries found. Stopping pagination.")
                break
            all_breweries.extend(breweries)
            logger.info(f"Page {page} fetched: {len(breweries)} breweries. Total so far: {len(all_breweries)}")
            if len(breweries) < PER_PAGE:
                logger.info("Last page reached (less than PER_PAGE items).")
                break
    except Exception as e:
        logger.error(f"Unexpected error during pagination: {e}")
        raise

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_breweries, f, indent=2)
        logger.info(f"{len(all_breweries)} breweries saved to {output_file}")
    except Exception as e:
        logger.error(f"Failed to write output file {output_file}: {e}")
        raise

    return len(all_breweries)
