import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from unidecode import unidecode
import logging

logger = logging.getLogger(__name__)

def transform_bronze_to_silver(bronze_path: str, silver_path: str, execution_date: str = None):
    if not execution_date:
        execution_date = datetime.today().strftime("%Y-%m-%d")

    input_file = Path(bronze_path) / execution_date / "breweries.json"
    logger.info(f"Reading input file: {input_file}")

    try:
        with open(input_file, "r", encoding="utf-8") as f:
            breweries = json.load(f)
        logger.info(f"Loaded {len(breweries)} breweries from bronze data.")
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_file}")
        return 0
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from {input_file}: {e}")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error reading input file: {e}")
        return 0

    try:
        df_breweries = pd.DataFrame(breweries)
        logger.info(f"Initial DataFrame created with {len(df_breweries)} rows.")

        df_breweries['state'] = df_breweries['state'].apply(unidecode)
        df_breweries = df_breweries.dropna(subset=["id", "state"])
        df_breweries["ingestion_date"] = execution_date
        logger.info(f"Transformed DataFrame now has {len(df_breweries)} valid rows after cleaning.")
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        return 0

    states = df_breweries['state'].nunique()
    logger.info(f"Preparing to write data for {states} states to silver layer...")

    processed_states = 0
    total_saved = 0

    for state, group_df_breweries in df_breweries.groupby("state"):
        try:
            output_dir = Path(silver_path) / f"state={state}"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{execution_date}.parquet"

            group_df_breweries.to_parquet(output_file, index=False)
            logger.info(f"[{processed_states + 1}/{states}] Saved {len(group_df_breweries)} rows for state '{state}' to {output_file}")
            total_saved += len(group_df_breweries)
            processed_states += 1

        except Exception as e:
            logger.error(f"Failed to write parquet for state '{state}': {e}")

    logger.info(f"Saved transformed data for {total_saved} breweries to silver layer.")
    return total_saved
