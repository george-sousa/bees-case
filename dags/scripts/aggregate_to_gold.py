import pandas as pd
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_ingestion_dates(silver_path: Path):
    try:
        sample_state = next(silver_path.iterdir())
        dates = sorted({f.stem for f in sample_state.glob("*.parquet")})
        logger.info(f"Found {len(dates)} ingestion dates in silver layer.")
        return dates
    except StopIteration:
        logger.error("No directories found in silver path.")
        return []
    except Exception as e:
        logger.error(f"Error while listing ingestion dates: {e}")
        return []


def load_silver_data_for_date(silver_path: Path, date: str):
    dfs = []
    try:
        for state_dir in silver_path.glob("state=*"):
            file = state_dir / f"{date}.parquet"
            if file.exists():
                dfs.append(pd.read_parquet(file))
            else:
                logger.warning(f"Missing file: {file}")
        if dfs:
            logger.info(f"Loaded data from {len(dfs)} state files for {date}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading data for {date}: {e}")
        return pd.DataFrame()


def generate_gold_summary(silver_path: str, gold_path: str):
    silver_path = Path(silver_path)
    gold_path = Path(gold_path)

    logger.info(f"Starting gold summary generation from {silver_path} to {gold_path}")
    dates = get_ingestion_dates(silver_path)
    total_dates = len(dates)

    if not dates:
        logger.warning("No ingestion dates found. Aborting gold summary generation.")
        return

    for i, date in enumerate(dates, 1):
        logger.info(f"[{i}/{total_dates}] Processing date: {date}")

        df = load_silver_data_for_date(silver_path, date)
        if df.empty:
            logger.warning(f"No data found for {date}, skipping.")
            continue

        try:
            summary_df = df.groupby(["state", "brewery_type"]).size().reset_index(name="brewery_count")
            summary_df["date"] = date

            output_dir = gold_path / date
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / "summary.parquet"

            summary_df.to_parquet(output_file, index=False)
            logger.info(f"[{i}/{total_dates}] Gold summary saved for {date} at {output_file}")
        except Exception as e:
            logger.error(f"Error generating or saving summary for {date}: {e}")

    logger.info("Gold summary generation completed.")
