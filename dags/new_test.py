from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

# Constants
API_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 200
BRONZE_PATH = Path("./data-lake/bronze/open_brewery_api")
SILVER_PATH = Path("./data-lake/silver/breweries")
GOLD_PATH = Path("./data-lake/gold/breweries_summary")
MAX_PAGES = 1000

# Default DAG arguments
default_args = {
    'owner': 'Data Engineering Team',
    'email_on_failure': True,
    "email": ["george.sousa.evm@gmail.com"],
    'retries': 0,
}

# Utility Functions
def fetch_brewery_page(page: int, per_page: int):
    import requests
    response = requests.get(API_URL, params={"page": page, "per_page": per_page})
    if response.status_code != 200:
        raise Exception(f"Failed to fetch page {page}: {response.status_code}")
    return response.json()

def get_latest_bronze_folder(bronze_base: Path) -> Path:
    folders = [f for f in bronze_base.iterdir() if f.is_dir()]
    return max(folders, key=lambda f: f.name)

# DAG definition
with DAG(
    dag_id="breweries_2_dag",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    schedule="0 0 * * *"
) as breweries_2_dag:

    @task()
    def extract_to_bronze(per_page: int = PER_PAGE, execution_date: str = None):
        import json
        from datetime import datetime

        all_breweries = []
        if not execution_date:
            execution_date = datetime.today().strftime("%Y-%m-%d")

        output_dir = BRONZE_PATH / execution_date
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "breweries.json"

        for page in range(1, MAX_PAGES + 1):
            try:
                breweries = fetch_brewery_page(page, per_page)
            except Exception as e:
                print(f"❌ Error fetching page {page}: {e}")
                break

            if not breweries:
                break

            all_breweries.extend(breweries)

            if len(breweries) < per_page:
                break

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_breweries, f, indent=2)

        print(f"✅ Extracted {len(all_breweries)} breweries to {output_file}")
        return len(all_breweries)

    @task()
    def transform_and_save_to_silver(execution_date: str = None):
        import json
        import pandas as pd
        from datetime import datetime
        from unidecode import unidecode

        if not execution_date:
            execution_date = datetime.today().strftime("%Y-%m-%d")

        input_file = BRONZE_PATH / execution_date / "breweries.json"

        with open(input_file, "r", encoding="utf-8") as f:
            breweries = json.load(f)

        df = pd.DataFrame(breweries)
        df['state'] = df['state'].apply(unidecode)
        df = df.drop(columns=["address_2", "address_3", "street"], errors="ignore")
        df = df.dropna(subset=["id", "state"])
        df["ingestion_date"] = execution_date

        for state, group_df in df.groupby("state"):
            output_dir = SILVER_PATH / f"state={state}"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{execution_date}.parquet"
            group_df.to_parquet(output_file, index=False)

        print(f"✅ Transformed {len(df)} records from bronze to silver.")

    def get_ingestion_dates(silver_path: Path):
        sample_state = next(silver_path.iterdir())
        return sorted({f.stem for f in sample_state.glob("*.parquet")})

    def load_silver_data_for_date(ingestion_date: str):
        import pandas as pd
        dfs = []
        for state_dir in SILVER_PATH.glob("state=*"):
            file = state_dir / f"{ingestion_date}.parquet"
            if file.exists():
                dfs.append(pd.read_parquet(file))
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    @task()
    def generate_gold_summary():
        import logging
        import pandas as pd

        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")

        try:
            ingestion_dates = get_ingestion_dates(SILVER_PATH)
        except Exception as e:
            logging.error(f"❌ Error listing ingestion dates: {e}")
            return

        for date in ingestion_dates:
            try:
                df = load_silver_data_for_date(date)

                if df.empty:
                    logging.warning(f"⚠️ No data found for {date}")
                    continue

                summary_df = (
                    df.groupby(["state", "brewery_type"], dropna=False)
                    .size()
                    .reset_index(name="brewery_count")
                )
                summary_df["date"] = date

                output_dir = GOLD_PATH / date
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = output_dir / "summary.parquet"
                summary_df.to_parquet(output_file, index=False)

                logging.info(f"✅ Gold summary written for {date} -> {output_file}")
            except Exception as e:
                logging.error(f"❌ Error processing gold summary for {date}: {e}")

    extract_to_bronze() >> transform_and_save_to_silver() >> generate_gold_summary()
