from datetime import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
# from airflow import PythonOperator
from pathlib import Path

# Constants
API_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 200  # max is 200 per the API
BRONZE_PATH = "./data-lake/bronze/open_brewery_api"
SILVER_PATH = "./data-lake/silver/breweries"
GOLD_PATH = "./data-lake/gold/breweries_summary"
MAX_PAGES = 1000  # safety limit

# s3 = boto3.client("s3")


# def fetch_brewery_page(page: int, per_page: int = PER_PAGE):
def fetch_brewery_page(page: int, per_page: int):
    import requests
    response = requests.get(API_URL, params={"page": page, "per_page": per_page})
    if response.status_code != 200:
        raise Exception(f"Failed to fetch page {page}: {response.status_code}")
    return response.json()

# Auto-detect the latest ingestion folder (e.g., 2025-05-13)
def get_latest_bronze_folder(bronze_base: Path) -> Path:
    folders = [f for f in bronze_base.iterdir() if f.is_dir()]
    latest_folder = max(folders, key=lambda f: f.name)
    return latest_folder

# Default arguments
default_args = {
    'owner': 'Data Engineering Team',
    'email_on_failure': True,  # We'll handle failures with a custom callback
    "email": ["george.sousa.evm@gmail.com"],
    'retries': 0,
}

# A DAG represents a workflow, a collection of tasks
with DAG(
    dag_id="breweries_dag",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    schedule="0 0 * * *"
    ) as breweries_dag:

    @task()
    def extract_to_bronze(per_page: int = PER_PAGE, execution_date: str = None):
        """
        Main extraction function. Fetches all brewery pages and stores in S3.
        Parameters:
            execution_date (str): ISO format date (YYYY-MM-DD). Defaults to today.
        """
        
        import json
        import os
        from datetime import datetime
        from pathlib import Path

        number_of_breweries_found = 0
        all_breweries = []
        if not execution_date:
            execution_date = datetime.today().strftime("%Y-%m-%d")

        output_dir = Path(f"{BRONZE_PATH}/{execution_date}")
        output_dir.mkdir(parents=True, exist_ok=True)

        for page in range(1, MAX_PAGES + 1):
            # print(f"Fetching page {page}")
            breweries = fetch_brewery_page(page, per_page)

            if not breweries:
                # print(f"No data on page {page}. Stopping.")
                break

            # Handling overfetching, to reduce bandwidth use
            # elif len(breweries) < per_page:
            #     number_of_breweries_found += len(breweries)
            #     # print(breweries)
            #     # print(len(breweries))
            #     # print(f"Reached end of data at page {page}.")
            #     break

            number_of_breweries_found += len(breweries)
            all_breweries.extend(breweries)

            # key = f"{S3_BASE_PATH}/{execution_date}/page={page}.json"
            # key = f"{S3_BASE_PATH}/{execution_date}/page={page}.json"

            # Idempotency check: skip if file already exists
            # try:
            #     s3.head_object(Bucket=S3_BUCKET, Key=key)
            #     print(f"File already exists: {key} — skipping.")
            #     continue
            # except ClientError as e:
            #     if e.response['Error']['Code'] != '404':
            #         raise

                # Save to JSON file
            output_file = output_dir / "breweries.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_breweries, f, indent=2)

            if len(breweries) < per_page:
                print(f"Reached end of data at page {page}.")
                break

            # upload_to_s3(breweries, key)
            # print(breweries)
            # print('---')
            # print('---')
            # print('---')
        print(number_of_breweries_found)
        return number_of_breweries_found

    # number_of_breweries_found = extract_to_bronze_s3(per_page=200)

    # print(f"Total number of breweries loaded: {number_of_breweries_found}")



    @task()
    def transform_and_save_to_silver(execution_date: str = None):
        import json
        import os
        import pandas as pd
        from datetime import datetime
        from unidecode import unidecode

        if not execution_date:
            execution_date = datetime.today().strftime("%Y-%m-%d")
        # latest_bronze_folder = get_latest_bronze_folder(BRONZE_PATH)
        # latest_bronze_folder = BRONZE_PATH 
        latest_bronze_folder = Path(f"{BRONZE_PATH}/{execution_date}")
        input_file = latest_bronze_folder / "breweries.json"

        with open(input_file, "r", encoding="utf-8") as f:
            breweries = json.load(f)


        df = pd.DataFrame(breweries)
        # y = [print(x) for x in df.state]
        df['state'] = df['state'].apply(unidecode)

        # Basic cleanup: drop nulls in key fields, drop redundant fields
        df = df.drop(columns=["address_2", "address_3", "street"], errors="ignore")
        df = df.dropna(subset=["id", "state"])

        # Optional: rename fields for consistency
        # df = df.rename(columns={"state": "state", "brewery_type": "type"})

        # Add an ingestion date column for tracking
        ingestion_date = latest_bronze_folder.name  # folder name is the date
        df["ingestion_date"] = ingestion_date

        # Write each state as a separate partitioned Parquet file
        for state, group_df in df.groupby("state"):
            output_dir = Path(SILVER_PATH) / f"state={state}"
            output_dir.mkdir(parents=True, exist_ok=True, )

            output_file = output_dir / f"{ingestion_date}.parquet"
            group_df.to_parquet(output_file, index=False)

        print(f"✅ Transformed {len(df)} records from bronze to silver.")

    
    

    def get_ingestion_dates(silver_path: Path):
        # Extract all available ingestion dates from one of the state folders
        sample_state = next(silver_path.iterdir())
        return sorted({f.stem for f in sample_state.glob("*.parquet")})

    def load_silver_data_for_date(ingestion_date: str):
        import pandas as pd
        dfs = []
        for state_dir in Path(SILVER_PATH).glob("state=*"):
            parquet_file = state_dir / f"{ingestion_date}.parquet"
            if parquet_file.exists():
                df = pd.read_parquet(parquet_file)
                dfs.append(df)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    @task
    def generate_gold_summary():
        ingestion_dates = get_ingestion_dates(Path(SILVER_PATH))

        for date in ingestion_dates:
            df = load_silver_data_for_date(date)

            if df.empty:
                print(f"⚠️ No data found for {date}")
                continue

            # Group and aggregate
            summary_df = (
                df.groupby(["state", "brewery_type"])
                .size()
                .reset_index(name="brewery_count")
            )

            # Add date column for partitioning
            summary_df["date"] = date

            # Define output path
            output_path = Path(GOLD_PATH) / f"{date}"
            output_path.mkdir(parents=True, exist_ok=True)
            summary_file = output_path / "summary.parquet"

            # Save to Parquet
            summary_df.to_parquet(summary_file, index=False)
            print(f"✅ Gold summary written for {date} -> {summary_file}")

    # Base input/output paths (relative to your project root)
    # BRONZE_PATH = Path("./data-lake/bronze/open-brewery-api")
    # Set dependencies between tasks
    extract_to_bronze() >> transform_and_save_to_silver() >> generate_gold_summary()