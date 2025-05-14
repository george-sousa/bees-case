from airflow.sdk import DAG, task
from datetime import datetime, timedelta
from scripts.extract_from_api import extract_breweries_to_bronze
from scripts.transform_to_silver import transform_bronze_to_silver
from scripts.aggregate_to_gold import generate_gold_summary

BRONZE_PATH = "/opt/airflow/data-lake/bronze/open_brewery_api"
SILVER_PATH = "/opt/airflow/data-lake/silver/breweries"
GOLD_PATH = "/opt/airflow/data-lake/gold/breweries_summary"

default_args = {
    'owner': 'Data Engineering Team',
    'email_on_failure': True,
    'retries': 3, # Transient network failures are common; retrying often succeeds
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="breweries_pipeline_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 0 * * *",
    catchup=False
) as breweries_pipeline_dag:

    @task()
    def extract():
        return extract_breweries_to_bronze(BRONZE_PATH)

    @task()
    def transform():
        return transform_bronze_to_silver(BRONZE_PATH, SILVER_PATH)

    @task()
    def aggregate():
        return generate_gold_summary(SILVER_PATH, GOLD_PATH)

    #extract() #>> transform() >> aggregate()
    # transform()
    aggregate()
