import pytest
import os
import json
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, '"/opt/airflow/dags/scripts')

from extract_from_api import fetch_brewery_page, extract_breweries_to_bronze
from transform_to_silver import transform_bronze_to_silver
from aggregate_to_gold import generate_gold_summary

# ---------- FIXTURES ----------

@pytest.fixture
def fake_brewery_data():
    return [
        {
            "id": "1",
            "name": "Brew Co",
            "brewery_type": "micro",
            "state": "New York",
            "country": "United States",
            "street": "123 Main St",
            "city": "New York",
            "postal_code": "10001"
        },
        {
            "id": "2",
            "name": "Ale House",
            "brewery_type": "brewpub",
            "state": "California",
            "country": "United States",
            "street": "456 Oak St",
            "city": "Los Angeles",
            "postal_code": "90001"
        }
    ]

@pytest.fixture
def test_paths(tmp_path, fake_brewery_data):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"
    gold_path = tmp_path / "gold"
    execution_date = "2025-01-01"

    # Write raw data (bronze)
    bronze_dir = bronze_path / execution_date
    bronze_dir.mkdir(parents=True)
    with open(bronze_dir / "breweries.json", "w") as f:
        json.dump(fake_brewery_data, f)

    return {
        "bronze": bronze_path,
        "silver": silver_path,
        "gold": gold_path,
        "date": execution_date
    }

# ---------- TEST CASES ----------

def test_fetch_single_page():
    result = fetch_brewery_page(page=1, per_page=5)
    assert isinstance(result, list)
    assert len(result) <= 5
    assert all("name" in b for b in result)

def test_extract_breweries_creates_file(tmp_path):
    bronze_path = tmp_path / "bronze"
    record_count = extract_breweries_to_bronze(str(bronze_path), execution_date="2025-01-02")
    assert (bronze_path / "2025-01-02" / "breweries.json").exists()
    assert record_count > 0

def test_transform_creates_partitioned_parquet(test_paths):
    transform_bronze_to_silver(
        bronze_path=str(test_paths["bronze"]),
        silver_path=str(test_paths["silver"]),
        execution_date=test_paths["date"]
    )

    # Verify California parquet exists
    california_file = test_paths["silver"] / "state=California" / f"{test_paths['date']}.parquet"
    assert california_file.exists()

def test_transformed_data_has_expected_columns(test_paths):
    transform_bronze_to_silver(
        str(test_paths["bronze"]),
        str(test_paths["silver"]),
        test_paths["date"]
    )
    file = test_paths["silver"] / "state=New York" / f"{test_paths['date']}.parquet"
    df = pd.read_parquet(file)
    expected_columns = {"id", "name", "brewery_type", "state", "country", "city", "postal_code", "ingestion_date"}
    assert expected_columns.issubset(df.columns)

def test_transformed_data_is_cleaned(test_paths):
    # Modify bronze to include null `id`
    bad_record = {
        "id": None,
        "name": "Bad Brew",
        "brewery_type": "nano",
        "state": "Nevada"
    }
    bronze_file = test_paths["bronze"] / test_paths["date"] / "breweries.json"
    with open(bronze_file, "r+") as f:
        data = json.load(f)
        data.append(bad_record)
        f.seek(0)
        json.dump(data, f)
        f.truncate()

    count = transform_bronze_to_silver(
        str(test_paths["bronze"]),
        str(test_paths["silver"]),
        test_paths["date"]
    )

    # Should ignore the bad record with null ID
    assert count == 2

def test_generate_gold_summary_creates_output(test_paths):
    transform_bronze_to_silver(
        str(test_paths["bronze"]),
        str(test_paths["silver"]),
        test_paths["date"]
    )
    generate_gold_summary(
        silver_path=str(test_paths["silver"]),
        gold_path=str(test_paths["gold"])
    )

    gold_file = test_paths["gold"] / test_paths["date"] / "summary.parquet"
    assert gold_file.exists()

def test_gold_summary_contains_aggregated_data(test_paths):
    transform_bronze_to_silver(
        str(test_paths["bronze"]),
        str(test_paths["silver"]),
        test_paths["date"]
    )
    generate_gold_summary(
        silver_path=str(test_paths["silver"]),
        gold_path=str(test_paths["gold"])
    )
    summary = pd.read_parquet(test_paths["gold"] / test_paths["date"] / "summary.parquet")
    assert "brewery_count" in summary.columns
    assert summary["brewery_count"].sum() == 2
