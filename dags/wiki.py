from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
import pandas as pd
import requests
import os

# === CONFIG ===
BASE_URL_TEMPLATE = "https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month:02d}"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def download_and_ingest():
    # Step 1: Dynamic date (yesterday)
    now = datetime.now(timezone.utc) - timedelta(days=1)
    year, month, day, hour = now.year, now.month, now.day, now.hour

    base_url = BASE_URL_TEMPLATE.format(year=year, month=month)
    file_name = f"pageviews-{year}{month:02d}{day:02d}-{hour:02d}0000.gz"
    download_url = f"{base_url}/{file_name}"
    local_path = f"/opt/airflow/data/{file_name}"

    print(f"Downloading: {download_url}")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    resp = requests.get(download_url, stream=True)
    if resp.status_code != 200:
        raise Exception(f"Failed to download file: {resp.status_code}")

    with open(local_path, "wb") as f:
        f.write(resp.content)
    print(f"Downloaded: {local_path}")

    # Step 2: Parse gzip
    df = pd.read_csv(
        local_path,
        compression="gzip",
        sep=r"\s+",
        header=None,
        names=["domain_code", "page_title", "view_count", "response_size"],
    )
    print(f"Parsed {len(df)} rows")

    # Step 3: Add metadata
    df["project"] = "en.wikipedia.org"
    df["dt_hour"] = datetime(year, month, day, hour, tzinfo=timezone.utc)
    df["fetched_at"] = datetime.now(timezone.utc)

    # Step 4: Get Supabase connection from Airflow
    conn = BaseHook.get_connection("supabase_postgres")
    engine = create_engine(conn.get_uri())

    # Step 5: Create table if not exists
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS wiki_pageviews (
                id SERIAL PRIMARY KEY,
                domain_code TEXT,
                page_title TEXT,
                view_count INTEGER,
                response_size INTEGER,
                project TEXT,
                dt_hour TIMESTAMP,
                fetched_at TIMESTAMP
            )
        """))

    # Step 6: Insert data
    try:
        df.to_sql("wiki_pageviews", engine, if_exists="append", index=False)
        print("✅ Data inserted successfully into Supabase")
    except Exception as e:
        print(f"❌ Insert failed: {e}")
        return

    # Step 7: Verify count
    with engine.connect() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM wiki_pageviews"))
        print(f"📊 Total rows in table: {result.scalar()}")

    # Step 8: Cleanup
    os.remove(local_path)
    print("🧹 Cleanup complete")


# === DAG Definition ===
with DAG(
    dag_id="wiki_pageviews_dump_to_supabase",
    default_args=default_args,
    description="Daily Wikipedia pageviews dump to Supabase",
    start_date=datetime(2025, 10, 21),
    schedule_interval="0 0 * * *",  # Daily at midnight UTC
    catchup=False,
    tags=["supabase", "wikipedia", "pageviews"],
) as dag:

    run_task = PythonOperator(
        task_id="download_ingest_task",
        python_callable=download_and_ingest,
    )
