"""
One combined Airflow DAG file.py

Assignment 3: Big Data Engineering | Dhrishita Parve (26304511)

Loads raw CSV files from the Cloud Composer bucket into the Postgres Bronze
schema on Cloud SQL. One DAG serves both Part 1 and Part 3:

  * Part 1 - Trigger with month_file = 05_2020.csv: loads the four reference
             files (census G01/G02, LGA code, LGA suburb) and the first month
             of listings.
             
  * Part 3 - Trigger once per month, in chronological order, choosing the
             next month_file. Reference tasks see their tables are already
             loaded and skip; only listings load.

month_file is a DAG param, shown as a drop-down on the "Trigger DAG" form.

Design notes
  * schedule_interval=None: loads are manual so the dbt job can run between
    months (required for the SCD2 snapshots to capture monthly history).
    
  * Files are read as strings: Bronze stores raw TEXT; all typing and
    cleaning happens in dbt.
    
  * Every listing row is tagged with source_file. dbt derives the reporting
    month from it because some scrape dates fall outside their file's month.
    
  * Re-running a month is safe: rows from the same file are deleted and
    reloaded inside one transaction.
    
  * Data is bulk-loaded with Postgres COPY, far faster than INSERTs.
  
  * Fail fast, then retry: every connection has a 30 s connect timeout, TCP
    keepalives and statement/lock timeouts, and each task has a 10-minute
    execution timeout with 2 automatic retries. A slow or dropped connection
    to Cloud SQL therefore fails within a minute and retries, instead of
    hanging for half an hour (see the Issues section of the report).
    
  * The reference checks run one at a time (max_active_tasks=1) so the small
    Composer environment never opens several slow connections at once.
"""

from datetime import datetime, timedelta
import io
import os
import time

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

# Composer mounts gs://<composer-bucket>/data/ at this path on every worker
DATA_DIR = "/home/airflow/gcs/data"
CONN_ID = "postgres_conn"          # Airflow connection to Cloud SQL Postgres

# Months in the order they must be loaded
MONTH_FILES = [
    "05_2020.csv", "06_2020.csv", "07_2020.csv", "08_2020.csv",
    "09_2020.csv", "10_2020.csv", "11_2020.csv", "12_2020.csv",
    "01_2021.csv", "02_2021.csv", "03_2021.csv", "04_2021.csv",
]


def _connect():

    """Opening a psycopg2 connection from the Airflow connection, with timeouts.

    connect_timeout   - give up after 30 s if Cloud SQL cannot be reached
    keepalives_*      - detect a dropped network connection within ~1 minute
    statement_timeout - no single statement may run longer than 5 minutes
    lock_timeout      - fail after 60 s instead of waiting forever on a lock
    """
    c = BaseHook.get_connection(CONN_ID)
    start = time.time()
    conn = psycopg2.connect(
        host=c.host,
        port=c.port or 5432,
        dbname=c.schema or "postgres",
        user=c.login,
        password=c.password,
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
        options="-c statement_timeout=300000 -c lock_timeout=60000",
        application_name="airflow_airbnb_bronze_loader",
    )
    print(f"Connected to {c.host} in {time.time() - start:.1f}s")
    return conn


def _read_csv(file_name, usecols=None):

    """Reading a CSV from the bucket's data/ folder, keeping every value as text."""
    
    path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(path):
        raise AirflowFailException(f"{file_name} not found in the bucket's data/ folder")
    return pd.read_csv(path, dtype=str, usecols=usecols)


def _copy_dataframe(df, table, conn):

    """Bulk-loading a DataFrame into Postgres with COPY ... FROM STDIN.
    Empty cells are written as empty CSV fields, which COPY stores as NULL."""
    
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    columns = ", ".join(c.lower() for c in df.columns)
    with conn.cursor() as cur:
        cur.copy_expert(f"COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv)", buffer)


def load_reference(file_name, table, usecols=None):

    """Loading a reference file once; skip if its Bronze table already has rows."""
    
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})")
            if cur.fetchone()[0]:
                print(f"{table} already loaded - skipping")
                return
        df = _read_csv(file_name, usecols)
        _copy_dataframe(df, table, conn)
        conn.commit()
        print(f"Loaded {len(df)} rows from {file_name} into {table}")
    finally:
        conn.close()


def load_listings(**context):

    """Loading one month of listings, chosen with the month_file param."""
    
    month_file = context["params"]["month_file"]
    df = _read_csv(month_file)
    df["source_file"] = month_file

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bronze.raw_listings WHERE source_file = %s", (month_file,))
            if cur.rowcount:
                print(f"Removed {cur.rowcount} rows from an earlier load of {month_file}")
        _copy_dataframe(df, "bronze.raw_listings", conn)
        conn.commit()                       # delete + reload succeed or fail together
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    print(f"Loaded {len(df)} rows from {month_file} into bronze.raw_listings")


default_args = {
    "retries": 2,                                # retry a dropped connection automatically
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),  # never hang longer than 10 minutes
}

with DAG(
    dag_id="airbnb_bronze_loader",
    description="Load Airbnb listings and Census reference data into Bronze",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,                 # manual trigger only
    catchup=False,
    max_active_runs=1,                      # one month at a time
    max_active_tasks=1,                     # one database connection at a time
    default_args=default_args,
    params={
        "month_file": Param(
            "05_2020.csv",
            type="string",
            enum=MONTH_FILES,
            description="Monthly listings file to load (load in this order)",
        )
    },
    tags=["bde", "assignment3", "bronze"],
) as dag:

    load_lga_code = PythonOperator(
        task_id="load_lga_code",
        python_callable=load_reference,
        op_kwargs={"file_name": "NSW_LGA_CODE.csv", "table": "bronze.raw_lga_code"},
    )

    load_lga_suburb = PythonOperator(
        task_id="load_lga_suburb",
        python_callable=load_reference,
        op_kwargs={
            "file_name": "NSW_LGA_SUBURB.csv",
            "table": "bronze.raw_lga_suburb",
            "usecols": [0, 1],              # drop the 25 empty trailing columns
        },
    )

    load_census_g01 = PythonOperator(
        task_id="load_census_g01",
        python_callable=load_reference,
        op_kwargs={"file_name": "2016Census_G01_NSW_LGA.csv", "table": "bronze.raw_census_g01"},
    )

    load_census_g02 = PythonOperator(
        task_id="load_census_g02",
        python_callable=load_reference,
        op_kwargs={"file_name": "2016Census_G02_NSW_LGA.csv", "table": "bronze.raw_census_g02"},
    )

    load_listings_task = PythonOperator(
        task_id="load_listings",
        python_callable=load_listings,
    )

    # Reference data must exist before listings are loaded
    
    [load_lga_code, load_lga_suburb, load_census_g01, load_census_g02] >> load_listings_task
