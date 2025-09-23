"""
==================================================================
GOOGLE INGESTION MODULE
------------------------------------------------------------------
This module ingests raw data from the Google Ads API into Google 
BigQuery, forming the raw data layer of the Ads Data Pipeline.

It orchestrates the full ingestion flow: from authenticating the SDK, 
to fetching data, enriching it, validating schema, and loading into 
BigQuery tables organized by campaign, ad, creative and metadata.

✔️ Supports append or truncate via configurable `write_disposition`  
✔️ Applies schema validation through centralized schema utilities  
✔️ Includes logging and CSV-based error tracking for traceability

⚠️ This module is strictly limited to *raw-layer ingestion*.  
It does **not** handle data transformation, modeling, or aggregation.
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add datetime utilities for integration
from datetime import datetime

# Add timezone ultilities for integration
import pytz

# Add JSON ultilities for integration
import json 

# Add logging ultilities forintegration
import logging

# Add UUID ultilities for integration
import uuid

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google Authentication modules for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal Facebook modules for handling
from src.enrich import (
    enrich_campaign_insights, 
    enrich_ad_insights   
)
from src.fetch import (
    fetch_account_name,
    fetch_campaign_metadata, 
    fetch_adset_metadata,
    fetch_ad_metadata,
    fetch_ad_creative,
    fetch_campaign_insights, 
    fetch_ad_insights,
)
from config.schema import ensure_table_schema

# Get environment variable for Company
COMPANY = os.getenv("COMPANY") 

# Get environment variable for Google Cloud Project ID
PROJECT = os.getenv("PROJECT")

# Get environment variable for Platform
PLATFORM = os.getenv("PLATFORM")

# Get environmetn variable for Department
DEPARTMENT = os.getenv("DEPARTMENT")

# Get environment variable for Account
ACCOUNT = os.getenv("ACCOUNT")

# Get nvironment variable for Layer
LAYER = os.getenv("LAYER")

# Get environment variable for Mode
MODE = os.getenv("MODE")

# 1. INGEST GOOGLE ADS METADATA FROM GOOGLE ADS API TO GOOGLE BIGQUERY

# 1.1. Ingest campaign metadata for Google Ads
def ingest_campaign_metadata(campaign_id_list: list) -> pd.DataFrame:
    print("🚀 [INGEST] Starting to ingest Google Ads campaign metadata...")
    logging.info("🚀 [INGEST] Starting Google Ads campaign metadata...")

    # 1.1.1. Validate input
    if not campaign_id_list:
        print("⚠️ [INGEST] Empty Google Ads campaign_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty Google Ads campaign_id_list provided then ingestion is suspended.")
        raise ValueError("❌ [INGEST] Google Ads campaign_id_list must be provided and not empty.")

    # 1.1.2. Call Google Ads API
    try:
        print(f"🔍 [INGEST] Triggering to fetch Google Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) from API...")
        logging.info(f"🔍 [INGEST] Triggering to fetch Google Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) from API...")
        df = fetch_campaign_metadata(campaign_id_list=campaign_id_list)
        if df.empty:
            print("⚠️ [INGEST] Empty Google Ads campaign metadata returned.")
            logging.warning("⚠️ [INGEST] Empty Google Ads campaign metadata returned.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger Google Ads campaign metadata fetch due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger Google Ads campaign metadata fetch due to {e}.")
        return pd.DataFrame()

    # 1.1.3. Prepare table_id
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest Google Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Google Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) with table_id {table_id}...")

    # 1.1.4. Enforce schema
    try:
        print(f"🔄 [INGEST] Triggering to enforce schema for {len(df)} row(s) of Google Ads campaign metadata...")
        logging.info(f"🔄 [INGEST] Triggering to enforce schema for {len(df)} row(s) of Google Ads campaign metadata...")
        df = ensure_table_schema(df, "ingest_campaign_metadata")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for Google Ads campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for Google Ads campaign metadata due to {e}.")
        return df

    # 1.1.5. Delete existing row(s) or create new table if it not exist
    try:
        print(f"🔍 [INGEST] Checking Google Ads campaign metadata table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking Google Ads campaign metadata table {table_id} existence...")
        df = df.drop_duplicates()

        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError(" ❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e

        try:
            client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            print(f"⚠️ [INGEST] Google Ads campaign metadata table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] Google Ads campaign metadata table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in df.dtypes.items():
                if dtype.name.startswith("int"):
                    bq_type = "INT64"
                elif dtype.name.startswith("float"):
                    bq_type = "FLOAT64"
                elif dtype.name == "bool":
                    bq_type = "BOOL"
                elif "datetime" in dtype.name:
                    bq_type = "TIMESTAMP"
                else:
                    bq_type = "STRING"
                schema.append(bigquery.SchemaField(col, bq_type))
            table = bigquery.Table(table_id, schema=schema)

            effective_partition = "date" if "date" in df.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
            clustering_fields = ["campaign_id"]
            filtered_clusters = [f for f in clustering_fields if f in df.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating Google Ads campaign metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating Google Ads campaign metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created Google Ads campaign metadata table {table_id}.")
            logging.info(f"✅ [INGEST] Successfully created Google Ads campaign metadata table {table_id}.")
        else:
            print(f"🔄 [INGEST] Google Ads campaign metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] Google Ads campaign metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = df[["campaign_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_campaign_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                join_condition = " AND ".join([
                    f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                    for col in ["campaign_id"]
                ])
                delete_query = f"""
                    DELETE FROM `{table_id}` AS main
                    WHERE EXISTS (
                        SELECT 1 FROM `{temp_table_id}` AS temp
                        WHERE {join_condition}
                    )
                """
                result = client.query(delete_query).result()
                client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Google Ads campaign metadata table {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Google Ads campaign metadata table {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique campaign_id found in Google Ads campaign metadata table {table_id} then deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique campaign_id found in Google Ads campaign metadata table {table_id} then deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during Google Ads campaign metadata ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during Google Ads campaign metadata ingestion due to {e}.")
        raise

    # 1.1.6. Upload Google Ads campaign metadata to Google BigQuery raw table
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of Google Ads campaign metadata {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of Google Ads campaign metadata {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Google Ads campaign metadata table {table_id}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Google Ads campaign metadata table {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload Google Ads campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload Google Ads campaign metadata due to {e}.")
        raise

    return df

# 2. INGEST GOOGLE ADS INSIGHTS FROM GOOGLE ADS API TO GOOGLE BIGQUERY

# 2.1. Ingest campaign insights for Google Ads
def ingest_google_campaign_insights(
    start_date: str,
    end_date: str,
    write_disposition: str = "WRITE_APPEND"
) -> pd.DataFrame:
    
    print(f"🚀 [INGEST] Starting to ingest Google Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest Google Ads campaign insights from {start_date} to {end_date}...")

    # 2.1.1. Call Google Ads API to fetch campaign insights
    print("🔍 [INGEST] Triggering to fetch Google Ads campaigns insights from API...")
    logging.info("🔍 [INGEST] Triggering to fetch Google Ads campaigns insights from API...")
    df = fetch_campaign_insights(start_date, end_date)    
    if df.empty:
        print("⚠️ [INGEST] Empty Google Ads campaign insights returned.")
        logging.warning("⚠️ [INGEST] Empty Google Ads campaign insights returned.")    
        return df

    # 2.1.2. Prepare table_id
    first_date = pd.to_datetime(df["date"].dropna().iloc[0])
    y, m = first_date.year, first_date.month
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"
    print(f"🔍 [INGEST] Proceeding to ingest Google Ads campaign insights from {start_date} to {end_date} with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Google Ads campaign insights from {start_date} to {end_date} with table_id {table_id}...")

    # 2.1.3. Enrich Google Ads campaign insights
    try:
        print(f"🔁 [INGEST] Trigger to enrich Google Ads campaign insights from {start_date} to {end_date} with {len(df)} row(s)...")
        logging.info(f"🔁 [INGEST] Trigger to enrich Google Ads campaign insights from {start_date} to {end_date} with {len(df)} row(s)...")
        df = enrich_campaign_insights(df)
        df["account_name"] = fetch_account_name()
        df["date_range"] = f"{start_date}_to_{end_date}"
        df["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger enrichment Google Ads campaign insights from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger enrichment Google Ads campaign insights from {start_date} to {end_date} due to {e}.")
        raise

    # 2.1.4. Cast Google Ads numeric fields to float
    try:
        numeric_fields = [
            "impressions",
            "clicks",
            "spend",        
            "conversions",
            "all_conversions",
        ]
        print(f"🔁 [INGEST] Casting Google Ads campaign insights {numeric_fields} numeric field(s)...")
        logging.info(f"🔁 [INGEST] Casting Google Ads campaign insights {numeric_fields} numeric field(s)...")
        for col in numeric_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"✅ [INGEST] Successfully casted Google Ads campaign insights {numeric_fields} numeric field(s) to float.")
        logging.info(f"✅ [INGEST] Successfully casted Google Ads campaign insights {numeric_fields} numeric field(s) to float.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to cast Google Ads numeric field(s) to float due to {e}.")
        logging.error(f"❌ [INGEST] Failed to cast Google Ads numeric field(s) to float due to {e}.")
        raise

    # 2.1.5. Enforce schema for Google Ads campaign insights
    try:
        print(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of Google Ads campaign insights...")
        logging.info(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of Google Ads campaign insights...")
        df = ensure_table_schema(df, schema_type="ingest_google_campaign_insights")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for Google Ads campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for Google Ads campaign insights due to {e}.")
        raise

    # 2.1.6. Parse date column(s) for Google Ads campaign insights
    try:
        print(f"🔁 [INGEST] Parsing Google Ads campaign insights {df.columns.tolist()} date column(s)...")
        logging.info(f"🔁 [INGEST] Parsing Google Ads campaign insights {df.columns.tolist()} date column(s)...")
        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
        df["date_start"] = df["date"].dt.strftime("%Y-%m-%d")
        print(f"✅ [INGEST] Successfully parsed {df.columns.tolist()} date column(s) for Google Ads campaign insights.")
        logging.info(f"✅ [INGEST] Successfully parsed {df.columns.tolist()} date column(s) for Google Ads campaign insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to parse date column(s) for Google Ads campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to parse date column(s) for Google Ads campaign insights due to {e}.")
        raise

    # 2.1.7. Delete existing row(s) or create new table if not exist
    try:
        print(f"🔍 [INGEST] Checking Google Ads campaign insights table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking Google Ads campaign insights table {table_id} existence...")
        df = df.drop_duplicates()
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        try:
            client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] Google Ads campaign insights table {table_id} not found then table creation will be proceeding...")
            logging.warning(f"⚠️ [INGEST] Google Ads campaign insights table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in df.dtypes.items():
                if dtype.name.startswith("int"):
                    bq_type = "INT64"
                elif dtype.name.startswith("float"):
                    bq_type = "FLOAT64"
                elif dtype.name == "bool":
                    bq_type = "BOOL"
                elif "datetime" in dtype.name:
                    bq_type = "TIMESTAMP"
                else:
                    bq_type = "STRING"
                schema.append(bigquery.SchemaField(col, bq_type))
            table = bigquery.Table(table_id, schema=schema)
            effective_partition = "date" if "date" in df.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
                print(f"🔍 [INGEST] Creating Google Ads campaign insights {table_id} using partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating Google Ads campaign insights {table_id} using partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created Google Ads campaign insights table {table_id} with partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created Google Ads campaign insights table {table_id} with partition on {effective_partition}.")
        else:
            new_dates = df["date_start"].dropna().unique().tolist()
            query_existing = f"SELECT DISTINCT date_start FROM `{table_id}`"
            existing_dates = [row.date_start for row in client.query(query_existing).result()]
            overlap = set(new_dates) & set(existing_dates)
            if overlap:
                print(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in Google Ads campaign insights {table_id} table then deletion will be proceeding...")
                logging.warning(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in Google Ads campaign insights {table_id} table then deletion will be proceeding...")
                for date_val in overlap:
                    query = f"""
                        DELETE FROM `{table_id}`
                        WHERE date_start = @date_value
                    """
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", date_val)]
                    )
                    try:
                        result = client.query(query, job_config=job_config).result()
                        deleted_rows = result.num_dml_affected_rows
                        print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in Google Ads campaign insights {table_id} table.")
                        logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in Google Ads campaign insights {table_id} table.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to delete existing rows in Google Ads campaign insights {table_id} table for {date_val} due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to delete existing rows in Google Ads campaign insights {table_id} table for {date_val} due to {e}.")
            else:
                print(f"✅ [INGEST] No overlapping dates found in Google Ads campaign insights {table_id} table then deletion is skipped.")
                logging.info(f"✅ [INGEST] No overlapping dates found in Google Ads campaign insights {table_id} table then deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during Google Ads campaign insights ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during Google Ads campaign insights ingestion due to {e}.")
        raise

    # 2.1.8. Upload to BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of Google Ads campaign insights to table {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of Google Ads campaign insights to table {table_id}...")
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
        )
        load_job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )
        load_job.result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Google Ads campaign insights.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Google Ads campaign insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload Google Ads campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload Google Ads campaign insights due to {e}.")
        raise

    return df