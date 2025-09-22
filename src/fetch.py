"""
==================================================================
GOOGLE FETCHING MODULE
------------------------------------------------------------------
This module is responsible for direct, authenticated access to the 
Google Ads API, encapsulating all logic required to 
fetch raw campaign, ad, creative, and metadata records.

It provides a clean interface to centralize API-related operations, 
enabling reusable, testable, and isolated logic for data ingestion 
pipelines without mixing transformation or storage responsibilities.

✔️ Initializes secure Google Ads Client and retrieves credentials dynamically  
✔️ Fetches data via API calls (with pagination) and returns structured DataFrames  
✔️ Does not handle Google BigQuery upload, schema validation, or enrichment logic

⚠️ This module focuses only on *data retrieval from the API*. 
It does not handle schema validation, data transformation, or 
storage operations such as uploading to BigQuery.
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add logging ultilities for integraton
import logging

# Add JSON ultilities for integration
import json

# Add Python Pandas libraries for integration
import pandas as pd

# Add time ultilities for integration
import time

# Add Google Ads modules for integration
from google.ads.googleads.client import GoogleAdsClient

# Add Google Secret Manager modules for integration
from google.cloud import secretmanager

# Add internal Google Ads modules for handling
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

# 1. FETCH GOOGLE ADS METADATA

# 1.1. Fetch Google Ads campaign metadata
def fetch_campaign_metadata(
    customer_id: str,
    campaign_id_list: list[str],
    fields: list[str] = None
) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Google Ads campaign metadata for customer_id {customer_id}...")
    logging.info(f"🚀 [FETCH] Starting to fetch Google Ads campaign metadata for customer_id {customer_id}...")

    # 2.1.1. Chuẩn bị fields mặc định
    default_fields = [
        "campaign.id",
        "campaign.name",
        "campaign.status",
        "campaign.advertising_channel_type",
        "campaign.advertising_channel_sub_type",
        "campaign.serving_status",
        "campaign.start_date",
        "campaign.end_date"
    ]
    fetch_fields = fields if fields else default_fields
    all_records = []

    try:
    # 2.1.2. Initialize Google Secret Manager client
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            secret_client = secretmanager.SecretManagerServiceClient()
            secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
            response = secret_client.access_secret_version(request={"name": secret_name})
            creds = json.loads(response.payload.data.decode("utf-8"))
            print(f"✅ [FETCH] Successfuly initialized Google Secret Manager client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfuly initialized Google Secret Manager client for Google Cloud Platform project {PROJECT}.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to initialze Google Secret Manager client due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialze Google Secret Manager client due to {e}.")
        
    # 2.1.3. Initialize Google Ads client
        try:
            print(f"🔍 [UPDATE] Initializing Google Ads client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [UPDATE] Initializing Google Ads client for Google Cloud Platform project {PROJECT}...")
            client = GoogleAdsClient.load_from_dict(creds, version="v16")
            print(f"✅ [UPDATE] Successfuly initialized Google Ads client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [UPDATE] Successfuly initialized Google Ads client for Google Cloud Platform project {PROJECT}.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to initialze Google Ads client due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialze Google Ads client due to {e}.")

    # 2.1.4. Loop through all campaign_id(s)
        print(f"🔍 [FETCH] Retrieving metadata for {len(campaign_id_list)} Google Ads campaign_id(s).")
        logging.info(f"🔍 [FETCH] Retrieving metadata for {len(campaign_id_list)} Google Ads campaign_id(s).")
        try:
            query = f"SELECT {', '.join(fields)} FROM campaign WHERE campaign.id IN ({','.join(map(str, campaign_id_list))})"
            ga_service = client.get_service("GoogleAdsService")
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                record = {
                    "campaign_id": row.campaign.id,
                    "campaign_name": row.campaign.name,
                    "status": row.campaign.status.name,
                    "channel_type": row.campaign.advertising_channel_type.name,
                    "channel_sub_type": row.campaign.advertising_channel_sub_type.name,
                    "serving_status": row.campaign.serving_status.name,
                }
                all_records.append(record)
        except Exception as e:
            print(f"❌ [FETCH] Failed to fetch Google Ads campaign metadata due to {e}.")
            logging.error(f"❌ [FETCH] Failed to fetch Google Ads campaign metadata due to {e}.")

    # 2.1.5. Convert to dataframe
        print(f"🔄 [FETCH] Converting metadata for {len(campaign_id_list)} Google Ads campaign_id(s) to dataframe...")
        logging.info(f"🔄 [FETCH] Converting metadata for {len(campaign_id_list)} Google Ads campaign_id(s) to dataframe...")
        if not all_records:
            print("⚠️ [FETCH] No Google Ads campaign metadata fetched.")
            logging.warning("⚠️ [FETCH] No Google Ads campaign metadata fetched.")
            return pd.DataFrame()
        try:
            df = pd.DataFrame(all_records)
            print(f"✅ [FETCH] Successfully converted Google Ads campaign metadata to dataframe with {len(df)} row(s).")
            logging.info(f"✅ [FETCH] Successfully converted Google Ads campaign metadata to dataframe with {len(df)} row(s).")
        except Exception as e:
            print(f"❌ [FETCH] Failed to convert Google Ads campaign metadata to dataframe due to {e}.")
            logging.error(f"❌ [FETCH] Failed to convert Google Ads campaign metadata to dataframe due to {e}.")
            return pd.DataFrame()

    # 2.1.6. Enforce schema
        try:
            print(f"🔄 [FETCH] Enforcing schema for {len(df)} row(s) of Google Ads campaign metadata...")
            logging.info(f"🔄 [FETCH] Enforcing schema for {len(df)} row(s) of Google Ads campaign metadata...")            
            df = ensure_table_schema(df, "fetch_campaign_metadata")            
            print(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of Google Ads campaign metadata.")
            logging.info(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of Google Ads campaign metadata.")        
        except Exception as e:
            print(f"❌ [FETCH] Failed to enforce schema for Google Ads campaign metadata due to {e}.")
            logging.error(f"❌ [FETCH] Failed to enforce schema for Google Ads campaign metadata due to {e}.")
            return pd.DataFrame()
        if not isinstance(df, pd.DataFrame):
            print("❌ [FETCH] Final Google Ads campaign metadata output is not a Python DataFrame.")
            logging.error("❌ [FETCH] Final Google Ads campaign metadata output is not a Python DataFrame.")
            return pd.DataFrame()
        print(f"✅ [FETCH] Successfully returned final Google Ads campaign metadata dataframe with shape {df.shape}.")
        logging.info(f"✅ [FETCH] Successfully returned final Google Ads campaign metadata dataframe with shape {df.shape}.")
        return df
    except Exception as e:
        print(f"❌ [FETCH] Failed to fetch Google Ads campaign metadata due to {e}.")
        logging.error(f"❌ [FETCH] Failed to fetch Google Ads campaign metadata due to {e}.")
        return pd.DataFrame()

# 2. FETCH GOOGLE ADS INSIGHTS

# 2.2. Fetch Google Ads campaign insights (metrics only)
def fetch_campaign_insights(customer_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Google Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Starting to fetch Google Ads campaign insights from {start_date} to {end_date}...")

    try:
        # 1. Load credentials from Secret Manager
        print("🔍 [FETCH] Retrieving Google Ads credentials from Secret Manager...")
        logging.info("🔍 [FETCH] Retrieving Google Ads credentials from Secret Manager...")

        secret_client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{PROJECT}/secrets/{SECRET_ID}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        creds = json.loads(response.payload.data.decode("utf-8"))

        client = GoogleAdsClient.load_from_dict(creds, version="v16")
        print("✅ [FETCH] Successfully initialized Google Ads client.")
        logging.info("✅ [FETCH] Successfully initialized Google Ads client.")

        # 2. Define query (metrics only)
        query = f"""
            SELECT
                campaign.id,
                metrics.impressions,
                metrics.clicks,
                metrics.conversions,
                metrics.cost_micros,
                segments.date
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """
        print(f"🔍 [FETCH] Preparing Google Ads API query: {query}")
        logging.info(f"🔍 [FETCH] Preparing Google Ads API query.")

        ga_service = client.get_service("GoogleAdsService")

        # 3. Make request
        records = []
        for attempt in range(2):
            try:
                print(f"🔍 [FETCH] Fetching campaign insights for customer_id {customer_id}, attempt {attempt+1}...")
                logging.info(f"🔍 [FETCH] Fetching campaign insights for customer_id {customer_id}, attempt {attempt+1}...")

                response = ga_service.search(customer_id=customer_id, query=query)

                for row in response:
                    records.append({
                        "campaign_id": row.campaign.id,
                        "date": row.segments.date,
                        "impressions": row.metrics.impressions,
                        "clicks": row.metrics.clicks,
                        "conversions": row.metrics.conversions,
                        "cost_micros": row.metrics.cost_micros,
                    })

                if not records:
                    print("⚠️ [FETCH] No data returned from Google Ads API.")
                    logging.warning("⚠️ [FETCH] No data returned from Google Ads API.")
                    return pd.DataFrame()

                df = pd.DataFrame(records)
                print(f"✅ [FETCH] Successfully retrieved {len(df)} row(s) from Google Ads.")
                logging.info(f"✅ [FETCH] Successfully retrieved {len(df)} row(s) from Google Ads.")
                return df

            except Exception as e_inner:
                print(f"⚠️ [FETCH] Google Ads API error: {e_inner}")
                logging.error(f"⚠️ [FETCH] Google Ads API error: {e_inner}")
                if attempt == 1:
                    return pd.DataFrame()
                time.sleep(1)

    except Exception as e_outer:
        print(f"❌ [FETCH] Failed to fetch Google Ads campaign insights: {e_outer}")
        logging.error(f"❌ [FETCH] Failed to fetch Google Ads campaign insights: {e_outer}")
        return pd.DataFrame()