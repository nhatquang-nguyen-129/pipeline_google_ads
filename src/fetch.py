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
def fetch_campaign_metadata(campaign_id_list: list[str]) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Google Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch Google Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")

    
    # 2.1.1. Validate input
    if not campaign_id_list:
        print("⚠️ [FETCH] Empty Google Ads campaign_id_list provided.")
        logging.warning("⚠️ [FETCH] Empty Google Ads campaign_id_list provided.")
        return pd.DataFrame()
    
    # 2.1.2. Prepare field(s)
    fetch_fields = [
        "campaign.id",
        "campaign.name",
        "campaign.status",
        "campaign.advertising_channel_type",
        "campaign.advertising_channel_sub_type",
        "campaign.serving_status",
        "campaign.start_date",
        "campaign.end_date"
    ]
    all_records = []
    print(f"🔄 [FETCH] Preparing to fetch Google Ads campaign metadata with {fetch_fields} field(s)...")
    logging.info(f"🔄 [FETCH] Preparing to fetch Google Ads campaign metadata with {fetch_fields} field(s)...")

    try:
    
    # 2.1.3. Initialize Google Secret Manager client
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for project {PROJECT}...")
            secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Initialized Google Secret Manager client for project {PROJECT}.")
            logging.info(f"✅ [FETCH] Initialized Google Secret Manager client for project {PROJECT}.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to initialize Secret Manager client: {e}")
            logging.error(f"❌ [FETCH] Failed to initialize Secret Manager client: {e}")
            return pd.DataFrame()

    # 2.1.3. Initialize Google Ads client
        try:
            print(f"🔍 [FETCH] Retrieving Google Ads ad account information for {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Google Ads ad account information for {ACCOUNT} from Google Secret Manager...") 
            google_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            google_secret_name = f"projects/{PROJECT}/secrets/{google_secret_id}/versions/latest"
            print(f"✅ [FETCH] Successfully retrieved Google Ads account secret_id {google_secret_id} for account environment variable {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Google Ads account secret_id {google_secret_id} for account environment variable {ACCOUNT} from Google Secret Manager.")   
            response = secret_client.access_secret_version(request={"name": google_secret_name})
            creds = json.loads(response.payload.data.decode("utf-8"))
            customer_id = creds["customer_id"]
            print(f"🔍 [FETCH] Initializing Google Ads client for customer_id {customer_id} from Google Secret Manager account secret_id {google_secret_id}...")
            logging.info(f"🔍 [FETCH] Initializing Google Ads client for customer_id {customer_id} from Google Secret Manager account secret_id {google_secret_id}...")
            google_ads_client = GoogleAdsClient.load_from_dict(creds, version="v16")
            print(f"✅ [FETCH] Successfully initialized Google Ads client for customer_id {customer_id}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Ads client for customer_id {customer_id}.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to initialize Google Ads client due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Ads client due to {e}.")
            return pd.DataFrame()

    # 2.1.4. Loop through campaign_id(s)
        try:
            print(f"🔍 [FETCH] Retrieving campaign metadata for {len(campaign_id_list)} Google Ads campaign_id(s)...")
            logging.info(f"🔍 [FETCH] Retrieving campaign metadata for {len(campaign_id_list)} Google Ads campaign_id(s)...")
            ids_str = ",".join(map(str, campaign_id_list))
            query = f"SELECT {', '.join(fetch_fields)} FROM campaign WHERE campaign.id IN ({ids_str})"
            google_ads_service = google_ads_client.get_service("GoogleAdsService")
            response = google_ads_service.search(customer_id=customer_id, query=query)
            for row in response:
                record = {
                    "campaign_id": row.campaign.id,
                    "campaign_name": row.campaign.name,
                    "status": row.campaign.status.name,
                    "channel_type": row.campaign.advertising_channel_type.name,
                    "channel_sub_type": row.campaign.advertising_channel_sub_type.name,
                    "serving_status": row.campaign.serving_status.name,
                    "start_date": row.campaign.start_date,
                    "end_date": row.campaign.end_date,
                    "account_id": customer_id,
                }
                all_records.append(record)
        except Exception as e:
            print(f"❌ [FETCH] Failed to campaign metadata for Google Ads due to {e}.")
            logging.error(f"❌ [FETCH] Failed to campaign metadata for Google Ads due to {e}.")
            return pd.DataFrame()

    # 2.1.5. Convert to Python DataFrame
        if not all_records:
            print("⚠️ [FETCH] No Google Ads campaign metadata fetched.")
            logging.warning("⚠️ [FETCH] No Google Ads campaign metadata fetched.")
            return pd.DataFrame()
        try:
            print(f"🔄 [FETCH] Converting metadata for {len(campaign_id_list)} campaign_id(s) to DataFrame...")
            logging.info(f"🔄 [FETCH] Converting metadata for {len(campaign_id_list)} campaign_id(s) to DataFrame...")
            df = pd.DataFrame(all_records)
            print(f"✅ [FETCH] Successfully converted metadata to DataFrame with {len(df)} row(s).")
            logging.info(f"✅ [FETCH] Successfully converted metadata to DataFrame with {len(df)} row(s).")
        except Exception as e:
            print(f"❌ [FETCH] Failed to convert metadata to DataFrame: {e}")
            logging.error(f"❌ [FETCH] Failed to convert metadata to DataFrame: {e}")
            return pd.DataFrame()
        return df
    except Exception as e:
        print(f"❌ [FETCH] Unexpected error: {e}")
        logging.error(f"❌ [FETCH] Unexpected error: {e}")
        return pd.DataFrame()

# 2. FETCH GOOGLE ADS INSIGHTS

# 2.1. Fetch campaign insights for Google Ads
def fetch_campaign_insights(customer_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Google Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Starting to fetch Google Ads campaign insights from {start_date} to {end_date}...")

    try:
    # 2.1.1. Initialize Google Secret Manager client
        try: 
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfuly initialized Google Secret Manager client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfuly initialized Google Secret Manager client for Google Cloud Platform project {PROJECT}.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to initialze Google Secret Manager client due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialze Google Secret Manager client due to {e}.")
    
    # 2.1.2. Initialize Google Ads client
        try:
            print(f"🔍 [UPDATE] Initializing Google Ads client for Google Ads account {PROJECT}...")
            logging.info(f"🔍 [UPDATE] Initializing Google Ads client for Google Ads account {PROJECT}...")
            google_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            google_secret_name = f"projects/{PROJECT}/secrets/{google_secret_id}/versions/latest"
            response = google_secret_client.access_secret_version(request={"name": google_secret_name})
            creds = json.loads(response.payload.data.decode("utf-8"))
            google_ads_client = GoogleAdsClient.load_from_dict(creds, version="v16")
            print(f"✅ [UPDATE] Successfuly initialized Google Ads client for Google Ads account_id {google_secret_id}.")
            logging.info(f"✅ [UPDATE] Successfuly initialized Google Ads client for Google Ads account_id {google_secret_id}.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to initialze Google Ads client due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialze Google Ads client due to {e}.")

    # 2.1.3. Define query (metrics only)
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

        google_ads_service = google_ads_client.get_service("GoogleAdsService")

        # 3. Make request
        records = []
        for attempt in range(2):
            try:
                print(f"🔍 [FETCH] Fetching campaign insights for customer_id {customer_id}, attempt {attempt+1}...")
                logging.info(f"🔍 [FETCH] Fetching campaign insights for customer_id {customer_id}, attempt {attempt+1}...")

                response = google_ads_service.search(customer_id=customer_id, query=query)

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