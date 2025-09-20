from google.ads.googleads.client import GoogleAdsClient
from google.cloud import secretmanager
import pandas as pd
import logging
import time
import json

from google.ads.googleads.client import GoogleAdsClient
from google.cloud import secretmanager
import pandas as pd
import logging
import time
import json

PROJECT = "your-gcp-project"
SECRET_ID = "google_ads_credentials"

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

        # 2. Define query
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign.status,
                metrics.impressions,
                metrics.clicks,
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
                        "campaign_name": row.campaign.name,
                        "status": row.campaign.status.name,
                        "impressions": row.metrics.impressions,
                        "clicks": row.metrics.clicks,
                        "cost_micros": row.metrics.cost_micros,
                        "date": row.segments.date,
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

