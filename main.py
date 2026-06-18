import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from datetime import datetime, timedelta
import json
import traceback
from zoneinfo import ZoneInfo

from google.cloud import secretmanager
from google.api_core.client_options import ClientOptions

from dags.dags_google_ads import dags_google_ads

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")
MODE = os.getenv("MODE")

if not all([
    COMPANY,
    PROJECT,
    DEPARTMENT,
    ACCOUNT,
    MODE
]):

    raise EnvironmentError(
        "❌ [MAIN] Failed to execute Google Ads main entrypoint due to missing required environment variables."
    )

def main():
    """
    Main Google Ads entrypoint
    ---
    Principles:
        1. Resolve execution time window from MODE
        2. Read & validate OS environment variables
        3. Load secrets from GCP Secret Manager
        4. Resolve customer_id and access_token
        5. Dispatch execution to DAG orchestrator
    ---
    Returns:
        None
    """
    
    print(
        "🔄 [MAIN] Triggering to execute Google Ads main entrypoint for "
        f"{ACCOUNT} account of "
        f"{DEPARTMENT} department in "
        f"{COMPANY} company with "
        f"{MODE} mode to Google Cloud project "
        f"{PROJECT}..."
    ) 

    # Resolve input time range
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")
    
    today = datetime.now(ICT)
    
    if MODE == "today":
    
        start_date = end_date = today.strftime("%Y-%m-%d")

    elif MODE == "last3days":
    
        start_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")
    
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    elif MODE == "last7days":

        start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")

        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    elif MODE == "thismonth":

        start_date = today.replace(day=1).strftime("%Y-%m-%d")

        end_date = today.strftime("%Y-%m-%d")

    elif MODE == "lastmonth":

        last_month_end = today.replace(day=1) - timedelta(days=1)

        start_date = last_month_end.replace(day=1).strftime("%Y-%m-%d")

        end_date = last_month_end.strftime("%Y-%m-%d")

    else:

        raise ValueError(
            "⚠️ [MAIN] Failed to trigger Google Ads main entrypoint due to unsupported mode "
            f"{MODE}."
        )
    
    print(
        "✅ [MAIN] Successfully resolved "
        f"{MODE} mode to date range from "
        f"{start_date} to "
        f"{end_date}."
    )

    # Initialize Google Secret Manager
    try:
        
        print(
            "🔍 [MAIN] Initialize Google Secret Manager client..."
        )
        
        google_secret_client = secretmanager.SecretManagerServiceClient(
            client_options=ClientOptions(
                api_endpoint="secretmanager.googleapis.com"
            )
        )

        print(
            "✅ [MAIN] Successfully initialized Google Secret Manager client."
        )
    
    except Exception as e:
        
        raise RuntimeError(
            "❌ [MAIN] Failed to initialize Google Secret Manager client due to "
            f"{e}."
        )
        
    # Resolve customer_id from Google Secret Manager
    secret_customer_id = (
        f"{COMPANY}_secret_{DEPARTMENT}_google_account_id_{ACCOUNT}"
    )
    
    secret_customer_name = (
        f"projects/{PROJECT}/secrets/{secret_customer_id}/versions/latest"
    )
    
    try:
    
        print(
            "🔍 [MAIN] Retrieving Google Ads secret_customer_id "
            f"{secret_customer_id} from Google Secret Manager..."
        )       

        secret_customer_response = google_secret_client.access_secret_version(
            name=secret_customer_name,
            timeout=10.0,
        )
    
        google_customer_id = (
            secret_customer_response.payload.data.decode("utf-8")
            .replace("-", "")
            .replace(" ", "")
            .strip()
        )
        
        print(
            "✅ [MAIN] Successfully retrieved Google Ads customer_id "
            f"{google_customer_id} from Google Secret Manager."
        )
    
    except Exception as e:
    
        raise RuntimeError(
            "❌ [MAIN] Failed to retrieve Google Ads customer_id from Google Secret Manager due to "
            f"{e}."
        )

    # Resolve JSON credentials from Google Secret Manager
    secret_credentials_json = (
        f"{COMPANY}_secret_all_google_token_access_user"
    )
    
    secret_credentials_name = (
        f"projects/{PROJECT}/secrets/{secret_credentials_json}/versions/latest"
    )

    try:       
    
        print(
            "🔍 [MAIN] Retrieving Google Ads secret_credentials_json "
            f"{secret_credentials_json} from Google Secret Manager..."
        )

        secret_credentials_response = google_secret_client.access_secret_version(
            name=secret_credentials_name
        )
    
        google_ads_credentials = json.loads(
            secret_credentials_response.payload.data.decode("UTF-8")
        )
        
        print(
            "✅ [MAIN] Successfully retrieved Google Ads credentials from Google Secret Manager."
        )
    
    except Exception as e:
    
        raise RuntimeError(
            "❌ [MAIN] Failed to retrieve Google Ads credentials from Google Secret Manager due to "
            f"{e}."
        )        

    # Execute DAGs
    dags_google_ads(
        google_ads_credentials=google_ads_credentials,
        customer_id=google_customer_id,
        start_date=start_date,
        end_date=end_date
    )

    # Entrypoint
if __name__ == "__main__":

    try:

        main()

    except Exception:

        print(
            "❌ [MAIN] Failed to execute Google Ads main entrypoint due to..."
        )

        traceback.print_exc()

        sys.exit(1)