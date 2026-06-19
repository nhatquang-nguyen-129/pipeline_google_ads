import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from typing import List
import pandas as pd

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

def extract_campaign_metadata(
    google_ads_credentials: dict,
    customer_id: str,
    campaign_id_list: List[str],
) -> pd.DataFrame:
    """
    Extract Google Ads campaign metadata
    ---
    Principles:
        1. Initialize Google Ads client
        2. Execute GAQL query for campaign metadata
        3. Stream using search_stream API
        4. Return extracted data
        5. Enforce List[dict] to DataFrame
    ---
    Returns:
        1. pandas.DataFrame:
            Flattened Google Ads campaign metadata records
    """

    # Validate input
    if not campaign_id_list:
        
        print(
            "⚠️ [EXTRACT] Failed to extract Google Ads campaign metadata for customer_id "
            f"{customer_id} due to no input campaign_ids then extraction will be suspended."
        )
        
        return pd.DataFrame()

    # Initialize Google Ads client
    google_ads_config = {
        "developer_token": google_ads_credentials["developer_token"],
        "client_id": google_ads_credentials["client_id"],
        "client_secret": google_ads_credentials["client_secret"],
        "refresh_token": google_ads_credentials["refresh_token"],
        "login_customer_id": google_ads_credentials["login_customer_id"],
        "use_proto_plus": True,
    }

    try:
        
        print(
            "🔍 [EXTRACT] Initializing Google Ads client for customer_id "
            f"{customer_id}..."
        )
        
        google_ads_client = GoogleAdsClient.load_from_dict(
            google_ads_config
        )

        print(
            "✅ [EXTRACT] Successfully initialized Google Ads client."
        )
    
    except Exception as e:

        error = RuntimeError(
            "❌ [EXTRACT] Failed to initialize Google Ads client due to "
            f"{e}."
        )

        error.retryable = False
        
        raise error from e

    # Make Google Ads API call for campaign metadata
    rows: List[dict] = [] 
    
    campaign_ids_str = ", ".join(str(cid) for cid in campaign_id_list)    
    
    _QUERY_CAMPAIGN_METADATA = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.serving_status,           
            customer.id
        FROM campaign
        WHERE campaign.id IN ({campaign_ids_str})
    """

    try:        
        
        print(
            "🔍 [EXTRACT] Extracting Google Ads campaign metadata for customer_id "
            f"{customer_id} with "
            f"{len(campaign_id_list)} campaign_id(s)..."
        )
        
        google_ads_service = google_ads_client.get_service("GoogleAdsService")        
        
        response = google_ads_service.search(
            customer_id=customer_id,
            query=_QUERY_CAMPAIGN_METADATA,
        )       

        for row in response:
        
            campaign = row.campaign

            rows.append({
                "customer_id": str(row.customer.id),
                "campaign_id": str(campaign.id),
                "campaign_name": campaign.name,
                "campaign_status": campaign.status.name,
            })

        df = pd.DataFrame(rows)

        print(
            "✅ [EXTRACT] Successfully extracted Google Ads campaign metadata for customer_id "
            f"{customer_id} with "
            f"{len(df)}/{len(campaign_id_list)} row(s)."
        )

        return df

    except GoogleAdsException as e:

        error_codes = [
            failure.error_code for failure in e.failure.errors
        ]

        # Retryable API error
        if any(
            failure.error_code.internal_error
            or failure.error_code.resource_exhausted
            or failure.error_code.server_error
            or failure.error_code.timeout_error
            for failure in e.failure.errors
        ):
            
            error = RuntimeError(
                "⚠️ [EXTRACT] Failed to extract Google Ads campaign metadata for customer_id "
                f"{customer_id} with "
                f"{len(campaign_id_list)} campaign_id(s) due to "
                f"{error_codes} then this request is eligible to retry."
            )
            
            error.retryable = True
            
            raise error from e

        # Non-retryable API error
        if any(
            failure.error_code.authentication_error
            or failure.error_code.authorization_error
            or failure.error_code.request_error
            or failure.error_code.field_error
            or failure.error_code.policy_finding_error
            for failure in e.failure.errors
        ):
           
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract Google Ads campaign metadata for customer_id "
                f"{customer_id} with "
                f"{len(campaign_id_list)} campaign_id(s) due to "
                f"{error_codes} then this request is not eligible to retry."
            )
           
            error.retryable = False
           
            raise error from e

        # Retryable request timeout error
    except TimeoutError as e:
        
        error = RuntimeError(
            "⚠️ [EXTRACT] Failed to extract Google Ads campaign metadata for customer_id "
            f"{customer_id} with "
            f"{len(campaign_id_list)} campaign_id(s) due to request timeout error then this request is eligible to retry."
        )
        
        error.retryable = True
        
        raise error from e

        # Retryable request connection error
    except ConnectionError as e:
        
        error = RuntimeError(
            "⚠️ [EXTRACT] Failed to extract Google Ads campaign metadata for customer_id "
            f"{customer_id} with "
            f"{len(campaign_id_list)} campaign_id(s) due to request connection error then this request is eligible to retry."
        )
        
        error.retryable = True
        
        raise error from e

        # Unknown non-retryable error
    except Exception as e:
        
        error = RuntimeError(
            "❌ [EXTRACT] Failed to extract Google Ads campaign metadata for customer_id "
            f"{customer_id} with "
            f"{len(campaign_id_list)} campaign_id(s) due to "
            f"{e}."
        )
        
        error.retryable = False
        
        raise error from e