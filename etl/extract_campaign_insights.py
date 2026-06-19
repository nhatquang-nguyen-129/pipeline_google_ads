import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from typing import List
import pandas as pd

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

def extract_campaign_insights(
    *,
    google_ads_credentials: dict,
    customer_id: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    Extract Google Ads campaign insights
    ---
    Principles:
        1. Initialize Google Ads client
        2. Execute GAQL query for campaign insights
        3. Stream using search_stream API
        4. Return extracted data
        5. Enforce List[dict] to DataFrame
    ---
    Returns:
        1. pandas.DataFrame:
            Flattened Google Ads campaign insight records
    """

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

    # Make Google Ads API call for campaign insights
    rows: List[dict] = []
    
    batch_count = 0 

    _QUERY_CAMPAIGN_INSIGHTS = f"""
        SELECT
            segments.date,
            customer.id,
            campaign.id,
            campaign.advertising_channel_type,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
    """
    
    try:
        
        print(
            "🔍 [EXTRACT] Extracting Google Ads campaign insights for customer_id "
            f"{customer_id} from start_date "
            f"{start_date} to end_date "
            f"{end_date}..."
        )       

        google_ads_service = google_ads_client.get_service("GoogleAdsService")
        
        request = google_ads_client.get_type("SearchGoogleAdsStreamRequest")
        
        request.customer_id = customer_id
        
        request.query = _QUERY_CAMPAIGN_INSIGHTS        
        
        stream = google_ads_service.search_stream(request=request)

        for batch in stream:

            batch_count += 1

            for row in batch.results:

                rows.append({
                    "date": row.segments.date,
                    "customer_id": row.customer.id,
                    "campaign_id": row.campaign.id,
                    "channel_type": row.campaign.advertising_channel_type.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "cost": row.metrics.cost_micros / 1_000_000,
                    "conversions": row.metrics.conversions,
                    "conversion_value": row.metrics.conversions_value,
                })

        df = pd.DataFrame(rows)

        print(
            "✅ [EXTRACT] Successfully extracted Google Ads campaign insights with "
            f"{len(df)} row(s) for customer_id "
            f"{customer_id} from "
            f"{start_date} to "
            f"{end_date} with "
            f"{batch_count} batch(es)."
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
                "⚠️ [EXTRACT] Failed to extract Google Ads campaign insights for customer_id "
                f"{customer_id} from "
                f"{start_date} to "
                f"{end_date} due to "
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
                "❌ [EXTRACT] Failed to extract Google Ads campaign insights for customer_id "
                f"{customer_id} from "
                f"{start_date} to "
                f"{end_date} due to "
                f"{error_codes} then this request is not eligible to retry."
            )
            
            error.retryable = False
            
            raise error from e

        # Retryable request timeout error
    except TimeoutError as e:
        
        error = RuntimeError(
            "⚠️ [EXTRACT] Failed to extract Google Ads campaign insights for customer_id "
            f"{customer_id} from "
            f"{start_date} to "
            f"{end_date} due to request timeout error then this request is eligible to retry."
        )
        
        error.retryable = True
        
        raise error from e

        # Retryable request connection error
    except ConnectionError as e:
        
        error = RuntimeError(
            "⚠️ [EXTRACT] Failed to extract Google Ads campaign insights for customer_id "
            f"{customer_id} from "
            f"{start_date} to "
            f"{end_date} due to request connection error then this request is eligible to retry."
        )
        
        error.retryable = True
        
        raise error from e

        # Unknown non-retryable error
    except Exception as e:
        
        error = RuntimeError(
            "❌ [EXTRACT] Failed to extract Google Ads campaign insights for customer_id "
            f"{customer_id} from "
            f"{start_date} to "
            f"{end_date} due to "
            f"{e}."
        )
        
        error.retryable = False
        
        raise error from e