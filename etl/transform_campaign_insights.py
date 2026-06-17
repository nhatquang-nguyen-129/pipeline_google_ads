import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

def transform_campaign_insights(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform Google Ads campaign insights
    ---
    Principles:
        1. Validate input
        2. Parse actions
        3. Resolve results
        4. Normalize date dimension
        5. Enforce numeric schema
    ---
    Returns:
        1. pandas.DataFrame:
            Enforced campaign insights records
    """

    # Validate input
    print(
        "🔄 [TRANSFORM] Validating column(s) for "
        f"{len(df)} row(s) of Google Ads campaign insights..."
    )

    if df.empty:

        raise ValueError(
            "❌ [TRANSFORM] Failed to validate column(s) for Google Ads campaign insights due to empty input DataFrame."
        )

    required_cols = {"date"}

    actual_cols = {
        str(col).strip()
        for col in df.columns
    }

    missing_cols = required_cols - actual_cols

    extra_cols = actual_cols - required_cols

    print(
        "✅ [TRANSFORM] Successfully validated DataFrame for Google Ads campaign insights with "
        f"{df.shape} shape with total column(s) "
        f"{len(actual_cols)}/{len(required_cols)} total column including "
        f"{len(missing_cols)} missing column(s) and "
        f"{len(extra_cols)} extra column(s)."
    )

    if missing_cols:

        raise ValueError(
            "❌ [TRANSFORM] Failed to transform validated DataFrame for Google Ads campaign insights due to missing required column(s) "
            f"{sorted(missing_cols)}"
        )
    
    # Parse columns
    df = df.copy()
    
    df["customer_id"] = df["customer_id"].astype(str)
    
    df["campaign_id"] = df["campaign_id"].astype(str)
    
    df["impressions"] = df["impressions"].astype("int64")
    
    df["clicks"] = df["clicks"].astype("int64")
    
    df["spend"] = df["cost"].round().astype("int64")
    
    df["conversions"] = df["conversions"].astype("float64")
    
    df["conversion_value"] = df["conversion_value"].astype("float64")    
    
    df = df.assign(
        date=pd.to_datetime(df["date"], errors="coerce", utc=True).dt.floor("D"),
        year=pd.to_datetime(df["date"], errors="coerce", utc=True).dt.year,
        month=pd.to_datetime(df["date"], errors="coerce", utc=True).dt.strftime("%Y-%m"),
    )

    df = df.drop(columns=["cost"])

    print(
        "✅ [TRANSFORM] Successfully transformed Google Ads campaign insights with "
        f"{len(df)} row(s)."
    )

    return df