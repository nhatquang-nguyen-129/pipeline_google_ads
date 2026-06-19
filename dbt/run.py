import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import subprocess

def dbt_google_ads(
    *,
    google_cloud_project: str,
    select: str
):
    """
    DBT Execution for Google Ads
    ---
    Principles:
        1. Initialize dbt CLI execution environment
        2. Construct dbt build command with model selection
        3. Execute dbt build within project directory context
        4. Capture subprocess execution status and surface failures
        5. Finalize execution with success confirmation
    ---
    Returns:
        1. None:
    """

    cmd = [
        "dbt",
        "build",
        "--profiles-dir", ".",
        "--select", select,
        "--no-write-json"
    ]

    print(
        f"🔄 [DBT] Executing dbt build for Google Ads "
        f"{select} selector to Google Cloud Project "
        f"{google_cloud_project}..."
    )

    try:

        process = subprocess.Popen(
            cmd,
            cwd="dbt",
            env=os.environ,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        for line in process.stdout:

            print(line, end="")

        process.wait()

        if process.returncode != 0:

            raise RuntimeError(
                "❌ [DBT] Failed to execute dbt build for Google Ads "
                f"{select} selector to Google Cloud Project "
                f"{google_cloud_project} with return code "
                f"{process.returncode}."
            )

        print(
            f"✅ [DBT] Successfully executed dbt build for Google Ads "
            f"{select} selector to Google Cloud Project "
            f"{google_cloud_project}."
        )

    except Exception as e:
        
        raise RuntimeError(
            "❌ [DBT] Failed to execute dbt build for Google Ads "
            f"{select} selector to Google Cloud Project "
            f"{google_cloud_project} due to "
            f"{e}."
        ) from e