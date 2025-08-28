"""
==================================================================
MAIN ENTRYPOINT FOR PLATFORM-AGNOSTIC DATA UPDATES
------------------------------------------------------------------
This script serves as the unified CLI **controller** for triggering  
ads data updates across multiple platforms (e.g., Facebook, Google),  
based on command-line arguments and environment variables.

It supports **incremental daily ingestion** for selected data layers  
(e.g., campaign, ad) and allows flexible control over date ranges.

✔️ Dynamic routing to the correct update module based on PLATFORM  
✔️ CLI flags to select data layers and date range mode  
✔️ Shared logging and error handling across update jobs  
✔️ Supports scheduled jobs or manual on-demand executions  

⚠️ This script does *not* contain data processing logic itself.  
It simply delegates update tasks to platform-specific modules  
(e.g., services.facebook.update, services.budget.update).
==================================================================
"""
# Add project root to sys.path to enable absolute imports
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add argparse to parse comman-line arguments
import argparse

# Add dynamic platform-specific modules
import importlib

# Import datetime to calculate time
from datetime import datetime, timedelta

# Add logging capability for tracking process execution and errors
import logging

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

# Get validated environment variables
if not all([COMPANY, PLATFORM, ACCOUNT, LAYER, MODE]):
    raise EnvironmentError("❌ [MAIN] Missing required environment variables COMPANY/PLATFORM/ACCOUNT/LAYER/MODE.")

# 1. DYNAMIC IMPORT MODULE BASED ON PLATFORM
try:
    update_module = importlib.import_module(f"services.{PLATFORM}.update")
except ModuleNotFoundError:
    raise ImportError(f"❌ [MAIN] Platform '{PLATFORM}' is not supported so please ensure services/{PLATFORM}/update.py exists.")

# 1.2. Main entrypoint function
def main():
    today = datetime.today()

    # 1.2.1. PLATFORM = facebook (keep original logic)
    if PLATFORM == "facebook":
        try:
            update_campaign_insights = update_module.update_campaign_insights
            update_ad_insights = update_module.update_ad_insights
        except AttributeError:
            raise ImportError(f"❌ [MAIN] Facebook update module must define update_campaign_insights and update_ad_insights.")
        layers = [layer.strip() for layer in LAYER.split(",") if layer.strip()]
        if len(layers) != 1:
            raise ValueError("⚠️ [MAIN] Only one layer is supported per execution so please run separately for each layer.")
        if MODE == "today":
            start_date = end_date = today.strftime("%Y-%m-%d")
        elif MODE == "last3days":
            start = today - timedelta(days=3)
            start_date = start.strftime("%Y-%m-%d")
            end_date = today.strftime("%Y-%m-%d")
        elif MODE == "last7days":
            start = today - timedelta(days=7)
            start_date = start.strftime("%Y-%m-%d")
            end_date = today.strftime("%Y-%m-%d")
        elif MODE == "thismonth":
            start = today.replace(day=1)
            start_date = start.strftime("%Y-%m-%d")
            end_date = today.strftime("%Y-%m-%d")
        elif MODE == "lastmonth":
            first_day_this_month = today.replace(day=1)
            last_day_last_month = first_day_this_month - timedelta(days=1)
            first_day_last_month = last_day_last_month.replace(day=1)
            start_date = first_day_last_month.strftime("%Y-%m-%d")
            end_date = last_day_last_month.strftime("%Y-%m-%d")
        else:
            raise ValueError(f"⚠️ [MAIN] Unsupported mode {MODE} so please re-check input environment variable.")
        if "campaign" in layers:
            print(f"🚀 [MAIN] Starting to update {PLATFORM} campaign insights of {COMPANY} in {MODE} mode and {layers} layer from {start_date} to {end_date}...")
            logging.info(f"🚀 [MAIN] Starting to update {PLATFORM} campaign insights of {COMPANY} in {MODE} mode and {layers} layer from {start_date} to {end_date}...")
            update_campaign_insights(start_date=start_date, end_date=end_date)
            print(f"✅ [MAIN] Successfully completed update {PLATFORM} campaign insights of {COMPANY} in {MODE} mode and {layers} layer from {start_date} to {end_date}.")
            logging.info(f"✅ [MAIN] Successfully completed update {PLATFORM} campaign insights of {COMPANY} in {MODE} mode and {layers} layer from {start_date} to {end_date}.")
        if "ad" in layers:
            print(f"🚀 [MAIN] Starting to update {PLATFORM} ad insights of {COMPANY} in {MODE} mode and {layers} layer from {start_date} to {end_date}...")
            logging.info(f"🚀 [MAIN] Starting to update {PLATFORM} ad insights of {COMPANY} in {MODE} mode and {layers} layer from {start_date} to {end_date}...")
            update_ad_insights(start_date=start_date, end_date=end_date)
            print(f"✅ [MAIN] Successfully completed update {PLATFORM} ad insights of {COMPANY} in {MODE} mode and {layers} layer from {start_date} to {end_date}.")
            logging.info(f"✅ [MAIN] Successfully completed update {PLATFORM} ad insights of {COMPANY} in {MODE} mode and {layers} layer from {start_date} to {end_date}.")

    # 1.2.2. PLATFORM = budget
    elif PLATFORM == "budget":
        try:
            update_budget_allocation = update_module.update_budget_allocation
        except AttributeError:
            raise ImportError(f"❌ [MAIN] Budget update module must define 'update_budget_allocation'.")
        if MODE == "thismonth":
            thang = today.strftime("%Y-%m")   # e.g. "2025-08"
        elif MODE == "lastmonth":
            first_day_this_month = today.replace(day=1)
            last_day_last_month = first_day_this_month - timedelta(days=1)
            thang = last_day_last_month.strftime("%Y-%m")   # e.g. "2025-07"
        else:
            raise ValueError(f"⚠️ [MAIN] Unsupported mode {MODE} for budget. Use thismonth or lastmonth.")
        if LAYER != "all":
            raise ValueError("⚠️ [MAIN] Budget only supports LAYER=all.")
        print(f"🚀 [MAIN] Starting to update budget allocation of {COMPANY} for {thang}...")
        logging.info(f"🚀 [MAIN] Starting to update budget allocation of {COMPANY} for {thang}...")
        update_budget_allocation(thang)
        print(f"✅ [MAIN] Successfully completed update budget allocation of {COMPANY} for {thang}.")
        logging.info(f"✅ [MAIN] Successfully completed update budget allocation of {COMPANY} for {thang}.")

     # 1.2.3. PLATFORM = ads
    elif PLATFORM == "ads":
        try:
            update_spend = update_module.update_spend_all
            update_recon = update_module.update_recon_all
        except AttributeError:
            raise ImportError(f"❌ [MAIN] Ads update module must define 'mart_spend_all' and 'mart_recon_all'.")
        layers = [layer.strip() for layer in LAYER.split(",") if layer.strip()]
        if len(layers) != 1:
            raise ValueError("⚠️ [MAIN] Ads only supports one LAYER per execution (spend or recon).")
        if MODE != "all":
            raise ValueError("⚠️ [MAIN] Ads only supports MODE=all.")
        layer = layers[0]
        if layer == "spend":
            print(f"🚀 [MAIN] Starting to build unified ads spend mart for {COMPANY}...")
            logging.info(f"🚀 [MAIN] Starting to build unified ads spend mart for {COMPANY}...")
            update_spend()
            print(f"✅ [MAIN] Successfully built unified ads spend mart for {COMPANY}.")
            logging.info(f"✅ [MAIN] Successfully built unified ads spend mart for {COMPANY}.")
        elif layer == "recon":
            print(f"🚀 [MAIN] Starting to build unified ads spend reconciliation mart for {COMPANY}...")
            logging.info(f"🚀 [MAIN] Starting to build unified ads spend reconciliation mart for {COMPANY}...")
            update_recon()
            print(f"✅ [MAIN] Successfully built unified ads spend reconciliation mart for {COMPANY}.")
            logging.info(f"✅ [MAIN] Successfully built unified ads spend reconciliation mart for {COMPANY}.")
        else:
            raise ValueError(f"⚠️ [MAIN] Unsupported ads LAYER={layer}. Use spend or recon.")

# 1.3. Entrypoint guard to run main() when this script is executed directly
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"❌ Update failed: {e}")
        sys.exit(1)
