# Authentication for Google Ads

## Purpose

- Generate a long-lived **refresh token** via **one-time OAuth login** via browser

- Generate a reusable Google Ads credentials payload which only needs to be run only **ONCE**

- Use manual login with **Application Default Credentials** for local environment

- Use **Service Account** authentication to manage permissions in cloud environments

- Use centralized **Google Cloud Project** with required APIs enabled for cloud deployment

---

## Install

### Prerequisites for Bootstrap Authentication

- Google Ads **Developer Token** from Google Ads MCC UI

- Google Cloud Project with enabled **Google Ads API**

- Google Account with access to **Google Ads MCC**

- OAuth Client ID from a Google Cloud **Desktop App**

- Downloaded **JSON secret client** at OAuth2 App creation time 

---

### Run Bootstrap Authentication

- Fill required Google Ads MCC Developer Token variable
```text
GOOGLE_ADS_MCC_DEVELOPER_TOKEN = "PUT_YOUR_GOOGLE_ADS_MCC_DEVELOPER_TOKEN_HERE"
```

- Fill required Google Ads MCC customer ID variable
```text
GOOGLE_ADS_MCC_CUSTOMER_ID = "PUT_YOUR_GOOGLE_ADS_MCC_CUSTOMER_ID_HERE"
```

- Run `google_ads_oauth.py` to get assembled single payload credentials
```bash
python auth/google_ads_oauth.py
```

- Script opens browser for OAuth consent and example console output below:
```text
Successfully generated credential payload:
{
  "developer_token": "EXAMPLE-DEVELOPER-TOKEN",
  "client_id": "EAMPLE-CLIENT-ID.apps.googleusercontent.com",
  "client_secret": "EXAMPLE-CLIENT-SECRET",
  "refresh_token": "EXAMPLE_REFRESH_TOKEN_LONG_STRING",
  "login_customer_id": "1234567890"
}
```

- Copy the credential payload above then store it securely with `Secret Manager

- Use the credential payload to initialize `GoogleAdsClient`

---

## Revoke

### Use cases

- Google Ads will **invalidate refresh token** if access from OAuth Desktop App has been revoked

- Google Ads will **invalidate refresh token** if OAuth client ID has been changed or deleted

- Google Ads will **invalidate refresh token** if OAuth consent screen has been modified significantly

- Google Ads will **invalidate refresh token** if Google account password has been reset

- Google Ads will **invalidate refresh token** if this token is unused for 6 months

---

### Re-run Bootstrap Authentication

- Re-run `google_ads_oauth.py` to get assembled single payload credentials
```bash
python auth/google_ads_oauth.py
```

- Script opens browser for OAuth consent and example console output below:
```text
Successfully generated credential payload:
{
  "developer_token": "EXAMPLE-DEVELOPER-TOKEN",
  "client_id": "EAMPLE-CLIENT-ID.apps.googleusercontent.com",
  "client_secret": "EXAMPLE-CLIENT-SECRET",
  "refresh_token": "EXAMPLE_REFRESH_TOKEN_LONG_STRING",
  "login_customer_id": "1234567890"
}
```

- Copy the newly generated credential payload above then update new version with Secret Manager

- Use the credential payload to initialize `GoogleAdsClient`

## Local setup

### Local setup for Windows

- Download and install Google Cloud SDK from official source
```bash
https://cloud.google.com/sdk
```

- Verify installed Google Cloud SDK version
```bash
gcloud --version
```

- Login to Google Cloud on your Windows local environment
```bash
gcloud auth login
```

- Login and create **Application Default Credentials** (ADC) used by Google BigQueryAirflow/dbt/Terraform or any other Google Cloud client libraries
```bash
gcloud auth application-default login
```

- Verify authenticated Google accounts
```bash
gcloud auth list
```

- Check all accessible Google Cloud projects attached to the current ADC
```bash
gcloud projects list
```

- Set default Google Cloud project for Google BigQuery and quota billing
```bash
gcloud auth application-default set-quota-project YOUR_GOOGLE_CLOUD_PROJECT_ID
```

- Check Google Cloud quota project attached to ADC
```bash
gcloud config get-value project
```

- Verify ADC is working
```bash
gcloud auth application-default print-access-token
```

---

### Local setup for MacOS

- Install **Homebrew** from official source
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

- Add **Homebrew** to your system path once the installation finishes if you're using an **Apple Silicon Mac with M chip**
```bash
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"
```

- Verify Homebrew version
```bash
brew --version
```

- Download and install Google Cloud SDK from official source
```bash
brew install --cask google-cloud-sdk
```

- Verify installed Google Cloud SDK version
```bash
gcloud --version
```

- Login and create **Application Default Credentials** (ADC) used by Google BigQueryAirflow/dbt/Terraform or any other Google Cloud client libraries
```bash
gcloud auth application-default login
```

- Verify authenticated Google accounts
```bash
gcloud auth list
```

- Check all accessible Google Cloud projects attached to the current ADC
```bash
gcloud projects list
```

- Set default Google Cloud project for Google BigQuery and quota billing
```bash
gcloud auth application-default set-quota-project YOUR_GOOGLE_CLOUD_PROJECT_ID
```

- Check Google Cloud quota project attached to ADC
```bash
gcloud config get-value project
```

- Verify ADC is working
```bash
gcloud auth application-default print-access-token
```

## Cloud Run setup

### Enable minimum required APIs and services

- Enable **Cloud Run API** for container execution in the target Google Cloud project

- Enable **Cloud Run API** for container execution in the target Google Cloud project

- Enable **Google BigQuery API** for data warehouse access in the target Google Cloud project

---

### Enable Service Account

- Create a dedicated Google Cloud Platform's **Service Account** for pipeline_recon_ads

- Grant **Cloud Run Admin permissions** for required IAM Roles

- Grant **BigQuery Data Editor** and **BigQuery Job User** for required IAM Roles