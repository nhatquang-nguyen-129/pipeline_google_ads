from google_auth_oauthlib.flow import InstalledAppFlow

# Thông tin client
CLIENT_CONFIG = {
    "installed": {
        "client_id": "44783457431-4c7etrfa2s329v5vmhk0ums82hj3o4ve.apps.googleusercontent.com",
        "client_secret": "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",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": ["http://localhost"]
    }
}

SCOPES = ["https://www.googleapis.com/auth/adwords"]

flow = InstalledAppFlow.from_client_config(CLIENT_CONFIG, SCOPES)

creds = flow.run_local_server(port=0)  # sẽ mở trình duyệt
print("Refresh token:", creds.refresh_token)