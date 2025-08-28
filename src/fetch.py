import sys
from google_auth_oauthlib.flow import InstalledAppFlow

# Thay bằng thông tin của bạn
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

def main():
    flow = InstalledAppFlow.from_client_config(
        {
            "installed": {
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        },
        scopes=["https://www.googleapis.com/auth/adwords"],
    )

    creds = flow.run_local_server(port=8080, prompt="consent", authorization_prompt_message="")
    print(f"\nRefresh token:\n{creds.refresh_token}\n")

if __name__ == "__main__":
    main()
