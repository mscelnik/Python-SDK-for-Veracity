""" Example showing how a user authenticates with Veracity locally with their webbrowser.

This flow is suitable for native applications running on the user's own computer.

Notes:
    For native apps, you must use the http://localhost redirect URI.  This must be
    specified in the Veracity developer portal exactly as used here.

"""

import os
import requests
from veracity_platform.identity import InteractiveBrowserCredential


CLIENT_ID = os.environ.get("TEST_CONF_APP_ID")
# TODO: Secret should not be necessary for user-auth flow, but Veracity IDP doesn't work without it.
CLIENT_SECRET = os.environ.get("TEST_CONF_APP_SECRET")
SUBSCRIPTION_KEY = os.environ.get("TEST_CONF_APP_SUB")
REDIRECT_URI = "http://localhost/login"

cred = InteractiveBrowserCredential(CLIENT_ID, REDIRECT_URI, client_secret=CLIENT_SECRET)
scopes = ["veracity"]
# scopes = ['https://dnvglb2cprod.onmicrosoft.com/83054ebf-1d7b-43f5-82ad-b2bde84d7b75/user_impersonation']
token = cred.get_token(scopes=scopes, timeout=30)
print(f"Veracity API token:\n{token}\n\n")
assert "access_token" in token


# Use the token to get some information from Veracity API.
url = "https://api.veracity.com/veracity/datafabric/data/api/1/resources"
headers = {
    "Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY,
    "Authorization": f'Bearer {token["access_token"]}',
}
response = requests.get(url, headers=headers)
print(response.status_code)
print(response.text)
