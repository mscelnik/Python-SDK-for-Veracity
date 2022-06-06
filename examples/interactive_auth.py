""" Example showing how a user authenticates with Veracity locally with their webbrowser.

This flow is suitable for native applications running on the user's own computer.

Notes:
    For native apps, you must use the http://localhost redirect URI.  This must be
    specified in the Veracity developer portal exactly as used here.

"""

import os
import requests
from veracity_platform.identity import InteractiveBrowserCredential, verify_token


CLIENT_ID = os.environ.get("TEST_CONF_APP_ID")
CLIENT_SECRET = os.environ.get("TEST_CONF_APP_SECRET")
SUBSCRIPTION_KEY = os.environ.get("TEST_CONF_APP_SUB")
REDIRECT_URI = "http://localhost/login"

cred = InteractiveBrowserCredential(CLIENT_ID, REDIRECT_URI, client_secret=CLIENT_SECRET)
scopes = ["veracity"]
token = cred.get_token(scopes=scopes, timeout=30)
print(f"Veracity API token:\n{token}\n\n")
assert "access_token" in token

print(verify_token(token["access_token"]))

# Use the token to get some information from Veracity API.
url = "https://api.veracity.com/veracity/datafabric/data/api/1/resources"
headers = {
    "Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY,
    "Authorization": f'Bearer {token["access_token"]}',
}
response = requests.get(url, headers=headers)
print(response.status_code)
print(response.text)
