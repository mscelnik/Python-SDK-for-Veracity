""" Example showing how to authenticate as an application without a user.

This flow is suitable for web services which require access to Veracity services
but do not operate on user data.
"""

import os
import requests
import msal
from veracity_platform.identity import ClientSecretCredential, InteractiveBrowserCredential

CLIENT_ID = os.environ.get("EXAMPLE_VERACITY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("EXAMPLE_VERACITY_CLIENT_SECRET")
SUBSCRIPTION_KEY = os.environ.get("EXAMPLE_VERACITY_SUBSCRIPTION_KEY")
RESOURCE_URL = 'https://dnvglb2cprod.onmicrosoft.com/dfba9693-546d-4300-bcd7-d8d525bdff38'


# cred = InteractiveBrowserCredential(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
# token = cred.get_token(scopes=['veracity'])

# cred = ClientSecretCredential(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
# token = cred.get_token(scopes=['veracity'])

client = msal.ConfidentialClientApplication(
    client_id=CLIENT_ID,
    client_credential=CLIENT_SECRET,
    authority="https://login.microsoftonline.com/dnvglb2cprod.onmicrosoft.com",
)

token = client.acquire_token_for_client(scopes=['https://dnvglb2cprod.onmicrosoft.com/83054ebf-1d7b-43f5-82ad-b2bde84d7b75/.default'])


print(token)
access_token = token['access_token']


url = 'https://api.veracity.com/veracity/services/v3/this/services?page=1&pageSize=1'
headers = {
    'Authorization': f'Bearer {access_token}',
    'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY,
}
response = requests.get(url, headers=headers)
print(response)
print(response.text)
