import os
import requests
from veracity_platform.identity import InteractiveBrowserCredential

CLIENT_ID = os.environ['EXAMPLE_VERACITY_CLIENT_ID']
CLIENT_SECRET = os.environ['EXAMPLE_VERACITY_CLIENT_SECRET']
SUBSCRIPTION = os.environ['EXAMPLE_VERACITY_SUBSCRIPTION_KEY']
REDIRECT_URI = "http://localhost/login"

cred = InteractiveBrowserCredential(
    client_id=CLIENT_ID,
    redirect_uri=REDIRECT_URI,
    client_secret=CLIENT_SECRET,
)

token = cred.get_token(scopes=['https://dnvglb2cprod.onmicrosoft.com/dfba9693-546d-4300-bcd7-d8d525bdff38'])
print(token)
access_token = token['access_token']

uri = "https://api.veracity.com/veracity/datafabric/search/api/v1/me/Services"
response = requests.get(
    uri,
    headers={'Ocp-Apim-Subscription-Key': SUBSCRIPTION, 'Authorization': f'Bearer {access_token}'},
)

print(response.status_code)
print(response.text)

