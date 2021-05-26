""" Example showing how to authenticate as an application without a user.

This flow is suitable for web services which require access to Veracity services
but do not operate on user data.
"""

import os
from veracity_platform.identity import ClientSecretCredential

CLIENT_ID = os.environ.get("EXAMPLE_VERACITY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("EXAMPLE_VERACITY_CLIENT_SECRET")
SUBSCRIPTION_KEY = os.environ.get("EXAMPLE_VERACITY_SUBSCRIPTION_KEY")
RESOURCE_URL = 'https://dnvglb2cprod.onmicrosoft.com/dfba9693-546d-4300-bcd7-d8d525bdff38'

cred = ClientSecretCredential(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
token = cred.get_token(scopes=['veracity'])
print(token)
