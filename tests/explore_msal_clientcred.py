""" Testing MSAL for client credential applications.
"""

import os
import msal

from tests.conftest import CLIENT_SECRET

EXAMPLE_VERACITY_CLIENT_ID = os.environ["EXAMPLE_VERACITY_CLIENT_ID"]
EXAMPLE_VERACITY_CLIENT_SECRET = os.environ["EXAMPLE_VERACITY_CLIENT_SECRET"]
EXAMPLE_VERACITY_SUBSCRIPTION_KEY = os.environ["EXAMPLE_VERACITY_SUBSCRIPTION_KEY"]

client = msal.ConfidentialClientApplication(
    client_id=EXAMPLE_VERACITY_CLIENT_ID,
    client_credential=EXAMPLE_VERACITY_CLIENT_SECRET,
    authority="https://login.microsoftonline.com/dnvglb2cprod.onmicrosoft.com",
)

client.acquire_token_for_client()
