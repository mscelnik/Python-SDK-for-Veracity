""" Manual tests using MSAL/azure.identity with inline policy.
"""

import os
import azure.identity
from msal import authority
import veracity_platform
import msal

EXAMPLE_VERACITY_CLIENT_ID = os.environ["EXAMPLE_VERACITY_CLIENT_ID"]
EXAMPLE_VERACITY_CLIENT_SECRET = os.environ["EXAMPLE_VERACITY_CLIENT_SECRET"]
EXAMPLE_VERACITY_SUBSCRIPTION_KEY = os.environ["EXAMPLE_VERACITY_SUBSCRIPTION_KEY"]

scope = veracity_platform.identity.ALLOWED_SCOPES["veracity"]

# This does not work.  It complains that you need to provide a client credential.
client = msal.PublicClientApplication(
    client_id=EXAMPLE_VERACITY_CLIENT_ID,
    # client_credential=EXAMPLE_VERACITY_CLIENT_SECRET,
    authority="https://login.veracity.com/a68572e3-63ce-4bc1-acdc-b64943502e9d/B2C_1A_SignInWithADFSIdp",
    validate_authority=False,
)
token = client.acquire_token_interactive([scope])

# cred = azure.identity.InteractiveBrowserCredential(
#     authority="https://login.veracity.com/a68572e3-63ce-4bc1-acdc-b64943502e9d/B2C_1A_SignInWithADFSIdp",
#     tenant_id='',
#     client_id=EXAMPLE_VERACITY_CLIENT_ID,
#     redirect_uri="http://localhost:80/login",
#     validate_authority=False,
# )
# token = cred.get_token([scope])

print(token)
