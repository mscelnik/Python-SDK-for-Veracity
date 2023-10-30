""" How to verify a Veracity user/client token (ID or access).

Warning:
    For interactive (user) auth, your app registration must have http://localhost as a
    valid redirect URL.
"""

import os
from veracity_platform.identity import (
    ClientSecretCredential,
    InteractiveBrowserCredential,
    verify_token,
    TokenVerificationError,
)

RUN_INTERACTIVE = True

CLIENT_ID = os.environ.get("EXAMPLE_VERACITY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("EXAMPLE_VERACITY_CLIENT_SECRET")
SUBSCRIPTION_KEY = os.environ.get("EXAMPLE_VERACITY_SUBSCRIPTION_KEY")


def get_client_token():
    """ Get a token for the client/app (no user)."""
    cred = ClientSecretCredential(CLIENT_ID, CLIENT_SECRET)
    return cred.get_token(["veracity"])


def get_user_token():
    """ Get a token for the user (pops up a login page in browser)."""
    cred = InteractiveBrowserCredential(CLIENT_ID, client_secret=CLIENT_SECRET)
    return cred.get_token(["veracity"])


if __name__ == "__main__":
    if RUN_INTERACTIVE:
        token = get_user_token()
    else:
        token = get_client_token()

    print(token)

    # Verify the access token with the issuing authority (Veracity or Microsoft).
    try:
        verify_token(token['access_token'])
        print("Access token is valid")
    except TokenVerificationError:
        print("Access token is not valid!")

    # Verify the ID token with the issuing authority (Veracity).  Only applicable
    # for user tokens.
    if RUN_INTERACTIVE:
        try:
            verify_token(token['id_token'])
            print("ID token is valid")
        except TokenVerificationError:
            print("ID token is not valid!")

    # TODO: Optionally check the token belongs to a user authorised to access your app.
    pass
