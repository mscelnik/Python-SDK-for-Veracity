""" Demo Flask web app using the Veracity identity service.

This example retrieves a user token using auth-code-flow.  Both parts of the flow
are handled by the /login endpoint.  Note, the app's Reply URL must be set in
the developer portal exactly so in order for Veracity to return a token:

    - http://localhost/login           - when running the app locally.
    - https://<your_host_name>/login   - when deployed online.

Requirements:

    1. Before running, install Flask:

      $ pip install flask flask-session

"""

import os
from flask import Flask, request, redirect, session, url_for

# from flask_session import Session
from veracity_platform.identity import InteractiveBrowserCredential, verify_token, expand_veracity_scopes
from veracity_platform.service import UserAPI


app = Flask(__name__)
app.secret_key = "mytopsecretkey"  # Used by Flask to secure the session data.

# Initialize server-side session (necessary if the session cookie exceeds 4 KB).
# TODO:  Filesystem session won't work on Azure most likely.
# app.config['SESSION_TYPE'] = 'filesystem'
# sesh = Session(app)

# Parameters from veracity app on developer portal.  Caution! The redirect URI must
# be *exactly* the same as a "Reply URL" in the developer portal, including the port number!
# Veracity  will reject auth requests if the redirect URI does not match a specified reply URL.
CLIENT_ID = os.environ.get("EXAMPLE_VERACITY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("EXAMPLE_VERACITY_CLIENT_SECRET")
SUBSCRIPTION_KEY = os.environ.get("EXAMPLE_VERACITY_SUBSCRIPTION_KEY")
REDIRECT_URI = "http://localhost/login"
SCOPES = ["veracity"]


@app.route("/", methods=["get"])
async def index():
    """ Root endpoint for the application

    Will start authentication flow by redirecting to Veracity IDP if user not
    validated.
    """
    if not validate_user(session):
        print("Not logged in")
        return redirect(url_for("login"))

    access_token = session.get("access_token")
    async with UserAPI(access_token, SUBSCRIPTION_KEY) as user_api:
        profile = await user_api.get_profile()

    return profile


@app.route("/login", methods=["get", "post"])
def login():
    """ Handle user authentication using auth code flow.

    If 'code' is in the query parameters, attempts to acquire a token using
    auth code flow.  If that fails or there is no auth code, then redirects
    to the Veracity login page.  The Veracity login process will redirect back
    to this route (see REDIRECT_URI) with the auth 'code' as a query parameter.
    """

    # We use a interactive-browser credential to authenticate the user.  However, we
    # don't use the credential to generate a token directly, because it uses its
    # own webserver.  As this app handles its own web requests, we will use the
    # credential's service attribute (which is a msal.ConfidentialClientApplication
    # behind the scenes.) to perform auth-code flow.
    credential = InteractiveBrowserCredential(CLIENT_ID, client_secret=CLIENT_SECRET, redirect_uri=REDIRECT_URI)

    if "code" in request.args:
        flow = session.pop("flow", {})
        result = credential.acquire_token_by_auth_code_flow(flow, request.args)
        if "error" not in result:
            session["id_token"] = result.get("id_token")
            session["access_token"] = result.get("access_token")
            return redirect(url_for("index"))

    # No auth code or token acquisition failed.  Redirect to Veracity login.
    session["flow"] = credential.initiate_auth_code_flow(SCOPES)
    response = redirect(session["flow"]["auth_uri"])
    return response


def validate_user(session):
    try:
        token = session.get("id_token")
        jwt_verification = verify_token(token, audience=[CLIENT_ID])
        session["username"] = jwt_verification.get("name")
    except Exception as err:
        print(err)
        return False
    print("User token is valid.")
    return True


if __name__ == "__main__":
    # Ensure port is set exactly as REDIRECT_URI!  Default is 80 for HTTP if no port in REDIRECT_URI.
    app.run(debug=True, host="localhost", port=80)
