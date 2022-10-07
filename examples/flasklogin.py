""" Demo Flask web app with Flask-Login for Veracity user management.

This example retrieves a user token using auth-code-flow.  Both parts of the flow
are handled by the /login endpoint.  Note, the app's Reply URL must be set in
the developer portal exactly so in order for Veracity to return a token:

    - http://localhost/login           - when running the app locally.
    - https://<your_host_name>/login   - when deployed online.

Requirements:

    1. Before running, install Flask:

      $ pip install flask flask-session flask-login

"""

import os
from flask import Flask, request, redirect, session, url_for
from flask_login import LoginManager, login_required, login_user, logout_user, current_user

# from flask_session import Session
from veracity_platform.identity import InteractiveBrowserCredential, verify_token

# Parameters from veracity app on developer portal.  Caution! The redirect URI must
# be *exactly* the same as a "Reply URL" in the developer portal, including the port number!
# Veracity  will reject auth requests if the redirect URI does not match a specified reply URL.
CLIENT_ID = os.environ.get("EXAMPLE_VERACITY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("EXAMPLE_VERACITY_CLIENT_SECRET")
SUBSCRIPTION_KEY = os.environ.get("EXAMPLE_VERACITY_SUBSCRIPTION_KEY")
REDIRECT_URI = "http://localhost/login"
SCOPES = ["veracity"]

app = Flask(__name__)
app.secret_key = "mytopsecretkey"  # Used by Flask to secure the session data.

# Initialize server-side session (necessary if the session cookie exceeds 4 KB).
# TODO:  Filesystem session won't work on Azure most likely.
# app.config['SESSION_TYPE'] = 'filesystem'
# sesh = Session(app)

loginmgr = LoginManager()
loginmgr.init_app(app)
loginmgr.login_view = "login"


# We use a interactive-browser credential to authenticate the user.  However, we
# don't use the credential to generate a token directly, because it uses its
# own webserver.  As this app handles its own web requests, we will use the
# credential's service attribute (which is a msal.ConfidentialClientApplication
# behind the scenes.) to perform auth-code flow.
credential = InteractiveBrowserCredential(CLIENT_ID, client_secret=CLIENT_SECRET, redirect_uri=REDIRECT_URI)
# id_service = credential.service


class User:

    known_users = {}

    def __init__(self, user_id, session={}):
        self._id = user_id
        self._session = session
        self._jwt_content = None
        self._name = None
        self._verified = False
        self._verify()
        User.known_users[user_id] = self

    def _verify(self):
        try:
            jwt_content = verify_token(self.id_token)
            self._name = jwt_content.get("name")
            self._verified = True
        except Exception:
            self._verified = False

    def is_authenticated(self):
        return self._verified

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self._id

    @property
    def id_token(self):
        return self._session.get("id_token")

    @property
    def access_token(self):
        return self._session.get("access_token")

    @staticmethod
    def get(userid):
        User.known_users.get(userid)

    @staticmethod
    def from_flow(response):
        """ Creates a user from auth code flow response"""
        userid = response.get("userId")
        user = User(userid, session=response)
        return user

    def get_profile(self):
        """ Queries user profile from Veracity service API.
        """
        import requests

        url = "https://api.veracity.com/veracity/services/v3/my/profile"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY,
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return "Failed to get profile!"


@app.route("/", methods=["get"])
@login_required
def index():
    """ Will start authentication flow by redirecting to Veracity IDP.
    """
    print("GET INDEX PAGE")
    profile = current_user.get_profile()
    return profile


@app.route("/login", methods=["get", "post"])
def login():
    """ Handle user authentication using auth code flow.

    If 'code' is in the query parameters, attempts to acquire a token using
    auth code flow.  If that fails or there is no auth code, then redirects
    to the Veracity login page.  The Veracity login process will redirect back
    to this route (see REDIRECT_URI) with the auth 'code' as a query parameter.
    """
    print("START LOGIN")
    print(request.args)
    if "code" in request.args:
        print("HANDLE RETURN FLOW")
        flow = session.pop("flow", {})
        result = credential.acquire_token_by_auth_code_flow(flow, request.args)
        if "error" not in result:
            user = User.from_flow(result)
            print("NEW", user)
            print(User.known_users)
            login_user(user)
            print("CURRENT", current_user)
            return redirect(session.pop("next"))

    print("START AUTH FLOW")
    # No auth code or token acquisition failed.  Redirect to Veracity login.
    session["flow"] = credential.initiate_auth_code_flow(SCOPES)
    session["next"] = request.args.get("next", url_for("index"))
    response = redirect(session["flow"]["auth_uri"])
    return response


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return "Logged out"


@loginmgr.user_loader
def load_user(userid):
    print("HELLO", userid)
    return User.get(userid)


if __name__ == "__main__":
    # Ensure port is set exactly as REDIRECT_URI!  Default is 80 for HTTP if no port in REDIRECT_URI.
    app.run(debug=True, host="localhost", port=80)
