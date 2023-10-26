""" Veracity Identity API

It is not possible to use the Microsoft authentication library (msal) or the Azure
identity package (azure.identity) to authenticate with Veracity because they do not
direct to the correct login page.  This is because they lack the policy query parameter.
Instead, this package constructs wraps the lower-level msal.oauth2cli.oidc classes to
correctly authenticate against Veracity.

The classes in this module are designed as drop-in replacements for the msal.ClientApplication
and azure.identity.*Credential classes.

Notes:
    - Microsoft have a work-in-progress code for B2C authorization on GitHub:
      https://github.com/Azure-Samples/ms-identity-python-samples-common.  However,
      this is not yet production-ready.

References:
    - https://developer.veracity.com/services/identity
    - https://developer.veracity.com/docs/section/identity/identity
    - https://docs.microsoft.com/en-us/python/api/msal/msal.application.clientapplication
    - https://github.com/AzureAD/microsoft-authentication-library-for-python
    - https://github.com/veracity/Python-Sample-to-Connect-to-Veracity-Service
    - https://github.com/Azure-Samples/ms-identity-python-webapp
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import AnyStr, Dict, Sequence, Any
from urllib.error import HTTPError
import msal


MICROSOFT_AUTHORITY_HOSTNAME = "https://login.microsoftonline.com"
VERACITY_AUTHORITY_HOSTNAME = "https://login.veracity.com"

DEFAULT_TENANT_ID = "dnvglb2cprod.onmicrosoft.com"
DEFAULT_REPLY_URL = "http://localhost"
DEFAULT_POLICY = "b2c_1a_signinwithadfsidp"

VERACITY_SERVICE_ID = "83054ebf-1d7b-43f5-82ad-b2bde84d7b75"

# The service API scope is sufficient for all Veracity APIs, so don't need the others.  This contradicts the
# [documentation](https://developer.veracity.com/docs/section/identity/authentication/web-native#authenticating-a-user)
# but seems to work.  Veracity scopes require suffixes before use:
#    "/.default" for web app, user not present scenario.  For example:
#       https://dnvglb2cprod.onmicrosoft.com/83054ebf-1d7b-43f5-82ad-b2bde84d7b75/.default
#    "/user_impersonation" for client application, user present scenario.  For example:
#       'https://dnvglb2cprod.onmicrosoft.com/83054ebf-1d7b-43f5-82ad-b2bde84d7b75/user_impersonation

# See https://developer.veracity.com/docs/section/identity/authentication/web-native#authenticating-a-user for API scopes.
SERVICE_API_SCOPE = "https://dnvglb2cprod.onmicrosoft.com/83054ebf-1d7b-43f5-82ad-b2bde84d7b75"
DATAFABRIC_API_SCOPE = "https://dnvglb2cprod.onmicrosoft.com/37c59c8d-cd9d-4cd5-b05a-e67f1650ee14"

# See https://developer.veracity.com/docs/section/onboarding/clientv1 for resource URIs.
SERVICE_RESOURCE = "https://dnvglb2cprod.onmicrosoft.com/dfc0f96d-1c85-4334-a600-703a89a32a4c"
DATAFABRIC_RESOURCE = "https://dnvglb2cprod.onmicrosoft.com/dfba9693-546d-4300-bcd7-d8d525bdff38"

USER_SCOPES = {
    "veracity": f"{SERVICE_API_SCOPE}/user_impersonation",
    "veracity_service": f"{SERVICE_API_SCOPE}/user_impersonation",
    "veracity_datafabric": f"{SERVICE_API_SCOPE}/user_impersonation",
}

CONF_SCOPES = {
    "veracity": f"{SERVICE_RESOURCE}/.default",
    "veracity_service": f"{SERVICE_RESOURCE}/.default",
    "veracity_datafabric": f"{DATAFABRIC_RESOURCE}/.default",
}


def expand_veracity_scopes(scopes: Sequence[AnyStr], interactive: bool = False) -> Sequence[AnyStr]:
    """ Replaces Veracity short-hand scopes for actual scopes, scenario dependent.

    See :const:`USER_SCOPES` and :const:`CONF_SCOPES` for list of short-hand scopes.

    The actual scope depends on the scenario (user present or user not present),
    By default it works for the user not present scenario.

    Args:
        scopes (list): List of scopes, which may include Veracity shorthand scopes.
        interactive (bool): Set True for user-present, i.e. interactive, scenarios.

    Returns:
        A list of scopes with the Veracity scopes replaced with the correct
        scope for this required scenario.
    """
    if scopes is None:
        return None

    if interactive:
        allowed_scopes = USER_SCOPES
    else:
        allowed_scopes = CONF_SCOPES

    return [allowed_scopes.get(s, s) for s in scopes]


def oauth_config_veracity() -> Dict[str, Any]:
    """ Gets the Veracity oauth config from the internet as a dictionary.
    """
    import requests
    url = f"{VERACITY_AUTHORITY_HOSTNAME}/{DEFAULT_TENANT_ID}/v2.0/.well-known/openid-configuration?p={DEFAULT_POLICY}"
    response = requests.get(url)
    if response.status_code != 200:
        raise HTTPError(url, response.status_code, response.text, response.headers, None)
    return response.json()

def oauth_config_microsoft() -> Dict[str, Any]:
    """ Gets the Microsoft oauth config from the internet as a dictionary.
    """
    import requests
    url = f"{MICROSOFT_AUTHORITY_HOSTNAME}/{DEFAULT_TENANT_ID}/.well-known/openid-configuration"
    response = requests.get(url)
    if response.status_code != 200:
        raise HTTPError(url, response.status_code, response.text, response.headers, None)
    return response.json()


def verify_token(token: str, audience: Sequence[str] = None) -> Dict[str, Any]:
    """ Verifies a JWT access token with the Veracity authority.

    Use this method to validate/verify incoming tokens to your application.  Do
    not use it to validate tokens you get from the Veracity authority directly;
    those will be verified by Veracity.

    This method uses the pyjwt package.  It verifies:
        - The token signature (using the Veracity oauth public keys)
        - The audience is correct (if provided)
        - The issuer authority is Veracity.
        - The token is not expired.

    References:
        - https://developer.veracity.com/docs/section/identity/authentication/api#validating-the-access-token
        - https://auth0.com/docs/secure/tokens/access-tokens/validate-access-tokens#json-web-token-jwt-access-tokens
        - https://docs.microsoft.com/en-us/azure/active-directory/develop/access-tokens#validating-the-signature
        - https://jwt.io/
        - https://pyjwt.readthedocs.io/en/stable/api.html#jwt.decode

    Args:
        token: The encoded JWT to verify.
        audience: IDs of the audiences (application IDs) for this token.  By
            default this method does not verify the audience.

    Returns:
        The JWT claims (same as `jwt.decode`).

    Raises:
        Exception if token validation failed.
    """
    import requests
    import jwt

    payload =jwt.decode(token, algorithms="RS256", options={"verify_signature": False})  # Decode to see payload
    isapp = bool(payload.get('appid', False))  # Only applications have 'appid'; users have 'userId'
    
    # Get the oauth config for the Veracity authority.
    if isapp:
        config = oauth_config_microsoft()  # Applications tokens are issued by Microsoft
    else:
        config = oauth_config_veracity()  # User tokens are issued by Veracity

    # Get the Veracity keys.
    keys_url = config["jwks_uri"]
    response = requests.get(keys_url)
    if response.status_code != 200:
        raise HTTPError(keys_url, response.status_code, response.text, response.headers, None)
    key_data = response.json()

    # Get the key used by the token.
    jwk_set = jwt.PyJWKSet(key_data["keys"])
    header = jwt.get_unverified_header(token)
    jwk = next(filter(lambda jwk: jwk.key_id == header["kid"], jwk_set.keys))

    # Verify the token by decoding it.
    options = {"verify_signature": True, "verify_aud": audience is not None}
    return jwt.decode(token, jwk.key, algorithms=["RS256"], options=options, audience=audience, issuer=config["issuer"])


class IdentityError(Exception):
    pass


class Credential(object):
    """ A credential derived from Veracity. Can generate access tokens.

    Args:
        service (IdentityService): Veracity identity service which generated
            this credential.
    """

    def __init__(self, service):
        self.service = service

    @property
    def interactive(self):
        """ Is this credential interactive; does it require user input?
        """
        return isinstance(self.service, msal.PublicClientApplication)

    def get_token(self, scopes: Sequence[AnyStr], **kwargs) -> Dict[AnyStr, AnyStr]:
        raise NotImplementedError("Do not use base class directly.")


class AuthorizationCodeCredential(Credential):
    pass


class InteractiveBrowserCredential(Credential):
    """ Veracity credential using system browser for user interaction.
    This is essentially a copy of the azure.identity.InteractiveBrowserCredential.

    Args:
        client_id (str): Client application ID.
        redirect_uri (str): Redirect URI for interactive flow.  Uses http://localhost
            by default.  For apps running on a user's computer you should only change
            this if you want to use a port other than 80.  Note, on Mac port 80 is
            not open by default; you will have to open it!  Whatever URI you choose
            here, it MUST be specified in the Veracity Developer Portal as your
            app's Reply URL.
        client_secret (str): Optional client secret.
    """

    def __init__(
        self, client_id: AnyStr, redirect_uri: AnyStr = "http://localhost", client_secret: AnyStr = None,
    ):
        if client_secret:
            app = msal.ConfidentialClientApplication(
                client_id=client_id,
                client_credential=client_secret,
                authority=f"{VERACITY_AUTHORITY_HOSTNAME}/{DEFAULT_TENANT_ID}/{DEFAULT_POLICY}",
            )
        else:
            app = msal.PublicClientApplication(
                client_id=client_id,
                client_credential=client_secret,
                authority=f"{VERACITY_AUTHORITY_HOSTNAME}/{DEFAULT_TENANT_ID}/{DEFAULT_POLICY}",
            )

        super().__init__(app)
        self.redirect_uri = redirect_uri

    def get_token(self, scopes: Sequence[AnyStr], timeout: int = 30) -> Dict[AnyStr, AnyStr]:
        """ Get a user token interactively using the webbrowser.

        Internally this uses auth-code-flow to retrieve the token.  It creates a
        local HTTP server to handle the authorization response from Veracity.  For
        this to work on a user's local computer, you should set a Reply URL like
        http://localhost in the Veracity developer portal and provide that to
        the identity service.

        Args:
            scopes (list[str]): List of scopes to retrieve.  Do not include
                'openid', 'profile' or 'offline_access' - these get added
                automatically by the service.
            timeout (int): Time in seconds to wait for user to enter credentials.
        """

        import webbrowser

        # Start an HTTP server to receive the redirect.
        server = self._make_server(self.redirect_uri, timeout=timeout)
        if not server:
            raise IdentityError("Could not start an HTTP server for interactive credential.")

        flow = self.initiate_auth_code_flow(scopes)

        # Open system default browser to auth url.
        auth_url = flow["auth_uri"]
        if not webbrowser.open(auth_url):
            raise IdentityError("Failed to open system web browser for interactive credential.")

        # Block until the server times out or receives the post-authentication redirect.  Then
        # we check the response for possible errors.
        response = server.wait_for_redirect()

        if not response:
            raise IdentityError(f"Timed out after waiting {timeout} seconds for the user to authenticate.")

        if "error" in response:
            err = response.get("error_description") or response["error"]
            raise IdentityError(f"Authentication failed: {err}")

        # Redeem the authorization code for a token.  This handles any errors with
        # malformed responses, so we don't have to.
        token = self.acquire_token_by_auth_code_flow(flow, server.query_params)
        return token

    def _make_server(self, redirect_uri, timeout=30):
        """ Starts an HTTP service on localhost to listen for browser redirects.
        This works the same as azure.identity.InteractiveBrowserCredential.
        """
        server = AuthCodeRedirectServer(redirect_uri, timeout)
        return server

    def initiate_auth_code_flow(self, scopes):
        """ Wrapper for MSAL auth-code flow which interprets Veracity scopes.
        """
        clean_scopes = expand_veracity_scopes(scopes, interactive=True)
        return self.service.initiate_auth_code_flow(clean_scopes, redirect_uri=self.redirect_uri)

    def acquire_token_by_auth_code_flow(self, flow, query_params):
        """ Wrapper for MSAL auth-code flow.
        """
        return self.service.acquire_token_by_auth_code_flow(flow, query_params)


class CertificateCredential(Credential):
    pass


class ChainedTokenCredential(Credential):
    pass


class ClientSecretCredential(Credential):
    """ Authenticates as a service principal (no user present).
    This is a drop-in replacement for azure.identity.ClientSecretCredential.

    Args:
        client_id (str): Client application ID.
        client_secret (str): Client secret (i.e. service principal password.)
            Remember, you should not store secrets in your code!  Use
            environment variables or Azure KeyVault instead.
    """

    def __init__(
        self, client_id: AnyStr, client_secret: AnyStr, resource: AnyStr = None, **kwargs,
    ):
        # If we want to use client/secret auth we need to use the v1 endpoints.
        app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"{MICROSOFT_AUTHORITY_HOSTNAME}/{DEFAULT_TENANT_ID}",
        )
        super().__init__(app)
        self.resource = resource

    def get_token(self, scopes: Sequence[AnyStr], **kwargs) -> Dict[AnyStr, AnyStr]:
        if self.resource is not None:
            # Inject the resource into the token request body.
            kwargs["data"] = {"resource": self.resource}
        clean_scopes = expand_veracity_scopes(scopes, interactive=False)
        return self.service.acquire_token_for_client(clean_scopes, **kwargs)


class EnvironmentCredential(Credential):
    pass


class ManagedIdentityCredential(Credential):
    pass


class SharedTokenCacheCredential(Credential):
    pass


class DeviceCodeCredential(Credential):
    pass


class UsernamePasswordCredential(Credential):
    def __init__(self):
        raise NotImplementedError("Why are you storing user passwords? Use InteractiveBrowserCredential instead!")


class AuthCodeRedirectHandler(BaseHTTPRequestHandler):
    """ HTTP request handler to capture the authentication server's response.

    Copied from Microsoft's azure.identity._internal with a few edits.

    References:
        - https://github.com/Azure/azure-sdk-for-python
        - https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/identity/
            azure-identity/azure/identity/_internal/auth_code_redirect_handler.py
    """

    def do_GET(self):
        from urllib.parse import urlparse, parse_qs

        urlbits = urlparse(self.path)

        # If there are no query parameters, return and wait for the next request.
        if not urlbits.query:
            self.send_response(204)
            return

        # Take only the first of each parameter.
        self.server.query_params = {key: value[0] for key, value in parse_qs(urlbits.query).items()}

        # If there are query params, tell the user we have finished.
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"Veracity authentication complete. You can close this window.")

    def log_message(self, format, *args):
        pass  # this prevents server dumping messages to stdout


class AuthCodeRedirectServer(HTTPServer):
    """HTTP server that listens for the redirect request following an authorization code authentication.

    Copied from Microsoft's azure.identity._internal with a few edits.

    References:
        - https://github.com/Azure/azure-sdk-for-python
        - https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/identity/
            azure-identity/azure/identity/_internal/auth_code_redirect_handler.py
    """

    query_params = {}

    def __init__(self, uri: AnyStr, timeout: int):
        from urllib.parse import urlparse

        urlbits = urlparse(uri)
        hostname = urlbits.hostname
        port = urlbits.port or 80
        super().__init__((hostname, port), AuthCodeRedirectHandler)
        self.timeout = timeout

    def wait_for_redirect(self):
        while not self.query_params:
            try:
                self.handle_request()
            except (IOError, ValueError):
                # Socket has been closed, probably by handle_timeout
                break

        # Ensure the underlying socket is closed (a no-op when the socket is already closed)
        self.server_close()

        # If we timed out, this returns an empty dict
        return self.query_params

    def handle_timeout(self):
        """ Break the request-handling loop by tearing down the server.
        """
        self.server_close()


def get_datafabric_token(client_id: AnyStr, client_secret: AnyStr) -> Dict[AnyStr, AnyStr]:
    """ Quickly get an access token for the Veracity Data Fabric.
    """
    cred = ClientSecretCredential(client_id=client_id, client_secret=client_secret)
    return cred.get_token(scopes=["veracity_datafabric"])
