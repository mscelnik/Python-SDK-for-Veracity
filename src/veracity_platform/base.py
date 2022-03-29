""" Base components for the Veracity SDK.
"""

from typing import AnyStr, Dict, List
from aiohttp import ClientSession
from . import identity


class ApiBase(object):
    """ Base for API access classes. Provides connection/disconnection.

    All web calls are async using an aiohttp.ClientSession object.

    Arguments:
        credential (veracity.Credential): Provides oauth access tokens for the
            API (the user has to log in to retrieve these unless your client
            application has permissions to use the service.)
        subscription_key (str): Your application's API subscription key.  Gets
            sent in th Ocp-Apim-Subscription-Key header.
        scope (str): A valid scope for a Veracity API.  Only one permitted.  See
            `identity.ALLOWED_SCOPES` for options.
    """

    def __init__(
        self,
        credential: identity.Credential,
        subscription_key: AnyStr,
        scope: List[AnyStr],
    ):
        self.credential = credential
        self.subscription_key = subscription_key
        # By default we ask for access permission the service and data fabric APIs.
        self.scopes = [scope]
        # Use this session for all HTTP requests.  We also add authentication
        # headers to all requests by default, so the child API services do not
        # need to.
        self._session = None
        self._headers = {}

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.disconnect()

    @property
    def connected(self) -> bool:
        return self._session is not None

    @property
    def session(self) -> ClientSession:
        if self._session is None:
            raise RuntimeError("Must connect API before use.")
        return self._session

    @property
    def default_headers(self) -> Dict[AnyStr, AnyStr]:
        return self._headers

    async def connect(
        self,
        reset: bool = False,
        credential: identity.Credential = None,
        key: AnyStr = None,
    ) -> ClientSession:
        """ Create a single HTTP session to call the API.
        Optionally reset the existing session or change the credentials.

        Args:
            reset (bool): Set True to force HTTP session to reconnect.
            credential (veracity.Credential): Provides oauth access tokens for the
                API (the user has to log in to retrieve these unless your client
                application has permissions to use the service.)
            subscription_key (str): Your application's API subscription key.  Gets
                sent in th Ocp-Apim-Subscription-Key header.
        """
        # Use this session for all HTTP requests.  We also add authentication
        # headers to all requests; which we attempt to set now.
        reset_headers = reset or (self._session is None)

        if credential is not None:
            self.credential = credential
            reset_headers = True

        if key is not None:
            self.subscription_key = key
            reset_headers = True

        if reset_headers:
            token = self.credential.get_token(self.scopes)
            if "error" in token:
                raise RuntimeError(f"Failed to get token:\n{token}")
            assert (
                "access_token" in token
            ), "Token does not provide API access privileges for requested scopes."
            actual_token = token["access_token"]
            self._headers = {
                "Ocp-Apim-Subscription-Key": self.subscription_key,
                "Authorization": f"Bearer {actual_token}",
            }

        if reset:
            # This sets _session to None.
            await self.disconnect()

        if self._session is None:
            self._session = ClientSession(headers=self._headers)

        return self._session

    async def disconnect(self):
        """ Disconnects the HTTP session. Not essential but good practice.
        """
        from asyncio import shield

        if self._session is not None:
            await shield(self._session.connector.close())
            await shield(self._session.close())
            self._session = None
