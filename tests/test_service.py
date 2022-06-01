""" Unit tests for the Veracity service APIs.
"""

from contextlib import contextmanager
from urllib.error import HTTPError
from unittest import mock
import aiohttp
import pytest
from veracity_platform import service, identity


@contextmanager
def patch_response(session, method, status=200, text=b"", json=None):
    mockresponse = mock.AsyncMock(spec=aiohttp.ClientResponse)
    mockresponse.json.return_value = json
    mockresponse.text.return_value = text
    mockresponse.status = status
    with mock.patch.object(session, method, new=mock.AsyncMock(return_value=mockresponse)) as mockhttp:
        yield mockhttp


@pytest.fixture(scope="module")
def credential():
    mockcred = mock.MagicMock(spec=identity.Credential)
    mockcred.get_token.return_value = {"access_token": "MOCK_TOKEN"}
    yield mockcred


# @pytest.mark.requires_secrets
# @pytest.mark.requires_datafabric
class TestClientAPI(object):
    @pytest.fixture(scope="function")
    async def api(self, credential):
        # Mock out the aiohttp session for unit testing.  This prevents any real
        # web calls.
        with mock.patch("veracity_platform.base.ClientSession", autospec=True):
            api = service.ClientAPI(credential, "key")
            await api.connect()
            yield api

    # USER DIRECTORY.

    @pytest.mark.asyncio
    async def get_user_from_email(self, api):
        """ Get user by email address has no exceptions.
        """
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_user_from_email("a@a.com")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/users/by/email", params={"email": "a@a.com"}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def get_user_from_email_404(self, api):
        """ Get user by email address returns None if invalid email.
        """
        with patch_response(api.session, "get", 404, json={"id": 0}) as mockget:
            data = await api.get_user_from_email("a@a.com")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/users/by/email", params={"email": "a@a.com"}
            )
            assert data is None

    @pytest.mark.asyncio
    async def test_get_application_500(self, api):
        """ Get [current] application raises exception upon HTTP error other than 404.
        """
        with patch_response(api.session, "get", 500):
            with pytest.raises(HTTPError):
                await api.get_user_from_email("a@a.com")

    @pytest.mark.asyncio
    async def resolve_user(self, api):
        """ Get user by email address has no exceptions.
        """
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.resolve_user("a@a.com")
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/user/resolve(a@a.com)")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def resolve_user_404(self, api):
        """ Get user by email address returns None if invalid email.
        """
        with patch_response(api.session, "get", 404, json={"id": 0}) as mockget:
            data = await api.resolve_user("a@a.com")
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/user/resolve(a@a.com)")
            assert data is None

    @pytest.mark.asyncio
    async def get_user_from_email_500(self, api):
        """ Get [current] application raises exception upon HTTP error other than 404.
        """
        with patch_response(api.session, "get", 500):
            with pytest.raises(HTTPError):
                await api.resolve_user("a@a.com")
