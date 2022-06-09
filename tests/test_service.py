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


class TestClientAPI(object):
    @pytest.fixture(scope="function")
    async def api(self, credential):
        # Mock out the aiohttp session for unit testing.  This prevents any real
        # web calls.
        with mock.patch("veracity_platform.base.ClientSession", autospec=True):
            api = service.ClientAPI(credential, "key")
            await api.connect()
            yield api

    # SERVICES.

    @pytest.mark.asyncio
    async def test_get_services(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_services(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_post_notification(self, api):
        from datetime import datetime

        timestamp = datetime(2022, 6, 8)
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.post_notification("a", "b", "c", timestamp, ["a"], "0")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_subscribers(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_subscribers(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_subscriber(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_subscriber("0")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_add_subscriber(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.add_subscriber("0", "a")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_remove_subscriber(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.remove_subscriber("0")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    # USER DIRECTORY.

    @pytest.mark.asyncio
    async def test_create_user(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.create_user(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_user_from_email(self, api):
        """ Get user by email address has no exceptions.
        """
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_user_from_email("a@a.com")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/users/by/email", params={"email": "a@a.com"}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_user_from_email_404(self, api):
        """ Get user by email address returns None if invalid email.
        """
        with patch_response(api.session, "get", 404, json={"id": 0}) as mockget:
            data = await api.get_user_from_email("a@a.com")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/users/by/email", params={"email": "a@a.com"}
            )
            assert data is None

    @pytest.mark.asyncio
    async def test_get_user_from_email_500(self, api):
        """ Get user by email address raises exception upon HTTP error other than 404.
        """
        with patch_response(api.session, "get", 500):
            with pytest.raises(HTTPError):
                await api.get_user_from_email("a@a.com")

    @pytest.mark.asyncio
    async def test_resolve_user(self, api):
        """ Get user by email address has no exceptions.
        """
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.resolve_user("a@a.com")
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/user/resolve(a@a.com)")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_resolve_user_404(self, api):
        """ Get user by email address returns None if invalid email.
        """
        with patch_response(api.session, "get", 404, json={"id": 0}) as mockget:
            data = await api.resolve_user("a@a.com")
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/user/resolve(a@a.com)")
            assert data is None

    @pytest.mark.asyncio
    async def test_resolve_user_500(self, api):
        """ Get [current] application raises exception upon HTTP error other than 404.
        """
        with patch_response(api.session, "get", 500):
            with pytest.raises(HTTPError):
                await api.resolve_user("a@a.com")


class TestUserAPI(object):
    @pytest.fixture(scope="function")
    async def api(self, credential):
        # Mock out the aiohttp session for unit testing.  This prevents any real
        # web calls.
        with mock.patch("veracity_platform.base.ClientSession", autospec=True):
            api = service.UserAPI(credential, "key")
            await api.connect()
            yield api

    @pytest.mark.asyncio
    async def test_get_companies(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_companies()
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/my/companies")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_messages(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_messages()
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/my/messages")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_message_count(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_message_count()
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/my/messages")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_message(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_message(0)
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/my/messages/0")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_validate_policies(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.validate_policies()
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/my/policies/validate()")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_validate_service_policy(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.validate_service_policy("0")
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/my/policy/0/validate()")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_profile(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_profile(0)
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/my/profile")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_services(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_services()
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/my/services")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_widgets(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_widgets()
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/my/widgets")
            assert data == {"id": 0}
