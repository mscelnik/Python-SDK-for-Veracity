""" Unit tests for the Veracity service APIs for apps.
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

    @pytest.mark.skip("Not implemented")
    @pytest.mark.asyncio
    async def test_get_services(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_services(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.skip("Not implemented")
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

    @pytest.mark.skip("Not implemented")
    @pytest.mark.asyncio
    async def test_get_subscribers(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_subscribers(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.skip("Not implemented")
    @pytest.mark.asyncio
    async def test_get_subscriber(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_subscriber("0")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.skip("Not implemented")
    @pytest.mark.asyncio
    async def test_add_subscriber(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.add_subscriber("0", "a")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.skip("Not implemented")
    @pytest.mark.asyncio
    async def test_remove_subscriber(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.remove_subscriber("0")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    # USER DIRECTORY.

    @pytest.mark.skip("Not implemented")
    @pytest.mark.asyncio
    async def test_create_user(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.create_user(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.skip("Not implemented")
    @pytest.mark.asyncio
    async def test_create_users(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.create_users(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_resolve_user(self, api):
        """ Get user by email address has no exceptions.
        """
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.resolve_user("a@a.com")
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/this/user/resolve(a@a.com)")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_resolve_user_404(self, api):
        """ Get user by email address returns None if invalid email.
        """
        with patch_response(api.session, "get", 404, json={"id": 0}) as mockget:
            data = await api.resolve_user("a@a.com")
            mockget.assert_called_with("https://api.veracity.com/veracity/services/v3/this/user/resolve(a@a.com)")
            assert data is None

    @pytest.mark.asyncio
    async def test_resolve_user_500(self, api):
        """ Get [current] application raises exception upon HTTP error other than 404.
        """
        with patch_response(api.session, "get", 500):
            with pytest.raises(HTTPError):
                await api.resolve_user("a@a.com")

    @pytest.mark.skip("Not implemented")
    @pytest.mark.asyncio
    async def test_get_user_picture(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_user_picture(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.skip("Not implemented")
    @pytest.mark.asyncio
    async def test_notify_users(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.notify_users(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.skip("Not implemented")
    @pytest.mark.asyncio
    async def test_verify_policy(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.verify_policy(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/this/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}
