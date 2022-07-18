""" Unit tests for the Veracity directory API.
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


class TestDirectoryAPI(object):
    @pytest.fixture(scope="function")
    async def api(self, credential):
        # Mock out the aiohttp session for unit testing.  This prevents any real
        # web calls.
        with mock.patch("veracity_platform.base.ClientSession", autospec=True):
            api = service.DirectoryAPI(credential, "key")
            await api.connect()
            yield api

    # COMPANY DIRECTORY.

    @pytest.mark.asyncio
    async def test_get_company(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_company(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_company_users(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_company_users(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    # SERVICE DIRECTORY.

    @pytest.mark.asyncio
    async def test_get_service(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_service(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_service_users(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_service_users(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_is_service_admin(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.is_service_admin(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_service_status(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_service_status(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_data_containers(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.data_containers(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_create_data_container_reference(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.create_data_container_reference(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_delete_data_container_reference(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.delete_data_container_reference(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    # USER DIRECTORY.

    @pytest.mark.asyncio
    async def test_accept_terms(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.accept_terms(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_activate_account(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.activate_account(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_delete_user(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.delete_user(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_exchange_otp_code(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.exchange_otp_code(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_pending_activation(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_pending_activation(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_user(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_user(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_users(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_users(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_user_companies(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_user_companies(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_user_resync(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_user_resync(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_user_from_email(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_user_from_email(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_user_services(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_user_services(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_user_subscription(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_user_subscription(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_update_current_user(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.update_current_user(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_change_current_user_email(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.change_current_user_email(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_change_current_user_phone(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.change_current_user_phone(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_change_current_user_password(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.change_current_user_password(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_validate_current_user_email(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.validate_current_user_email(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_validate_current_user_phone(self, api):
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.validate_current_user_phone(1)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/services/v3/directory/", params={"page": 1, "pageSize": 10}
            )
            assert data == {"id": 0}
