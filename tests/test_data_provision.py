""" Unit tests for the data fabric provisioning API.
"""

from contextlib import contextmanager
from unittest import mock
import aiohttp
import pytest
from veracity_platform import data, identity


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


class TestProvisionAPI(object):
    @pytest.fixture(scope="function")
    async def api(self, credential):
        # Mock out the aiohttp session for unit testing.  This prevents any real
        # web calls.
        with mock.patch("veracity_platform.base.ClientSession", autospec=True):
            api = data.ProvisionAPI(credential, "key")
            await api.connect()
            yield api

    @pytest.fixture(scope="function")
    async def api_context(self, credential):
        # Mock out the aiohttp session for unit testing.  This prevents any real
        # web calls.
        # This version used the API as a context manager.
        with mock.patch("veracity_platform.base.ClientSession", autospec=True):
            async with data.ProvisionAPI(credential, "key") as api:
                yield api

    # CONTAINERS.

    @pytest.mark.asyncio
    async def test_create_container(self, api):
        """ Creating a new container has no exceptions.
        """
        with patch_response(api.session, "post", 202, text="MOCK_GUID") as mockpost:
            data = await api.create_container(
                "mycontainer",
                "My Container",
                description="My new container",
                region="westeurope",
                tags=["my", "container"],
            )

            expected_body = {
                "storageLocation": "westeurope",
                "containerShortName": "mycontainer",
                "mayContainPersonalData": False,
                "title": "My Container",
                "description": "My new container",
                "icon": {"id": "Automatic_Information_Display", "backgroundColor": "#5594aa"},
                "tags": [{"title": "my", "type": "tag"}, {"title": "container", "type": "tag"}],
            }
            mockpost.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/provisioning/api/1/container", json=expected_body
            )
            assert data == "MOCK_GUID"
