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
        """Creating a new container has no exceptions."""
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

    @pytest.mark.asyncio
    async def test_copy_container(self, api):
        """Copying a container has no exceptions."""
        with patch_response(api.session, "post", 202, text="") as mockpost:
            await api.copy_container(
                "mycontainer",
                "myaccess",
                "newcontainer",
                "New Container",
                description="My copied container",
                tags=["my", "container"],
            )

            expected_body = {
                "sourceResourceId": "mycontainer",
                "copyResourceShortName": "newcontainer",
                "copyResourceMayContainPersonalData": False,
                "copyResourceTitle": "New Container",
                "copyResourceDescription": "My copied container",
                "copyResourceIcon": {"id": "Automatic_Information_Display", "backgroundColor": "#5594aa"},
                "copyResourceTags": [{"title": "my", "type": "tag"}, {"title": "container", "type": "tag"}],
            }
            mockpost.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/provisioning/api/1/container/copycontainer",
                json=expected_body,
                params={"accessId": "myaccess"},
            )

    @pytest.mark.asyncio
    async def test_delete_container(self, api):
        """Deleting a container has no exceptions."""
        with patch_response(api.session, "delete", 202, text="") as mockdelete:
            await api.delete_container("mycontainer")
            mockdelete.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/provisioning/api/1/container/mycontainer"
            )

    @pytest.mark.asyncio
    async def test_create_event_subscription(self, api):
        """Creating a new event subscription has no exceptions."""
        with patch_response(api.session, "post", 202, text="") as mockpost:
            await api.create_event_subscription(
                "mysub",
                "AccessShare",
                "https://mycallback",
            )

            expected_body = {
                "subscriptionName": "mysub",
                "callback": "https://mycallback",
                "topic": "AccessShare",
                "regions": ["westeurope"],
            }
            mockpost.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/provisioning/api/1/container/SubscribeToCustomEvents",
                json=expected_body,
            )

    @pytest.mark.asyncio
    async def test_delete_event_subscription(self, api):
        """Deleting a subscription has no exceptions."""
        with patch_response(api.session, "delete", 202, text="") as mockdelete:
            await api.delete_event_subscription("mysub")
            mockdelete.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/provisioning/api/1/container/SubscribeToCustomEvents",
                json={"subscriptionName": "mysub"},
            )

    @pytest.mark.asyncio
    async def test_create_blob_change_subscription(self, api):
        """Creating a new event subscription has no exceptions."""
        with patch_response(api.session, "post", 202, text="") as mockpost:
            await api.create_blob_change_subscription(
                "mysub",
                "mycontainer",
                ["BlobUpserted"],
                "https://mycallback",
            )

            expected_body = {
                "subscriptionName": "mysub",
                "containerId": "mycontainer",
                "subscriptionTypes": ["BlobUpserted"],
                "callback": "https://mycallback",
            }
            mockpost.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/provisioning/api/1/container/SubscribeToBlobContainerEvents",
                json=expected_body,
            )

    @pytest.mark.asyncio
    async def test_delete_blob_change_subscription(self, api):
        """Deleting a subscription has no exceptions."""
        with patch_response(api.session, "delete", 202, text="") as mockdelete:
            await api.delete_blob_change_subscription("mysub", "mycontainer")
            mockdelete.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/provisioning/api/1/container/SubscribeToBlobContainerEvents",
                json={"subscriptionName": "mysub", "containerId": "mycontainer"},
            )
