""" Unit tests for the service API /my endpoints.
"""

import pytest
from veracity_platform import service


@pytest.fixture(scope='module')
def credential(CLIENT_ID, CLIENT_SECRET, RESOURCE_URL):
    from veracity_platform import identity
    yield identity.ClientSecretCredential(CLIENT_ID, CLIENT_SECRET, resource=RESOURCE_URL)


@pytest.mark.requires_secrets
class TestUserAPI(object):

    @pytest.fixture()
    async def api(self, credential, SUBSCRIPTION_KEY):
        try:
            api = service.UserAPI(credential, SUBSCRIPTION_KEY)
            await api.connect()
            yield api
        finally:
            await api.disconnect()

    @pytest.mark.asyncio
    async def test_connect(self, credential, SUBSCRIPTION_KEY):
        api = service.UserAPI(credential, SUBSCRIPTION_KEY)
        assert api is not None
        try:
            await api.connect()
        finally:
            await api.disconnect()

    @pytest.mark.asyncio
    async def test_get_companies(self, api):
        data = await api.get_companies()
        assert data is not None
        print(data)
        assert False
