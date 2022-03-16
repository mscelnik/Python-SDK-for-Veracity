""" Unit tests for the service API /my endpoints.
"""

import pytest
from veracity_platform import service
from veracity_platform import identity


@pytest.mark.interactive
@pytest.mark.requires_secrets
class TestUserAPI(object):
    @pytest.fixture(scope="class")
    def credential(self, CLIENT_ID, CLIENT_SECRET):
        yield identity.InteractiveBrowserCredential(
            CLIENT_ID, client_secret=CLIENT_SECRET
        )

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
