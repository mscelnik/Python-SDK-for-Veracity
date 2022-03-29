""" Unit tests for shared components.
"""

from unittest import mock
import pytest
from veracity_platform import base, identity


@pytest.fixture(scope="module")
def credential():
    mockcred = mock.MagicMock(spec=identity.Credential)
    mockcred.get_token.return_value = {"access_token": "MOCK_TOKEN"}
    yield mockcred


class TestApiBase(object):
    @pytest.mark.asyncio
    async def test_connect(self, credential):
        api = base.ApiBase(credential, "key", scope="veracity_service")
        assert api is not None
        try:
            await api.connect()
            print(api._headers)
        finally:
            await api.disconnect()
