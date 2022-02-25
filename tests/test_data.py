""" Integration (API call) tests for the data fabric API.

Prerequisites:

    - Registered client credential app at developer.veracity.com.
    - Access to a test data container in the Veracity data fabric.
    - Set up an Azure key vault or environment variables (see README).
"""

import pytest
from veracity_platform import data


@pytest.fixture(scope='module')
def credential(CLIENT_ID, CLIENT_SECRET, RESOURCE_URL):
    from veracity_platform import identity
    yield identity.ClientSecretCredential(CLIENT_ID, CLIENT_SECRET, resource=RESOURCE_URL)


@pytest.mark.requires_secrets
@pytest.mark.requires_datafabric
class TestDataFabricAPI(object):

    @pytest.fixture()
    async def api(self, credential, SUBSCRIPTION_KEY):
        try:
            api = data.DataFabricAPI(credential, SUBSCRIPTION_KEY)
            await api.connect()
            yield api
        finally:
            await api.disconnect()

    @pytest.mark.asyncio
    async def test_connect(self, credential, SUBSCRIPTION_KEY):
        api = data.DataFabricAPI(credential, SUBSCRIPTION_KEY)
        assert api is not None
        try:
            await api.connect()
        finally:
            await api.disconnect()

    # APPLICATIONS.

    @pytest.mark.asyncio
    async def test_get_application(self, api):
        """ Get [current] application has no exceptions.

        Checks two methods:

            - get_current_application
            - get_application
        """
        data = await api.get_current_application()
        assert data is not None
        assert 'error' not in data
        assert 'id' in data

        data = await api.get_application(data['id'])
        assert data is not None

    @pytest.mark.asyncio
    async def test_add_application(self, api):
        """ Get application has no exceptions.
        """
        await api.add_application()

    @pytest.mark.asyncio
    async def test_update_application_role(self, api):
        """ Update application role has no exceptions.
        """
        await api.update_application_role()

    # GROUPS.

    @pytest.mark.asyncio
    async def test_get_groups(self, api):
        """ Get groups has no exceptions.
        """
        await api.get_groups()

    @pytest.mark.asyncio
    async def test_add_group(self, api):
        """ Add group has no exceptions.
        """
        await api.add_group()

    @pytest.mark.asyncio
    async def test_get_group(self, api):
        """ Get group has no exceptions.
        """
        await api.get_group()

    @pytest.mark.asyncio
    async def test_update_group(self, api):
        """ Update group has no exceptions.
        """
        await api.update_group()

    @pytest.mark.asyncio
    async def test_delete_group(self, api):
        """ Delete group has no exceptions.
        """
        await api.delete_group()

    # KEY TEMPLATES.

    @pytest.mark.asyncio
    async def test_get_keytemplates(self, api):
        """ Get keytemplates has no exceptions.
        """
        data = await api.get_keytemplates()
        assert data is not None

    # LEDGER - NO LONGER AVAILABLE.

    @pytest.mark.skip('Ledger has been discontinued')
    @pytest.mark.asyncio
    async def test_ledger(self, api, CONTAINER_ID):
        """ Get ledger from a demo container.
        """
        data = await api.get_ledger(CONTAINER_ID)
        assert data is not None

    # RESOURCES.

    @pytest.mark.asyncio
    async def test_get_resources(self, api):
        """ Get resources has no exceptions.
        """
        data = await api.get_resources()
        assert data is not None

    @pytest.mark.asyncio
    async def test_get_resource(self, api):
        """ Get resource has no exceptions.
        """
        data = await api.get_resource()
        assert data is not None

    # ACCESSES.

    @pytest.mark.asyncio
    async def test_get_accesses(self, api, CONTAINER_ID):
        """ Get all access shares for a demo container.
        """
        data = await api.get_accesses(CONTAINER_ID)
        assert data is not None
        assert 'results' in data
        assert 'page' in data and data['page'] == 1

    @pytest.mark.asyncio
    async def test_get_best_access(self, api, CONTAINER_ID):
        """ Get an access share ID for a demo container.
        Note, we cannot test precisely the access because it depends on the
        test environment.
        """
        data = await api.get_best_access(CONTAINER_ID)
        assert data is not None

    @pytest.mark.asyncio
    async def test_sas_new(self, api, CONTAINER_ID):
        """ Get new SAS key for a demo container.
        """
        sas = await api.get_sas_new(CONTAINER_ID)
        assert sas is not None

    @pytest.mark.asyncio
    async def test_sas_cached(self, api, CONTAINER_ID):
        """ Get new SAS key for a demo container.
        """
        # First ensure there is a SAS in the cache.
        sasnew = await api.get_sas_new(CONTAINER_ID)
        sas = api.get_sas_cached(CONTAINER_ID)
        assert sas == sasnew

    def test_access_level(self, api):
        import pandas as pd
        import pandas.testing as pdt
        accesses = pd.DataFrame(
            columns=['attribute1', 'attribute2', 'attribute3', 'attribute4'],
            data=[
                [False, True, False, False],  # Write.
                [True, False, False, True],  # Read and list.
                [True, True, False, True],  # Read, write and list.
                [True, True, True, True],  # Read, write, list and delete.
                [False, False, False, True],  # List.
            ]
        )
        expected = pd.Series([1, 6, 7, 15, 2], dtype='Int64')
        levels = api._access_levels(accesses)
        pdt.assert_series_equal(expected, levels)

    # DATA STEWARDS.

    @pytest.mark.asyncio
    async def test_get_data_stewards(self, api):
        data = await api.get_data_stewards()
        assert data is not None

    @pytest.mark.asyncio
    async def test_get_data_stewards_df(self, api):
        data = await api.get_data_stewards_df()
        assert data is not None

    @pytest.mark.asyncio
    async def test_delegate_data_steward(self, api):
        data = await api.delegate_data_steward()
        assert data is not None

    @pytest.mark.asyncio
    async def test_delete_data_steward(self, api):
        data = await api.delete_data_steward()
        assert data is not None

    @pytest.mark.asyncio
    async def test_transfer_ownership(self, api):
        data = await api.transfer_ownership()
        assert data is not None

    # TAGS.

    @pytest.mark.asyncio
    async def test_get_tags(self, api):
        data = await api.get_tags()
        assert data is not None

    @pytest.mark.asyncio
    async def test_add_tags(self, api):
        data = await api.add_tags()
        assert data is not None

    # USERS.

    @pytest.mark.asyncio
    async def test_get_shared_users(self, api):
        data = await api.get_shared_users()
        assert data is not None

    @pytest.mark.asyncio
    async def test_get_current_user(self, api):
        data = await api.get_current_user()
        assert data is not None

    @pytest.mark.asyncio
    async def test_get_user(self, api):
        data = await api.get_user()
        assert data is not None

    # CONTAINERS.

    @pytest.mark.asyncio
    async def test_get_container(self, api):
        data = await api.get_container()
        assert data is not None
