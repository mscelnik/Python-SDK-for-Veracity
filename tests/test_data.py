""" Unit tests for the data fabric API.
"""

from contextlib import contextmanager
from unittest import mock
import aiohttp
import pandas as pd
import pandas.testing as pdt
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


# @pytest.mark.requires_secrets
# @pytest.mark.requires_datafabric
class TestDataFabricAPI(object):
    @pytest.fixture(scope="function")
    async def api(self, credential):
        # Mock out the aiohttp session for unit testing.  This prevents any real
        # web calls.
        with mock.patch("veracity_platform.base.ClientSession", autospec=True):
            api = data.DataFabricAPI(credential, "key")
            await api.connect()
            yield api

    # APPLICATIONS.

    @pytest.mark.asyncio
    async def test_get_application(self, api):
        """ Get [current] application has no exceptions.

        Checks two methods:

            - get_current_application
            - get_application
        """
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_current_application()
            mockget.assert_called_with("https://api.veracity.com/veracity/datafabric/data/api/1/application")
            assert data == {"id": 0}

            data = await api.get_application("0")
            mockget.assert_called_with("https://api.veracity.com/veracity/datafabric/data/api/1/application/0")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_get_application_404(self, api):
        """ Get [current] application raises exception upon HTTP/404.
        """
        with patch_response(api.session, "get", 404):
            with pytest.raises(data.DataFabricError):
                await api.get_current_application()

            with pytest.raises(data.DataFabricError):
                await api.get_application("1")

    @pytest.mark.asyncio
    async def test_add_application_200(self, api):
        """ Get application has no exceptions.
        """
        with patch_response(api.session, "post", 200, json={"id": 0}) as mockpost:
            await api.add_application("1", "2", "role")
            mockpost.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/application",
                {"id": "1", "companyId": "2", "role": "role"},
            )

    @pytest.mark.asyncio
    async def test_add_application_409(self, api):
        """ Get application raises exception upon HTTP/409.
        """
        with patch_response(api.session, "post", 409):
            with pytest.raises(data.DataFabricError):
                await api.add_application("1", "2", "role")

    @pytest.mark.asyncio
    async def test_update_application_role(self, api):
        """ Update application role has no exceptions.
        """
        response = {"id": 0}
        with patch_response(api.session, "get", 200, json=response) as mockget:
            data = await api.update_application_role("myapp", "myrole")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/application/myapp?role=myrole"
            )
            assert data == response

    # GROUPS.

    @pytest.mark.asyncio
    async def test_get_groups(self, api):
        """ Get groups has no exceptions.
        """
        with patch_response(api.session, "get", 200, json={"id": 0}) as mockget:
            data = await api.get_groups()
            mockget.assert_called_with("https://api.veracity.com/veracity/datafabric/data/api/1/groups")
            assert data == {"id": 0}

    @pytest.mark.asyncio
    async def test_add_group(self, api):
        """ Add group has no exceptions.
        """
        payload = {
            "title": "mygroup",
            "description": "my description",
            "resourceIds": ["0"],
            "sortingOrder": 0.0,
        }
        expected = payload.copy()
        expected["id"] = "00000000-0000-0000-0000-000000000000"
        with patch_response(api.session, "post", 201, json=expected) as mockpost:
            actual = await api.add_group("mygroup", "my description", ["0"])
            mockpost.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/groups", payload,
            )
            assert expected == actual

    @pytest.mark.asyncio
    async def test_get_group(self, api):
        """ Get group has no exceptions.
        """
        expected = {
            "id": "1",
            "title": "mygroup",
            "description": "my description",
            "resourceIds": ["0"],
            "sortingOrder": 0.0,
        }
        with patch_response(api.session, "get", 200, json=expected) as mockget:
            actual = await api.get_group("1")
            mockget.assert_called_with("https://api.veracity.com/veracity/datafabric/data/api/1/groups/1")
            assert expected == actual

    @pytest.mark.asyncio
    async def test_get_group_404(self, api):
        """ Get group raises exception upon HTTP/404.
        """
        with patch_response(api.session, "get", 404):
            with pytest.raises(data.DataFabricError):
                await api.get_group("0")

    @pytest.mark.asyncio
    async def test_update_group_200(self, api):
        """ Update group has no exceptions.
        """
        payload = {
            "title": "mygroup",
            "description": "my description",
            "resourceIds": ["0"],
            "sortingOrder": 0.0,
        }
        with patch_response(api.session, "put", 200) as mockput:
            await api.update_group(0, "mygroup", "my description", ["0"])
            mockput.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/groups/0", payload,
            )

    @pytest.mark.asyncio
    async def test_delete_group_204(self, api):
        """ Delete group has no exceptions.
        """
        with patch_response(api.session, "delete", 204) as mockdelete:
            await api.delete_group("1")
            mockdelete.assert_called_with("https://api.veracity.com/veracity/datafabric/data/api/1/groups/1")

    # KEY TEMPLATES.

    @pytest.mark.asyncio
    async def test_get_keytemplates(self, api):
        """ Get key templates has no exceptions.
        """
        keys = [
            {
                "id": "00000000-0000-0000-0000-000000000000",
                "name": "mykey",
                "totalHours": 0,
                "isSystemKey": True,
                "description": "My key template",
                "attribute1": True,
                "attribute2": True,
                "attribute3": False,
                "attribute4": False,
            }
        ]

        keysdf = pd.DataFrame(
            columns=[
                "id",
                "name",
                "totalHours",
                "isSystemKey",
                "description",
                "attribute1",
                "attribute2",
                "attribute3",
                "attribute4",
            ],
            data=[
                ["00000000-0000-0000-0000-000000000000", "mykey", 0, True, "My key template", True, True, False, False,]
            ],
        )

        with patch_response(api.session, "get", 200, json=keys) as mockget:
            data = await api.get_keytemplates()
            mockget.assert_called_with("https://api.veracity.com/veracity/datafabric/data/api/1/keytemplates")
            assert data == keys

            data = await api.get_keytemplates_df()
            mockget.assert_called_with("https://api.veracity.com/veracity/datafabric/data/api/1/keytemplates")
            pdt.assert_frame_equal(keysdf, data)

    # LEDGER - NO LONGER AVAILABLE.

    @pytest.mark.skip("Ledger has been discontinued")
    @pytest.mark.asyncio
    async def test_ledger(self, api):
        """ Get ledger from a demo container.
        """
        # data = await api.get_ledger(CONTAINER_ID)
        # assert data is not None
        pass

    # RESOURCES.

    @pytest.mark.asyncio
    async def test_get_resources(self, api):
        """ Get resources has no exceptions.
        """
        response = [
            {
                "id": "00000000-0000-0000-0000-000000000000",
                "reference": "ref",
                "url": "http://uri",
                "lastModifiedUTC": "2020-01-01",
                "creationDateTimeUTC": "2020-01-01",
                "ownerId": "00000000-0000-0000-0000-000000000000",
                "accessLevel": "owner",
                "region": "2020-01-01",
                "keyStatus": "noKeys",
                "mayContainPersonalData": "unknown",
                "metadata": {
                    "title": "title",
                    "description": "description",
                    "icon": {"id": "0", "backgroundColor": "red"},
                    "tags": [{"id": "00000000-0000-0000-0000-000000000000", "title": "title"}],
                },
            }
        ]
        with patch_response(api.session, "get", 200, json=response) as mockget:
            data = await api.get_resources()
            mockget.assert_called_with("https://api.veracity.com/veracity/datafabric/data/api/1/resources")
            assert data == response

    @pytest.mark.asyncio
    async def test_get_resource(self, api):
        """ Get resource has no exceptions.
        """
        response = {
            "id": "00000000-0000-0000-0000-000000000000",
            "reference": "ref",
            "url": "http://uri",
            "lastModifiedUTC": "2020-01-01",
            "creationDateTimeUTC": "2020-01-01",
            "ownerId": "00000000-0000-0000-0000-000000000000",
            "accessLevel": "owner",
            "region": "2020-01-01",
            "keyStatus": "noKeys",
            "mayContainPersonalData": "unknown",
            "metadata": {
                "title": "title",
                "description": "description",
                "icon": {"id": "0", "backgroundColor": "red"},
                "tags": [{"id": "00000000-0000-0000-0000-000000000000", "title": "title"}],
            },
        }
        with patch_response(api.session, "get", 200, json=response) as mockget:
            data = await api.get_resource("mycontainer")
            mockget.assert_called_with("https://api.veracity.com/veracity/datafabric/data/api/1/resources/mycontainer")
            assert data == response

    # ACCESSES.

    @pytest.mark.asyncio
    async def test_get_accesses(self, api):
        """ Get all access shares for a demo container.
        """
        response = {
            "results": [
                {
                    "userId": "00000000-0000-0000-0000-000000000000",
                    "ownerId": "00000000-0000-0000-0000-000000000000",
                    "grantedById": "00000000-0000-0000-0000-000000000000",
                    "accessSharingId": "00000000-0000-0000-0000-000000000000",
                    "keyCreated": True,
                    "autoRefreshed": True,
                    "keyCreatedTimeUTC": "string",
                    "keyExpiryTimeUTC": "string",
                    "resourceType": "string",
                    "accessHours": 0,
                    "accessKeyTemplateId": "00000000-0000-0000-0000-000000000000",
                    "attribute1": True,
                    "attribute2": True,
                    "attribute3": True,
                    "attribute4": True,
                    "resourceId": "00000000-0000-0000-0000-000000000000",
                    "ipRange": {"startIp": "string", "endIp": "string"},
                    "comment": "string",
                }
            ],
            "page": 0,
            "resultsPerPage": 0,
            "totalPages": 0,
            "totalResults": 0,
        }
        with patch_response(api.session, "get", 200, json=response) as mockget:
            result = await api.get_accesses("1")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/resources/1/accesses",
                params={"pageNo": 1, "pageSize": 50},
            )
            assert result == response

            result = await api.get_accesses("1", 2)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/resources/1/accesses",
                params={"pageNo": 2, "pageSize": 50},
            )

            result = await api.get_accesses("1", 2, 100)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/resources/1/accesses",
                params={"pageNo": 2, "pageSize": 100},
            )

            result = await api.get_accesses("1", pageSize=10)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/resources/1/accesses",
                params={"pageNo": 1, "pageSize": 10},
            )

    @pytest.mark.asyncio
    async def test_get_accesses_df_nodata(self, api):
        """ Returns empty dataframe ok if no accesses.
        """
        response = {
            "results": [],
            "page": 0,
            "resultsPerPage": 0,
            "totalPages": 0,
            "totalResults": 0,
        }

        expected = pd.DataFrame(
            columns=[
                "userId",
                "ownerId",
                "grantedById",
                "accessSharingId",
                "keyCreated",
                "autoRefreshed",
                "keyCreatedTimeUTC",
                "keyExpiryTimeUTC",
                "resourceType",
                "accessHours",
                "accessKeyTemplateId",
                "attribute1",
                "attribute2",
                "attribute3",
                "attribute4",
                "resourceId",
                "startIp",
                "endIp",
                "comment",
                "level",
            ]
        )
        with patch_response(api.session, "get", 200, json=response) as mockget:
            result = await api.get_accesses_df("1")
            print(result)
            assert result is not None
            pdt.assert_frame_equal(expected, result, check_dtype=False)

    @pytest.mark.asyncio
    async def test_get_best_access(self, api):
        """ Get an access share ID for a demo container.
        Note, we cannot test precisely the access because it depends on the
        test environment.
        """
        from datetime import datetime, timedelta, timezone

        # First ensure there is a SAS in the cache.
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        tomorrow = datetime.now(timezone.utc) + timedelta(days=1)

        me = {"type": "user", "id": "0"}
        other_person = {"type": "user", "id": "NOTME"}
        nobody = {"type": "user", "id": "NOBODY"}

        accesses = pd.DataFrame(
            columns=["userId", "level", "keyExpiryTimeUTC", "autoRefreshed"],
            data=[
                ["0", 1, yesterday, True],
                ["NOTME", 2, tomorrow, True],
                ["0", 4, tomorrow, False],
                ["NOTME", 8, tomorrow, False],
            ],
        )

        with mock.patch.object(api, "whoami", return_value=me) as mock_whoami, mock.patch.object(
            api, "get_accesses_df", return_value=accesses
        ):
            mock_whoami.return_value = me
            data = await api.get_best_access("ContainerID")
            pdt.assert_series_equal(data, accesses.loc[2])

            mock_whoami.return_value = other_person
            data = await api.get_best_access("ContainerID")
            pdt.assert_series_equal(data, accesses.loc[3])

            mock_whoami.return_value = nobody
            data = await api.get_best_access("ContainerID")
            assert data is None

    @pytest.mark.asyncio
    async def test_share_access_200(self, api):
        response = {"accessSharingId": "00000000-0000-0000-0000-000000000000"}
        with patch_response(api.session, "post", 200, json=response) as mockpost:
            data = await api.share_access("0", "1", "2", autoRefreshed=True)
            mockpost.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/resources/0/accesses",
                json={"userId": "1", "accessKeyTemplateId": "2"},
                params={"autoRefreshed": "true"},
            )
            assert data == "00000000-0000-0000-0000-000000000000"

    @pytest.mark.asyncio
    async def test_revoke_access_200(self, api):
        with patch_response(api.session, "put", 200) as mockput:
            await api.revoke_access("0", "1")
            mockput.assert_called_with("https://api.veracity.com/veracity/datafabric/data/api/1/resources/0/accesses/1")

    @pytest.mark.asyncio
    async def test_sas_new(self, api):
        """ Get new SAS key given an access ID.
        """
        response = {
            "sasKey": "key",
            "sasuRi": "http://uri",
            "fullKey": "string",
            "sasKeyExpiryTimeUTC": "2020-01-01",
            "isKeyExpired": True,
            "autoRefreshed": True,
            "ipRange": {"startIp": "000.000.000.000", "endIp": "000.000.000.001"},
        }
        with patch_response(api.session, "put", 200, json=response) as mockput:
            sas = await api.get_sas_new("0", "1")
            mockput.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/resources/0/accesses/1/key"
            )

            expected = response.copy()
            expected["accessId"] = "1"
            assert sas == expected

    def test_sas_cached(self, api):
        """ Get new SAS key for a demo container.
        """
        from datetime import datetime, timedelta, timezone

        # First ensure there is a SAS in the cache.
        tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
        mock_cache = {"MyContainer": {"sasKeyExpiryTimeUTC": tomorrow.isoformat(), "isKeyExpired": False}}
        with mock.patch.object(api, "sas_cache", new=mock_cache):
            sas = api.get_sas_cached("MyContainer")
            assert sas == mock_cache["MyContainer"]

    def test_access_levels(self, api):
        import pandas as pd
        import pandas.testing as pdt

        accesses = pd.DataFrame(
            columns=["attribute1", "attribute2", "attribute3", "attribute4"],
            data=[
                [False, True, False, False],  # Write.
                [True, False, False, True],  # Read and list.
                [True, True, False, True],  # Read, write and list.
                [True, True, True, True],  # Read, write, list and delete.
                [False, False, False, True],  # List.
            ],
        )
        expected = pd.Series([1, 6, 7, 15, 2], dtype="Int64")
        levels = api._access_levels(accesses)
        pdt.assert_series_equal(expected, levels)

    # DATA STEWARDS.

    @pytest.mark.asyncio
    async def test_get_data_stewards(self, api):
        expected = [{"userId": "0", "resourceId": "1", "grantedBy": "2", "comment": "my comment",}]
        with patch_response(api.session, "get", 200, json=expected) as mockget:
            data = await api.get_data_stewards("1")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/resources/1/datastewards"
            )
            assert expected == data

    @pytest.mark.asyncio
    async def test_get_data_stewards_df(self, api):
        import pandas as pd

        response = [{"userId": "0", "resourceId": "1", "grantedBy": "2", "comment": "my comment"}]
        expected = pd.DataFrame(
            columns=["userId", "resourceId", "grantedBy", "comment"], data=[["0", "1", "2", "my comment"]],
        )
        with patch_response(api.session, "get", 200, json=response) as mockget:
            data = await api.get_data_stewards_df("1")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/resources/1/datastewards"
            )
            assert expected == data

    @pytest.mark.asyncio
    async def test_delegate_data_steward(self, api):
        expected = {
            "userId": "0",
            "resourceId": "1",
            "grantedBy": "2",
            "comment": "my comment",
        }
        with patch_response(api.session, "post", 200, json=expected) as mockpost:
            data = await api.delegate_data_steward(1, 0, "my comment")
            mockpost.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/resources/1/datastewards/0",
                {"comment": "my comment"},
            )
            assert expected == data

    @pytest.mark.asyncio
    async def test_delete_data_steward_200(self, api):
        with patch_response(api.session, "delete", 200) as mockdelete:
            await api.delete_data_steward(1, 0)
            mockdelete.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/resources/1/datastewards/0"
            )

    @pytest.mark.asyncio
    async def test_delete_data_steward_40x(self, api):
        """ Delete data steward raises exception upon HTTP/403 or HTTP/404.
        """
        with patch_response(api.session, "delete", 403):
            with pytest.raises(data.DataFabricError):
                await api.delete_data_steward(1, 0)

        with patch_response(api.session, "delete", 404):
            with pytest.raises(data.DataFabricError):
                await api.delete_data_steward(1, 0)

    @pytest.mark.asyncio
    async def test_transfer_ownership(self, api):
        response = {}
        with patch_response(api.session, "put", 200, json=response) as mockput:
            await api.transfer_ownership("1", "0", True)
            mockput.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/resources/1/owner",
                params={"userId": "0", "keepAccessAsDataSteward": "true"},
            )

    # TAGS.

    @pytest.mark.asyncio
    async def test_get_tags(self, api):
        expected = [{"id": "0", "title": "title"}]
        with patch_response(api.session, "get", 200, json=expected) as mockget:
            data = await api.get_tags()
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/tags",
                params={"includeDeleted": False, "includeNonVeracityApproved": False},
            )
            assert data == expected

            data = await api.get_tags(True)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/tags",
                params={"includeDeleted": True, "includeNonVeracityApproved": False},
            )

            data = await api.get_tags(True, True)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/tags",
                params={"includeDeleted": True, "includeNonVeracityApproved": True},
            )

            data = await api.get_tags(includeNonVeracityApproved=True)
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/tags",
                params={"includeDeleted": False, "includeNonVeracityApproved": True},
            )

    @pytest.mark.asyncio
    async def test_add_tags(self, api):
        response = [{"id": "0", "title": "mytag"}]
        with patch_response(api.session, "post", 200, json=response) as mockpost:
            result = await api.add_tags(["mytag"])
            mockpost.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/tags", [{"title": "mytag"}],
            )
            assert result == response

    # USERS.

    @pytest.mark.asyncio
    async def test_get_shared_users_200(self, api):
        response = [{"userId": "00000000-0000-0000-0000-000000000000"}]
        with patch_response(api.session, "get", 200, json=response) as mockget:
            data = await api.get_shared_users("1")
            mockget.assert_called_with(
                "https://api.veracity.com/veracity/datafabric/data/api/1/users/ResourceDistributionList?userId=1"
            )
            assert data == response

    @pytest.mark.asyncio
    async def test_get_shared_users_403(self, api):
        """ Get shared users raises exception upon HTTP/403.
        """
        with patch_response(api.session, "get", 403):
            with pytest.raises(data.DataFabricError):
                await api.get_shared_users("1")

    @pytest.mark.asyncio
    async def test_get_current_user(self, api):
        response = [{"userId": "00000000-0000-0000-0000-000000000000"}]
        with patch_response(api.session, "get", 200, json=response) as mockget:
            data = await api.get_current_user()
            assert data == response

    @pytest.mark.asyncio
    async def test_get_user(self, api):
        response = [{"userId": "0", "companyId": "1", "role": "role"}]

        with patch_response(api.session, "get", 200, json=response) as mockget:
            data = await api.get_user("0")
            mockget.assert_called_with("https://api.veracity.com/veracity/datafabric/data/api/1/users/0")
            assert data == response

    @pytest.mark.asyncio
    async def test_get_user_404(self, api):
        """ Get user raises exception upon HTTP/404.
        """
        with patch_response(api.session, "get", 404):
            with pytest.raises(data.DataFabricError):
                await api.get_user("0")

    @pytest.mark.asyncio
    async def test_whoami_user(self, api):
        me = {"userId": "0"}
        app = {"id": "1"}

        with mock.patch.object(api, "get_current_user", return_value=me), mock.patch.object(
            api, "get_current_application", return_value=app
        ):
            result = await api.whoami()
            assert result == {"type": "user", "id": "0"}

            with mock.patch.object(api, "get_current_user", side_effect=data.HTTPError("", "", "", {}, None)):
                result = await api.whoami()
                assert result == {"type": "application", "id": "1"}

    # CONTAINERS.

    @pytest.mark.asyncio
    async def test_get_container(self, api):
        sas = {"fullKey": "mysaskey"}
        with mock.patch.object(api, "get_sas", return_value=sas), mock.patch(
            "veracity_platform.data.ContainerClient"
        ) as mock_ContainerClient:
            data = await api.get_container("MyContainer")

            mock_from_container_url = mock_ContainerClient.from_container_url
            mock_from_container_url.assert_called_with("mysaskey")
            assert data == mock_from_container_url.return_value
