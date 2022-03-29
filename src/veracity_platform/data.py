""" Veracity Data Fabric API
"""


from typing import Any, AnyStr, List, Mapping, Sequence, Dict
from urllib.error import HTTPError
import pandas as pd
from azure.storage.blob.aio import ContainerClient
from .base import ApiBase
from . import identity


class DataFabricError(RuntimeError):
    pass


class DataFabricAPI(ApiBase):
    """Access to the data fabric endpoints (/datafabric) in the Veracity API.


    All web calls are async using aiohttp.  Returns web responses exactly as
    received, usually JSON.

    Arguments:
        credential (identity.Credential): Provides oauth access tokens for the
            API (the user has to log in to retrieve these unless your client
            application has permissions to use the service.)
        subscription_key (str): Your application's API subscription key.  Gets
            sent in th Ocp-Apim-Subscription-Key header.
        version (str): Not currently used.
    """

    API_ROOT = "https://api.veracity.com/veracity/datafabric"

    def __init__(
        self, credential: identity.Credential, subscription_key: AnyStr, version: AnyStr = None, **kwargs,
    ):
        super().__init__(
            credential, subscription_key, scope=kwargs.pop("scope", "veracity_datafabric"), **kwargs,
        )
        self._url = f"{DataFabricAPI.API_ROOT}/data/api/1"
        self.sas_cache = {}
        self.access_cache = {}

    @property
    def url(self) -> str:
        return self._url

    # APPLICATIONS.

    async def get_current_application(self) -> Dict[str, str]:
        """Gets information about the current application.

        Reference:
            https://api-portal.veracity.com/docs/services/data-api/operations/v1-0Application_Me

        Returns:
            Dictionary with application info like:

            .. code-block:: json

               {
                 "id": "00000000-0000-0000-0000-000000000000",
                 "companyId": "00000000-0000-0000-0000-000000000000",
                 "role": "string"
               }
        """
        url = f"{self._url}/application"
        resp = await self.session.get(url)
        data = await resp.json()
        if resp.status == 200:
            return data
        elif resp.status == 404:
            raise DataFabricError("Current application does not existing in the Data Fabric.")
        else:
            raise HTTPError(url, resp.status, data, resp.headers, None)

    async def get_application(self, applicationId: str) -> Dict[str, str]:
        """Gets information about an application in Veracity data fabric.

        Reference:
            https://api-portal.veracity.com/docs/services/data-api/operations/v1-0Application_Get

        Args:
            applicationId: The application ID.

        Returns:
            Dictionary with application info like:

            .. code-block:: json

               {
                 "id": "00000000-0000-0000-0000-000000000000",
                 "companyId": "00000000-0000-0000-0000-000000000000",
                 "role": "string"
               }
        """
        url = f"{self._url}/application/{applicationId}"
        resp = await self.session.get(url)
        data = await resp.json()
        if resp.status == 200:
            return data
        elif resp.status == 404:
            raise DataFabricError(f"Application {applicationId} does not existing in the Data Fabric.")
        else:
            raise HTTPError(url, resp.status, data, resp.headers, None)

    async def add_application(self, applicationId: str, companyId: str, role: str):
        """Adds a new application to the Data Fabric.

        Args:
            applicationId: GUID of the application to add to the Data Fabric.
            companyId: GUID of the company associated with the application.
            role: TODO - add role description.

        Warning:
            The user/service principal must have the "DataAdmin" role to perform
            this action.

        Reference:
            https://api-portal.veracity.com/docs/services/data-api/operations/v1-0Application_Create
        """
        url = f"{self._url}/application"
        body = {
            "id": applicationId,
            "companyId": companyId,
            "role": role,
        }
        resp = await self.session.post(url, body)
        if resp.status != 200:
            if resp.status == 409:
                raise DataFabricError(
                    f"HTTP/409 Application with ID {applicationId} already exists in the Data Fabric."
                )
            else:
                data = await resp.json()
                raise HTTPError(url, resp.status, data, resp.headers, None)

    async def update_application_role(self, applicationId, role):
        url = f"{self._url}/application/{applicationId}?role={role}"
        resp = await self.session.get(url)
        data = await resp.json()
        if resp.status != 200:
            raise HTTPError(url, resp.status, data, resp.headers, None)
        return data

    # GROUPS.

    async def get_groups(self) -> Dict[str, Any]:
        """Get user's container groups.

        The Data Fabric uses groups to organize containers in the web portal.  This
        is mostly for convenience and is on a per-user basis.  Users cannot share
        groups.

        Reference:
            https://api-portal.veracity.com/docs/services/data-api/operations/v1-0Groups_Get
        """
        url = f"{self._url}/groups"
        resp = await self.session.get(url)
        data = await resp.json()
        if resp.status != 200:
            raise HTTPError(url, resp.status, data, resp.headers, None)
        return data

    async def add_group(
        self, title: str, description: str, containerIds: Sequence[str], sortingOrder: float = 0.0,
    ) -> Dict[str, Any]:
        """Creates a new container group for the user.

        Reference:
            https://api-portal.veracity.com/docs/services/data-api/operations/v1-0Groups_Post

        Args:
            title: Group name for display in Data Fabric portal.
            description: Describe the group.
            containerIds: List of container GUIDs to put in the group.
            sortingOrder: TODO - describe sorting order.

        Returns:
            Dictionary with the group GUID and properties like:

            .. code-block:: json

               {
                    "id": "00000000-0000-0000-0000-000000000000",
                    "title": "string",
                    "description": "string",
                    "resourceIds": [
                        "00000000-0000-0000-0000-000000000000"
                    ],
                    "sortingOrder": 0.0
                }
        """
        url = f"{self._url}/groups"
        body = {
            "title": title,
            "description": description,
            "resourceIds": list(containerIds),
            "sortingOrder": sortingOrder,
        }
        resp = await self.session.post(url, body)
        data = await resp.json()
        if resp.status != 201:
            raise HTTPError(url, resp.status, data, resp.headers, None)
        return data

    async def get_group(self, groupId: str) -> Dict[str, Any]:
        """Gets information about a single group.

        Reference:
            https://api-portal.veracity.com/docs/services/data-api/operations/v1-0Groups_GetById

        Args:
            groupId: The group ID (must exist for the current user).

        Returns:
            Group information as a dictionary like:

            .. code-block:: json

               {
                   "id": "00000000-0000-0000-0000-000000000000",
                   "title": "string",
                   "description": "string",
                   "resourceIds": [
                       "00000000-0000-0000-0000-000000000000"
                   ],
                   "sortingOrder": 0.0
               }
        """
        url = f"{self._url}/groups/{groupId}"
        resp = await self.session.get(url)
        data = await resp.json()
        if resp.status == 200:
            return data
        elif resp.status == 404:
            raise DataFabricError(f"Group {groupId} does not exist for current user.")
        else:
            raise HTTPError(url, resp.status, data, resp.headers, None)

    async def update_group(self, groupId, *args, **kwargs):
        raise NotImplementedError()

    async def delete_group(self, groupId):
        raise NotImplementedError()

    # KEY TEMPLATES.

    async def get_keytemplates(self):
        """Gets key templates the current credential can generate.

        A key template denotes an access level (read, write, list, delete) and
        an expiry interval.  Typically you get the key templates prior to sharing
        container access with another user or application.  You should choose
        the lowest privileges required for that user to perform their required
        actions.  For example, do not give read privileges if they only need to
        write.

        Returns:
            Upon success, a list of key templates (each a dictionary).

        Exceptions:
            Raises HTTPError if not a 200 response.
        """
        url = f"{self._url}/keytemplates"
        resp = await self.session.get(url)
        data = await resp.json()
        if resp.status != 200:
            raise HTTPError(url, resp.status, data, resp.headers, None)
        return data

    async def get_keytemplates_df(self):
        """Gets key templates the current credential can generate as a Pandas dataframe.

        A key template denotes an access level (read, write, list, delete) and
        an expiry interval.  Typically you get the key templates prior to sharing
        container access with another user or application.  You should choose
        the lowest privileges required for that user to perform their required
        actions.  For example, do not give read privileges if they only need to
        write.

        Returns:
            Pandas dataframe with the key templates

        Exceptions:
            Raises HTTPError if not a 200 response.
        """
        data = await self.get_keytemplates()
        return pd.DataFrame(data)

    # LEDGER.

    async def get_ledger(self, containerId: AnyStr) -> pd.DataFrame:
        """DO NOT USE.  Veracity has removed the ledger feature."""
        raise NotImplementedError("The Veracity Data Fabric ledger has been discontinued.")

    # RESOURCES.

    async def get_resources(self) -> List[Dict[str, Any]]:
        """Gets metadata for all containers for which you can claim keys.

        Returns:
            Upon success, a list of container metadata (each a dictionary) like:

            .. code-block:: json

                [
                    {
                        "id": "00000000-0000-0000-0000-000000000000",
                        "reference": "string",
                        "url": "string",
                        "lastModifiedUTC": "string",
                        "creationDateTimeUTC": "string",
                        "ownerId": "00000000-0000-0000-0000-000000000000",
                        "accessLevel": "owner",
                        "region": "string",
                        "keyStatus": "noKeys",
                        "mayContainPersonalData": "unknown",
                        "metadata": {
                        "title": "string",
                        "description": "string",
                        "icon": {
                            "id": "string",
                            "backgroundColor": "string"
                        },
                        "tags": [
                            {
                            "id": "00000000-0000-0000-0000-000000000000",
                            "title": "string"
                            }
                        ]
                        }
                    }
                ]

        Raises:
            HTTPError for any response except 200.
        """
        url = f"{self._url}/resources"
        resp = await self.session.get(url)
        if resp.status != 200:
            raise HTTPError(url, resp.status, await resp.text(), resp.headers, None)
        data = await resp.json()
        return data

    async def get_resource(self, containerId: AnyStr):
        """Gets metadata for a single container.

        Args:
            containerId: Container ID.

        Returns:
            Upon success (HTTP/200), a dictionary of container metadata.

        Raises:
            DataFabricError for HTTP 403 or 404 errors.
            HTTPError for any other HTTP error code.
        """
        url = f"{self._url}/resources/{containerId}"
        resp = await self.session.get(url)
        data = await resp.json()
        if resp.status == 200:
            return data
        elif resp.status == 403:
            raise DataFabricError(
                f"HTTP/403 You do not have permission to view container {containerId}. Details:\n{data}"
            )
        elif resp.status == 404:
            raise DataFabricError(f"HTTP/404 Data Fabric container {containerId} does not exist. Details:\n{data}")
        else:
            raise HTTPError(url, resp.status, await resp.text(), resp.headers, None)

    # ACCESS.

    async def get_best_access(self, containerId: AnyStr) -> pd.Series:
        """Gets the best available access share ID for a Veracity container.
        Returns the access share ID with the highest available privileges.

        Args:
            containerId: Container ID.

        Returns:
            Pandas Series object with the best access specification if the user
            has access, otherwise None.
        """
        from datetime import datetime, timezone
        import pandas as pd

        me = await self.whoami()
        all_accesses = await self.get_accesses_df(containerId, pageSize=-1)
        expiry = pd.to_datetime(all_accesses["keyExpiryTimeUTC"])

        # Remove keys which are expired and cannot be refreshed.
        mask = all_accesses["autoRefreshed"] | (expiry >= datetime.now(timezone.utc))

        # Accesses only for the current user/application.
        mask = mask & (all_accesses["userId"] == me["id"])

        my_accesses = all_accesses[mask]

        if len(my_accesses) == 0:
            # TODO: Is this the best thing to do?  Raise exception instead?
            # User/application does not have permission to access the container.
            return None

        best_index = my_accesses["level"].astype(float).idxmax()
        return my_accesses.loc[best_index]

    async def get_accesses(self, containerId: AnyStr, pageNo: int = 1, pageSize: int = 50) -> Mapping[AnyStr, Any]:
        """Gets list of all available access specifications to a container.

        Reference:
            https://api-portal.veracity.com/docs/services/data-api/operations/v1-0Access_Get

        Args:
            containerId: The ID of the container.
            pageNo: For multi-page access lists, get this page number (starting at 1).
            pageSize: Number of accesses per page.

        Returns:
            A dictionary containing a list of access specification results like:

            .. code-block:: json

                {
                    "results": [
                        {
                        "userId": "00000000-0000-0000-0000-000000000000",
                        "ownerId": "00000000-0000-0000-0000-000000000000",
                        "grantedById": "00000000-0000-0000-0000-000000000000",
                        "accessSharingId": "00000000-0000-0000-0000-000000000000",
                        "keyCreated": true,
                        "autoRefreshed": true,
                        "keyCreatedTimeUTC": "string",
                        "keyExpiryTimeUTC": "string",
                        "resourceType": "string",
                        "accessHours": 0,
                        "accessKeyTemplateId": "00000000-0000-0000-0000-000000000000",
                        "attribute1": true,
                        "attribute2": true,
                        "attribute3": true,
                        "attribute4": true,
                        "resourceId": "00000000-0000-0000-0000-000000000000",
                        "ipRange": {
                            "startIp": "string",
                            "endIp": "string"
                        },
                        "comment": "string"
                        }
                    ],
                    "page": 0,
                    "resultsPerPage": 0,
                    "totalPages": 0,
                    "totalResults": 0
                }
        """
        url = f"{self._url}/resources/{containerId}/accesses"
        params = {"pageNo": pageNo, "pageSize": pageSize}
        resp = await self.session.get(url, params=params)
        if resp.status != 200:
            raise HTTPError(url, resp.status, await resp.text(), resp.headers, None)
        data = await resp.json()
        return data

    async def get_accesses_df(self, resourceId: AnyStr, pageNo: int = 1, pageSize: int = 50) -> pd.DataFrame:
        """Gets the access levels as a dataframe, including the "level" value."""
        import pandas as pd

        data = await self.get_accesses(resourceId, pageNo, pageSize)
        df = pd.DataFrame(data["results"])
        # Add the level values for future use.
        df["level"] = self._access_levels(df)
        self.access_cache[resourceId] = df
        return df

    async def share_access(
        self,
        containerId: AnyStr,
        userId: AnyStr,
        accessKeyTemplateId: AnyStr,
        autoRefreshed: bool = False,
        comment: AnyStr = None,
        startIp: AnyStr = None,
        endIp: AnyStr = None,
        *args,
        **kwargs,
    ):
        """Shares container access with a user/application.

        Args:
            containerId: Container ID to which to share access.
            autoRefreshed: Auto-renew keys when they expire?
            userId: ID of the user/application with which to share access.
            accessKeyTemplateId: Access level template (e.g read, write etc.)
                Get valid key templates using :meth:`get_keytemplates` method

        Returns:
            The accessSharingId (str) if successful.

        Exceptions:
            Raises DataFabric error for known errors.
            Raises HTTPError for unknown errors.
        """
        url = f"{self._url}/resources/{containerId}/accesses"

        # Build data payload.
        payload = {
            "userId": userId,
            "accessKeyTemplateId": accessKeyTemplateId,
        }
        if comment:
            payload["comment"] = comment
        if startIp and endIp:
            payload["ipRange"] = {"startIp": startIp, "endIp": endIp}

        resp = await self.session.post(url, json=payload, params={"autoRefreshed": str(autoRefreshed).lower()})
        data = await resp.json()

        if resp.status == 200:
            return data["accessSharingId"]
        elif resp.status == 400:
            raise DataFabricError(f"HTTP/400 Malformed payload to share container access. Details:\n{data}")
        elif resp.status == 404:
            raise DataFabricError(f"HTTP/404 Data Fabric container {containerId} does not exist. Details:\n{data}")
        else:
            raise HTTPError(url, resp.status, data, resp.headers, None)

    async def revoke_access(self, resourceId: AnyStr, accessId: AnyStr):
        raise NotImplementedError()

    async def get_sas(self, resourceId: AnyStr, accessId: AnyStr = None, **kwargs) -> pd.DataFrame:
        key = self.get_sas_cached(resourceId) or await self.get_sas_new(resourceId, accessId, **kwargs)
        return key

    async def get_sas_new(self, resourceId: AnyStr, accessId: AnyStr = None) -> Dict[str, Any]:
        """Gets a new SAS key to access a container.

        You can request a key with a specific access level (if you have the
        accessId).  By default this method will attempt to get the most
        permissive access level available for the active credential.

        Args:
            resourceId (str): The container GUID.
            accessId (str): Access level GUID, optional.

        Returns:
            Dictionary with SAS key details like:

            .. code-block:: json

                {
                    "sasKey": "string",
                    "sasuRi": "string",
                    "fullKey": "string",
                    "sasKeyExpiryTimeUTC": "string",
                    "isKeyExpired": true,
                    "autoRefreshed": true,
                    "ipRange": {
                        "startIp": "string",
                        "endIp": "string"
                    }
                }
        """
        if accessId is not None:
            access_id = accessId
        else:
            access = await self.get_best_access(resourceId)
            access_id = access.get("accessSharingId")

        assert access_id is not None, "Could not find access rights for current user."
        url = f"{self._url}/resources/{resourceId}/accesses/{access_id}/key"
        resp = await self.session.put(url)
        data = await resp.json()
        if resp.status != 200:
            raise HTTPError(url, resp.status, data, resp.headers, None)
        # The API response does not include the access ID; we add for future use.
        data["accessId"] = access_id
        self.sas_cache[resourceId] = data
        return data

    def get_sas_cached(self, resourceId: AnyStr) -> pd.DataFrame:
        from datetime import datetime, timezone
        import dateutil

        sas = self.sas_cache.get(resourceId)
        if not sas:
            return None
        expiry = dateutil.parser.isoparse(sas["sasKeyExpiryTimeUTC"])
        if (not sas["isKeyExpired"]) and (datetime.now(timezone.utc) < expiry):
            return sas
        else:
            # Remove the expired key from the cache.
            self.sas_cache.pop(resourceId)
            return None

    def _access_levels(self, accesses: pd.DataFrame) -> pd.Series:
        """Calculates an access "level" for each access in a dataframe.
        In general higher access level means more privileges.

        Notes:
            Attributes related to permissions in this way:

                | Attribute  | Permission | Score |
                | ---------  | ---------- | ----- |
                | attribute1 | Read       |   4   |
                | attribute2 | Write      |   1   |
                | attribute3 | Delete     |   8   |
                | attribute4 | List       |   2   |

            Scores are additive, so "read, write & list" = 7.  If you want to
            check an access has delete privileges, use level >= 8.

            Write is considered the lowest privilege as it does not allow data to
            be seen.

        Args:
            accesses (pandas.DataFrame): Accesses as returned by :meth:`get_accesses`.

        Returns:
            Pandas Series with same index as input.
        """
        import numpy as np

        scores = np.array([4, 1, 8, 2])
        attrs = accesses[["attribute1", "attribute2", "attribute3", "attribute4"]].to_numpy()
        levels = (attrs * scores).sum(axis=1)
        return pd.Series(levels, index=accesses.index, dtype="Int64")

    # DATA STEWARDS.

    async def get_data_stewards(self, containerId: AnyStr) -> List[Dict[str, str]]:
        """Gets a list of data stewards on a container.

        Reference:
            https://api-portal.veracity.com/docs/services/data-api/operations/v1-0DataStewards_GetDataStewardsByResourceId

        Args:
            containerId: The ID of the container.

        Returns:
            A list of data stewards, each a dictionary like:

            .. code-block:: json
                [
                {
                    "userId": "00000000-0000-0000-0000-000000000000",
                    "resourceId": "00000000-0000-0000-0000-000000000000",
                    "grantedBy": "00000000-0000-0000-0000-000000000000",
                    "comment": "string"
                }
                ]

        """
        url = f"{self._url}/resources/{containerId}/datastewards"
        resp = await self.session.get(url)
        data = await resp.json()
        if resp.status == 200:
            return data
        elif resp.status == 404:
            raise DataFabricError(f"Container {containerId} does not exist.")
        else:
            raise HTTPError(url, resp.status, data, resp.headers, None)

    async def get_data_stewards_df(self, resourceId: AnyStr) -> pd.DataFrame:
        raise NotImplementedError()

    async def delegate_data_steward(self, containerId: AnyStr, userId: AnyStr, comment: str) -> Dict[str, str]:
        """Delegates rights to the underlying Azure resource to a new data steward.

        Reference:
            https://api-portal.veracity.com/docs/services/data-api/operations/v1-0DataStewards_Post

        Args:
            containerId: GUID of the container.
            userId: GUID of the user to become a data steward.
            comment: Comment on the delegation.

        Returns:
            Dictionary with confirmatory details of the delegation like:

            .. code-block:: json

                {
                    "userId": "00000000-0000-0000-0000-000000000000",
                    "resourceId": "00000000-0000-0000-0000-000000000000",
                    "grantedBy": "00000000-0000-0000-0000-000000000000",
                    "comment": "string"
                }
        """
        url = f"{self._url}/resources/{containerId}/datastewards/{userId}"
        body = {"comment": comment}
        resp = await self.session.post(url, body)
        data = await resp.json()
        if resp.status != 200:
            raise HTTPError(url, resp.status, data, resp.headers, None)
        return data

    async def delete_data_steward(self, containerId: AnyStr, userId: AnyStr):
        """Removes a user as a container data steward."""
        url = f"{self._url}/resources/{containerId}/datastewards/{userId}"
        resp = await self.session.delete(url)
        if resp.status != 200:
            data = await resp.json()
            if resp.status == 403:
                raise DataFabricError(
                    f"HTTP/403 You do not have permission to delete data stewards on container {containerId}."
                )
            elif resp.status == 404:
                raise DataFabricError(
                    f"HTTP/404 Container {containerId} does not exist or user {userId} is not a data steward."
                )
            else:
                raise HTTPError(url, resp.status, data, resp.headers, None)

    async def transfer_ownership(self, resourceId: AnyStr, userId: AnyStr, keepaccess: bool = False):
        raise NotImplementedError()

    # TAGS.

    async def get_tags(self, includeDeleted: bool = False, includeNonVeracityApproved: bool = False) -> Sequence:
        """Gets metadata tags.

        Args:
            includeDeleted: Also get deleted tags (requires data admin privileges.)
            includeNonVeracityApproved: Also get get not approved by Veracity.

        Returns:
            List of tags like:

            .. code-block:: json

                [
                {
                    "id": "00000000-0000-0000-0000-000000000000",
                    "title": "string"
                }
                ]
        """
        params = {
            "includeDeleted": includeDeleted,
            "includeNonVeracityApproved": includeNonVeracityApproved,
        }
        url = f"{self._url}/tags"
        resp = await self.session.get(url, params=params)
        if resp.status == 200:
            return await resp.json()
        else:
            raise HTTPError(url, resp.status, await resp.text(), resp.headers, None)

    async def add_tags(self, tags: Sequence[str]):
        """Adds a list of tags to the Data Fabric.

        Returns:
            A list of tags with their IDs like:

            .. code-block:: json

                [
                    {
                        "id": "00000000-0000-0000-0000-000000000000",
                        "title": "string"
                    }
                ]

        """
        body = [{"title": tag} for tag in tags]
        url = f"{self._url}/tags"
        resp = await self.session.post(url, body)
        data = await resp.json()
        if resp.status != 200:
            raise HTTPError(url, resp.status, data, resp.headers, None)
        return data

    # USERS.

    async def get_shared_users(self, userId: AnyStr) -> List[Dict]:
        """Gets list of users with whom current user has shared storage account access.

        Args:
            userId: User ID whose resource list to check.
        """
        url = f"{self._url}/users/ResourceDistributionList?userId={userId}"
        resp = await self.session.get(url)
        data = await resp.json()
        if resp.status == 200:
            return data
        elif resp.status == 403:
            raise DataFabricError("You do not have permission to view resource list for user {userId}.")
        else:
            raise HTTPError(url, resp.status, await resp.text(), resp.headers, None)

    async def get_current_user(self) -> Mapping[str, str]:
        url = f"{self._url}/users/me"
        resp = await self.session.get(url)
        if resp.status == 200:
            return await resp.json()
        else:
            raise HTTPError(url, resp.status, await resp.text(), resp.headers, None)

    async def get_user(self, userId: AnyStr) -> Mapping:
        url = f"{self._url}/users/{userId}"
        resp = await self.session.get(url)
        data = await resp.json()
        if resp.status == 200:
            return data
        elif resp.status == 404:
            raise DataFabricError(f"User {userId} does not exist.")
        else:
            raise HTTPError(url, resp.status, await resp.text(), resp.headers, None)

    async def whoami(self) -> Mapping[str, str]:
        """User/application information (depending on token).

        Returns:
            A dictionary like:

            .. code-block:: json

               {
                   "id": "user/app Veracity ID",
                   "type": "entity type in (user, application)",
                   "role": "Data Fabric role",
                   "companyId": "ID of organization to which the user/app belongs"
               }
        """
        try:
            data = await self.get_current_user()
            data["type"] = "user"
            data["id"] = data["userId"]
            data.pop("userId")
        except HTTPError:
            # Probably an application, not a user.
            data = await self.get_current_application()
            data["type"] = "application"

        return data

    # CONTAINERS.

    async def get_container(self, containerId: AnyStr, **kwargs) -> ContainerClient:
        """Gets Veracity container client (using Azure Storage SDK.)"""
        sas = await self.get_sas(containerId, **kwargs)
        sasurl = sas["fullKey"]
        return ContainerClient.from_container_url(sasurl)
