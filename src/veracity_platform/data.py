""" Veracity Data Fabric API
"""


from typing import Any, AnyStr, List, Mapping, Sequence, Dict
from urllib.error import HTTPError
from xmlrpc.client import Boolean
import pandas as pd
from azure.storage.blob.aio import ContainerClient
from .base import ApiBase
from . import identity
from .errors import VeracityError, PermissionError


# Custom exceptions.
class DataFabricError(VeracityError):
    ...


class ContainerNotFoundError(DataFabricError):
    ...


class UserNotOwnerError(DataFabricError):
    ...


class DataFabricAPI(ApiBase):
    """Access to the data fabric endpoints (/datafabric) in the Veracity API.


    All web calls are async using aiohttp.  Returns web responses exactly as
    received, usually JSON.

    Arguments:
        credential: Oauth access token or the token provider (identity.Credential).
        subscription_key (str): Your application's API subscription key.  Gets
            sent in th Ocp-Apim-Subscription-Key header.
        version (str): Not currently used.
    """

    API_ROOT = "https://api.veracity.com/veracity/datafabric"

    def __init__(
        self,
        credential: identity.Credential,
        subscription_key: AnyStr,
        version: AnyStr = None,
        **kwargs,
    ):
        super().__init__(
            credential,
            subscription_key,
            scope=kwargs.pop("scope", "veracity_datafabric"),
            **kwargs,
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
        resp = await self.session.post(url, json=body)
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
        self,
        title: str,
        description: str,
        containerIds: Sequence[str],
        sortingOrder: float = 0.0,
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
        resp = await self.session.post(url, json=body)
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

    async def update_group(
        self, groupId: str, title: str, description: str, containerIds: List[str], sortingOrder: float = 0.0
    ):
        url = f"{self._url}/groups/{groupId}"
        body = {
            "title": title,
            "description": description,
            "resourceIds": list(containerIds),
            "sortingOrder": sortingOrder,
        }
        resp = await self.session.put(url, body)
        data = await resp.json()
        if resp.status == 200:
            return
        elif resp.status == 404:
            raise DataFabricError(f"Group {groupId} does not exist for current user.")
        else:
            raise HTTPError(url, resp.status, data, resp.headers, None)

    async def delete_group(self, groupId):
        url = f"{self._url}/groups/{groupId}"
        resp = await self.session.delete(url)
        data = await resp.json()
        if resp.status == 204:
            return
        elif resp.status == 404:
            raise DataFabricError(f"Group {groupId} does not exist for current user.")
        else:
            raise HTTPError(url, resp.status, data, resp.headers, None)

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
            Pandas dataframe with the key templates, sorted by ascending access
            level (higher level means more privileged access).

        Exceptions:
            Raises HTTPError if not a 200 response.
        """
        data = await self.get_keytemplates()
        df = pd.DataFrame(data)
        df["level"] = self._access_levels(df)
        return df.sort_values("level", inplace=False)

    async def get_keytemplate(
        self,
        read: bool = False,
        write: bool = False,
        list_: bool = False,
        delete: bool = False,
        duration: int = 1,
        exact_privileges: bool = False,
    ):
        """Gets a key template matching the required privileges.

        If you provide a duration, this method will locate the access template with
        the closest available duration <= to the desired duration.  By default it
        uses a duration of 1 hour (shortest available in Veracity).

        Args:
            read: Has user read access?
            write: Has user write access?
            list_: Has user ability to list container files?
            delete: Has user ability to delete files?
            duration: Desired maximum key expiry duration in hours.
            exact_privileges: Set True to match privileges exactly.  If False (default)
                then returns any rows with at least the required privileges, e.g.
                will return records where the key write privilege is True, even if
                argument write=False.

        Returns:
            A dictionary with the key template properties.
        """
        assert any(
            (read, write, delete, list_)
        ), "You must request at least one access privilege from (read, write, list, delete)."

        allkeys = await self.get_keytemplates_df()

        # Find key templates with the desired access privileges.
        privileged = self._filter_key_attributes(allkeys, read, write, list_, delete, exact_match=exact_privileges)

        # If we cannot match the desired privileges, fail.
        if len(privileged) == 0:
            names = ["read", "write", "delete", "list"]
            flags = [read, write, delete, list_]
            requested = [name for name, flag in zip(names, flags) if flag]
            raise RuntimeError(f"Cannot find key template with {'+'.join(requested)} access privileges.")

        # Get the key templates with up to the desired duration.
        keys = privileged[(privileged["totalHours"] - duration) <= 0]
        if len(keys) == 0:
            # If there are no keys with the desired duration, take the key
            # with the lower privilege and duration.
            key = privileged.sort_values(["level", "totalHours"], ascending=True).iloc[0]
        else:
            # Return the lower privilege key with the longest duration below that
            # requested.
            key = keys.sort_values(["level", "totalHours"], ascending=[True, False]).iloc[0]

        return key.to_dict()

    @staticmethod
    def _filter_key_attributes(
        keys: pd.DataFrame, read: bool, write: bool, list_: bool, delete: bool, exact_match: bool = False
    ):
        """Filters a data frame to match privilege levels.

        Veracity key/access privilege attributes are mapped in columns with names:

            | ---------- | ----------------- |
            | Column     | Purpose           |
            | ---------- | ----------------- |
            | attribute1 | Read privileges   |
            | attribute2 | Write privileges  |
            | attribute3 | Delete privileges |
            | attribute4 | List privileges   |
            | ---------- | ----------------- |

        Args:
            keys: DataFrame with key/access records.  Must have attribute columns 1-4.
            read: Has key read access?
            write: Has key write access?
            list_: Has key ability to list container files?
            delete: Has key ability to delete files?
            exact_match: Set True to match privileges exactly.  If False (default)
                then returns any rows with at least the required privileges, e.g.
                will return records where the key write privilege is True, even if
                argument write=False.

        Returns:
            Pandas dataframe with rows filtered to those matched the requested
            privileges.
        """
        if exact_match:
            mask = (
                (keys["attribute1"] == read)
                & (keys["attribute2"] == write)
                & (keys["attribute3"] == delete)
                & (keys["attribute4"] == list_)
            )
        else:
            # required = TT, optional = TF|FF, not-allowed = FT
            mask = (
                ~(~keys["attribute1"] & read)
                & ~(~keys["attribute2"] & write)
                & ~(~keys["attribute3"] & delete)
                & ~(~keys["attribute4"] & list_)
            )
        return keys.loc[mask]

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
        """Gets the access levels as a dataframe, including the "level" value.

        The data is sorted by ascending "level", where higher levels mean more access.

        Ensures the data frame has the correct columns, even if no accesses exist.
        """
        import pandas as pd

        data = await self.get_accesses(resourceId, pageNo, pageSize)
        results = data["results"]

        # Expand non-null IP ranges.
        for result in results:
            if result.get("ipRange") is not None:
                result["startIp"] = result["ipRange"]["startIp"]
                result["endIp"] = result["ipRange"]["startIp"]

        # Convert to data frame, ensuring correct columns.
        df = pd.DataFrame(results)
        ACCESS_COLUMNS = [
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
        ]
        df = df.reindex(columns=ACCESS_COLUMNS)

        # Add the level values for future use.
        df["level"] = self._access_levels(df)
        self.access_cache[resourceId] = df
        return df.sort_values("level", inplace=False)

    async def check_share_exists(
        self,
        containerId: AnyStr,
        userId: AnyStr,
        read: bool,
        write: bool,
        list_: bool,
        delete: bool,
        exact_privileges: bool = False,
    ):
        """Checks if the current user/app has shared access with another user.

        This method does not check key duration.

        Args:
            containerId: Container ID to which to share access.
            userId: User Veracity ID (GUID)
            read: Check user read access?
            write: Check user write access?
            list_: Check user ability to list container files?
            delete: Check user ability to delete files?
            exact_privileges: Set True to match privileges exactly.  If False (default)
                then returns any rows with at least the required privileges, e.g.
                will return records where the key write privilege is True, even if
                argument write=False.

        Returns:
            Access share ID is exists, otherwise None.
        """
        accesses = await self.get_accesses_df(containerId, pageSize=-1)

        if len(accesses) == 0:
            # Bail if there are no accesses (hence no shares).
            return

        me = await self.whoami()  # Logged in user/app.

        privileged = self._filter_key_attributes(accesses, read, write, list_, delete, exact_match=exact_privileges)
        if len(privileged) == 0:
            # Bail if there are no accesses with correct privileges (hence no shares).
            return

        mask = (privileged["userId"] == userId) & (privileged["grantedById"] == me["id"])
        existing_shares = privileged[mask]

        if len(existing_shares) > 0:
            print(existing_shares)
            return existing_shares["accessSharingId"].iloc[0]

    async def _share_access_by_permission(
        self,
        containerId: AnyStr,
        userId: AnyStr,
        read: bool = False,
        write: bool = False,
        list_: bool = False,
        delete: bool = False,
        duration: int = 1,
        autoRefreshed: bool = False,
        comment: AnyStr = None,
        startIp: AnyStr = None,
        endIp: AnyStr = None,
    ):
        """Attempts to share container access using the requested permissions.

        This method first looks for a suitable access share template, then
        applies that template for the user.  If no template is available, then
        raises an error.

        If the user already has a suitable access share, then this method does
        not create a new one; it returns the ID of the existing share.

        If you provide a duration, this method will locate the access template with
        the closest available duration <= to the desired duration.

        Args:
            containerId: Container ID to which to share access.
            userId: User Veracity ID (GUID)
            read: Give user read access?
            write: Give user write access?
            list_: Give user ability to list container files?
            delete: Give user ability to delete files?
            autoRefreshed: Should key automatically refresh if expired?
            duration: Desired maximum key expiry duration in hours.
            comment: Comment for the share, useful to remember why you shared!
            startIp: Start of valid user IP range, optional.
            endIp: End of valid user IP range, optional.

        Returns:
            GUID of access grant (sharing ID) if successful.

        Raises:
            RuntimeError if requested privileges cannot be met.
        """
        accessid = await self.check_share_exists(containerId, userId, read, write, list_, delete)
        if accessid:
            return accessid

        # There is no existing access, so get an appropriate template to create a new access.
        key = await self.get_keytemplate(read, write, list_, delete, duration)

        accessid = await self._share_access_with_template(
            containerId,
            userId,
            accessKeyTemplateId=key["id"],
            autoRefreshed=autoRefreshed,
            comment=comment,
            startIp=startIp,
            endIp=endIp,
        )
        return accessid

    async def _share_access_with_template(
        self,
        containerId: AnyStr,
        userId: AnyStr,
        accessKeyTemplateId: AnyStr,
        autoRefreshed: bool = False,
        comment: AnyStr = None,
        startIp: AnyStr = None,
        endIp: AnyStr = None,
    ):
        """Shares container access with a user/application.

        Args:
            containerId: Container ID to which to share access.
            autoRefreshed: Auto-renew keys when they expire?
            userId: ID of the user/application with which to share access.
            accessKeyTemplateId: Access level template (e.g read, write etc.)
                Get valid key templates using :meth:`get_keytemplates` method
            comment: Comment for the share, useful to remember why you shared!
            startIp: Start of valid user IP range, optional.
            endIp: End of valid user IP range, optional.

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

    async def share_access(
        self,
        containerId: AnyStr,
        userId: AnyStr,
        accessKeyTemplateId: AnyStr = None,
        autoRefreshed: bool = False,
        comment: AnyStr = None,
        startIp: AnyStr = None,
        endIp: AnyStr = None,
        read: bool = False,
        write: bool = False,
        list_: bool = False,
        delete: bool = False,
        duration: int = 1,
    ):
        """Shares container access with a user/application.

        Args:
            containerId: Container ID to which to share access.
            userId: ID of the user/application with which to share access.
            accessKeyTemplateId: Access level template (e.g read, write etc.)
                Get valid key templates using :meth:`get_keytemplates` method.
                If not provided, then you must request at least one privilege
                (read, write, list, delete) for the method to automatically
                find an access key template.
            autoRefreshed: Auto-renew keys when they expire?
            comment: Comment for the share, useful to remember why you shared!
            startIp: Start of valid user IP range, optional.
            endIp: End of valid user IP range, optional.
            read: Request user read access.
            write: Request user write access.
            list_: Request user ability to list container files.
            delete: Request user ability to delete files.
            autoRefreshed: Should key automatically refresh if expired?
            duration: Desired maximum key expiry duration in hours.

        Returns:
            The accessSharingId (str) if successful.

        Exceptions:
            Raises DataFabric error for known errors.
            Raises HTTPError for unknown errors.
        """
        if accessKeyTemplateId is not None:
            return await self._share_access_with_template(
                containerId,
                userId,
                accessKeyTemplateId,
                autoRefreshed=autoRefreshed,
                comment=comment,
                startIp=startIp,
                endIp=endIp,
            )
        else:
            assert any(
                (read, write, delete, list_)
            ), "Must provide access key template ID or at least one privilige (read, write, delete or list)."
            return await self._share_access_by_permission(
                containerId,
                userId,
                read=read,
                write=write,
                list_=list_,
                delete=delete,
                duration=duration,
                autoRefreshed=autoRefreshed,
                comment=comment,
                startIp=startIp,
                endIp=endIp,
            )

    async def revoke_access(self, containerId: AnyStr, accessId: AnyStr):
        url = f"{self._url}/resources/{containerId}/accesses/{accessId}"
        resp = await self.session.put(url)
        if resp.status == 200:
            return
        elif resp.status == 403:
            raise DataFabricError(f"HTTP/403 User is not the owner or data steward.")
        elif resp.status == 404:
            raise DataFabricError(f"HTTP/404 Data Fabric container {containerId} does not exist.")
        else:
            data = await resp.json()
            raise HTTPError(url, resp.status, data, resp.headers, None)

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
                {
                    "userId": "00000000-0000-0000-0000-000000000000",
                    "resourceId": "00000000-0000-0000-0000-000000000000",
                    "grantedBy": "00000000-0000-0000-0000-000000000000",
                    "comment": "string"
                }

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

    async def get_data_stewards_df(self, containerId: AnyStr) -> pd.DataFrame:
        data = await self.get_data_stewards(containerId)
        return pd.DataFrame(data, columns=["userId", "resourceId", "grantedBy", "comment"])

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
        resp = await self.session.post(url, json=body)
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

    async def transfer_ownership(self, containerId: AnyStr, userId: AnyStr, keepAccess: bool = False) -> Dict[str, Any]:
        """Transfers container ownership to another user.

        Requirements:
            - The current user must be the container owner
            - THe new user must have "Data Manager" role in Veracity.

        Args:
            containerId: GUID of the container.
            userId: GUID of the new container owner.
            keepAccess: Should current owner remain a data steward?

        Returns:
            Container metadata showing new owner.
        """
        url = f"{self._url}/resources/{containerId}/owner"
        resp = await self.session.put(
            url, params={"userId": userId, "keepAccessAsDataSteward": str(keepAccess).lower()}
        )
        data = await resp.json()
        if resp.status == 200:
            return data
        else:
            raise HTTPError(url, resp.status, data, resp.headers, None)

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
        resp = await self.session.post(url, json=body)
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


class ProvisionAPI(ApiBase):
    """Access to the data fabric provisioning API (/datafabric/provisioning) in Veracity.

    All web calls are async using aiohttp.  Returns web responses exactly as
    received, usually JSON.

    Attributes:
        credential: Oauth access token or the token provider (identity.Credential).
        subscription_key (str): Your application's API subscription key.  Gets
            sent in th Ocp-Apim-Subscription-Key header.
        version (str): Not currently used.
    """

    API_ROOT = "https://api.veracity.com/veracity/datafabric"

    def __init__(
        self,
        credential: identity.Credential,
        subscription_key: AnyStr,
        version: AnyStr = None,
        **kwargs,
    ):
        super().__init__(
            credential,
            subscription_key,
            scope=kwargs.pop("scope", "veracity_datafabric"),
            **kwargs,
        )
        self._url = f"{DataFabricAPI.API_ROOT}/provisioning/api/1"
        self.sas_cache = {}
        self.access_cache = {}

    @property
    def url(self) -> str:
        return self._url

    async def create_container(
        self,
        shortName: str,
        title: str,
        description: str = "",
        region: str = "westeurope",
        tags: List[str] = [],
        mayContainPersonalData: bool = False,
    ) -> str:
        """Creates a new blob container in the data fabric.

        Reference:
            https://api-portal.veracity.com/docs/services/5a72f224978c230c4c13aadb/operations/v1-0Container_ProvisionAzureBlobContainer?

        Args:
            shortName: Valid Azure container name (letters, numbers, no spaces and special characters)
            title: Short title displayed on the Data Fabric UI
            description: Long description of the container.
            region: Azure region.  Only use "westeurope"!
            tags: List of metadata tags.
            mayContainPersonalData: Flag if the container can contain personal data, e.g. for GDPR purposes.

        Returns:
            GUID of the created container.
        """
        url = f"{self._url}/container"
        body = {
            "storageLocation": region,
            "containerShortName": shortName,
            "mayContainPersonalData": mayContainPersonalData,
            "title": title,
            "description": description,
            "icon": {"id": "Automatic_Information_Display", "backgroundColor": "#5594aa"},
            "tags": [{"title": tag, "type": "tag"} for tag in tags],
        }
        resp = await self.session.post(url, json=body)
        data = await resp.text()
        if resp.status == 202:
            return data
        else:
            raise HTTPError(url, resp.status, data, resp.headers, None)

    async def copy_container(
        self,
        containerId,
        accessId,
        shortName,
        title,
        description: str = "",
        tags: List[str] = [],
        mayContainPersonalData: bool = False,
        groupId=None,
    ):
        """Copies a given Container with its content using an access sharing ID.

        Reference:
            https://api-portal.veracity.com/docs/services/5a72f224978c230c4c13aadb/operations/v1-0Container_CopyContainer?

        Args:
            sourceContainerId: The container to copy (GUID).
            accessId: The access/share used to copy the data (GUID).
            shortName: Valid Azure container name (letters, numbers, no spaces and special characters)
            title: Short title displayed on the Data Fabric UI
            description: Long description of the container.
            region: Azure region.  Only use "westeurope"!
            tags: List of metadata tags.
            mayContainPersonalData: Flag if the container can contain personal data, e.g. for GDPR purposes.
            groupId: Group in which to store the container (TODO: is this used?)

        Returns:
            GUID of the new (copy) container ID.
        """
        url = f"{self._url}/container/copycontainer"
        body = {
            "sourceResourceId": containerId,
            "copyResourceShortName": shortName,
            "copyResourceMayContainPersonalData": mayContainPersonalData,
            "copyResourceTitle": title,
            "copyResourceDescription": description,
            "copyResourceIcon": {"id": "Automatic_Information_Display", "backgroundColor": "#5594aa"},
            "copyResourceTags": [{"title": tag, "type": "tag"} for tag in tags],
        }
        if groupId:
            body["groupId"] = groupId
        resp = await self.session.post(url, json=body, params={"accessId": accessId})
        if resp.status == 202:
            return
        else:
            data = await resp.text()
            raise HTTPError(url, resp.status, data, resp.headers, None)

    async def delete_container(self, container_id: str) -> None:
        """Deletes a blob container given the ID.

        Reference:
            https://api-portal.veracity.com/docs/services/5a72f224978c230c4c13aadb/operations/v1-0Container_DeleteAzureBlobContainer?
        """
        url = f"{self._url}/container/{container_id}"
        resp = await self.session.delete(url)
        if resp.status == 202:
            return
        elif resp.status == 403:
            raise UserNotOwnerError("HTTP/403 User is not the container owner so cannot delete it.")
        elif resp.status == 404:
            raise ContainerNotFoundError("HTTP/404 The container does not exist.")
        else:
            data = await resp.text()
            raise HTTPError(url, resp.status, data, resp.headers, None)

    # EVENT SUBSCRIPTIONS.

    async def create_event_subscription(
        self, name: str, topic: str, callbackUrl: str, regions: List[str] = ["westeurope"]
    ) -> None:
        """Provision a callback for custom events.

        Call back url and subscription name Subscription name must be unique
        through the entire application.

        Available topics:
            - AccessShare

        Args:
            name: Subscription name, must be unique.
            topics: Topic to subscribe to.
            callbackUrl: URL for the callback which responds to events.
            regions: List of regions to observe.  By default just West Europe.

        Reference:
            https://api-portal.veracity.com/docs/services/5a72f224978c230c4c13aadb/operations/v1-0Container_SubscribeToCustomEvents?

        """
        url = f"{self._url}/container/SubscribeToCustomEvents"
        body = {
            "subscriptionName": name,
            "callback": callbackUrl,
            "topic": topic,
            "regions": regions,
        }
        resp = await self.session.post(url, json=body)
        if resp.status == 202:
            return
        else:
            data = await resp.text()
            raise HTTPError(url, resp.status, data, resp.headers, None)

    async def delete_event_subscription(self, name: str):
        """Delete a callback for custom events.

        Args:
            name: Subscription name to delete.

        Reference:
            https://api-portal.veracity.com/docs/services/5a72f224978c230c4c13aadb/operations/v1-0Container_UnsubscribeFromAzureBlobContainerEvents?
        """
        url = f"{self._url}/container/SubscribeToCustomEvents"
        body = {
            "subscriptionName": name,
        }
        resp = await self.session.delete(url, json=body)
        if resp.status == 202:
            return
        else:
            data = await resp.text()
            raise HTTPError(url, resp.status, data, resp.headers, None)

    async def create_blob_change_subscription(
        self, name: str, containerId: str, events: List[str], callbackUrl: str
    ) -> None:
        """Provision a callback for blob change events.

        Call back url and subscription name Subscription name must be unique
        through the entire application.

        Available events:
            - BlobUpserted

        Args:
            name: Subscription name, must be unique.
            containerId: The container on which to set the subscription (GUID).
            events: List of events to watch.
            callbackUrl: URL for the callback which responds to events.

        Reference:
            https://api-portal.veracity.com/docs/services/5a72f224978c230c4c13aadb/operations/v1-0Container_SubscribeToAzureBlobContainerEvents?
        """
        url = f"{self._url}/container/SubscribeToBlobContainerEvents"
        body = {
            "subscriptionName": name,
            "containerId": containerId,
            "subscriptionTypes": events,
            "callback": callbackUrl,
        }
        resp = await self.session.post(url, json=body)
        if resp.status == 202:
            return
        else:
            data = await resp.text()
            raise HTTPError(url, resp.status, data, resp.headers, None)

    async def delete_blob_change_subscription(self, name: str, containerId: str):
        """Delete a callback for blob change events.

        Args:
            name: Subscription name to delete.
            containerId: The container on which to set the subscription (GUID).

        Reference:
            https://api-portal.veracity.com/docs/services/5a72f224978c230c4c13aadb/operations/v1-0Container_UnsubscribeFromCustomEvents?
        """
        url = f"{self._url}/container/SubscribeToBlobContainerEvents"
        body = {
            "subscriptionName": name,
            "containerId": containerId,
        }
        resp = await self.session.delete(url, json=body)
        if resp.status == 202:
            return
        else:
            data = await resp.text()
            raise HTTPError(url, resp.status, data, resp.headers, None)

    # UTILITIES.

    async def list_regions(self):
        """Lists active Azure regions in which you can provision containers.

        Reference:
            https://api-portal.veracity.com/docs/services/5a72f224978c230c4c13aadb/operations/v1-0Regions_Get?

        Returns:
            List of active regions, each region is a dictionary of Azure region details.
        """
        url = f"{self._url}/regions"
        resp = await self.session.get(url)
        data = await resp.text()
        if resp.status == 200:
            return data
        else:
            raise HTTPError(url, resp.status, data, resp.headers, None)

    async def update_metadata(self):
        """Patch a container's metadata.

        Reference:
            https://api-portal.veracity.com/docs/services/5a72f224978c230c4c13aadb/operations/v1-0Container_UpdateMetadata?
        """
        raise NotImplementedError()
