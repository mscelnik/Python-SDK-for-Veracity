""" Veracity Services API

References:
    - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3
"""

from typing import AnyStr, Tuple, List, Dict, Any
from .base import ApiBase
import datetime

# TODO: Define a custom API exception.
from urllib.error import HTTPError


class UserAPI(ApiBase):
    """ Access to the current user endpoints (/my) in the Veracity REST-API.

    All web calls are async using aiohttp.  Returns web responses exactly as
    received, usually JSON.

    Arguments:
        credential: Oauth access token or the token provider (identity.Credential).
        subscription_key (str): Your application's API subscription key.  Gets
            sent in th Ocp-Apim-Subscription-Key header.
        version (str): Must be "v3" - other API versions not yet supported.
    """

    API_ROOT = "https://api.veracity.com/veracity/services"

    def __init__(self, credential, subscription_key, version="v3", **kwargs):
        super().__init__(
            credential, subscription_key, scope=kwargs.pop("scope", "veracity_service"), **kwargs,
        )
        self._url = f"{UserAPI.API_ROOT}/{version}/my"

    @property
    def url(self):
        return self._url

    async def get_companies(self):
        """ Gets all companies related to the current user.

        References:
            - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/My_GetMyCompanies?
        """
        endpoint = f"{self.url}/companies"
        resp = await self.session.get(endpoint)
        data = await resp.json(content_type=None)
        if resp.status != 200:
            raise HTTPError(endpoint, resp.status, data, resp.headers, None)
        return data

    async def get_messages(self, all=False):
        """ Reads the current user's messages.

        References:
            - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/My_GetMessagesAsync?
        """
        endpoint = f"{self.url}/messages"
        resp = await self.session.get(endpoint, params={"all": all})
        data = await resp.json()
        if resp.status != 200:
            raise HTTPError(endpoint, resp.status, data, resp.headers, None)
        return data

    async def get_message_count(self) -> int:
        """ Get unread message count for current user.

        References:
            - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/My_GetMessageCount?
        """
        endpoint = f"{self.url}/messages"
        resp = await self.session.get(endpoint)
        if resp.status != 200:
            data = await resp.json()
            raise HTTPError(endpoint, resp.status, data, resp.headers, None)
        return int(await resp.text())

    async def get_message(self, messageId):
        endpoint = f"{self.url}/messages/{messageId}"
        resp = await self.session.get(endpoint)
        data = await resp.json()
        if resp.status != 200:
            raise HTTPError(endpoint, resp.status, data, resp.headers, None)
        return data

    async def marks_messages_read(self):
        """ Marks all unread messages as read.

        References:
            - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14424acc4d910fcde88b6?
        """
        raise NotImplementedError()

    async def validate_policies(self, returnUrl=None):
        """ Validates all policies and returns a list of the policies that needs attention/

        References:
            - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/My_ValidatePolicies?
        """
        endpoint = f"{self.url}/policies/validate()"
        resp = await self.session.get(endpoint)
        if resp.status == 204:
            return True, []
        data = await resp.json()
        if resp.status == 406:
            return False, data["violatedPolicies"]
        else:
            raise HTTPError(endpoint, resp.status, data, resp.headers, None)

    async def validate_service_policy(
        self, serviceId: AnyStr, returnUrl=None, supportCode=None
    ) -> Tuple[bool, List[AnyStr]]:
        """ Validates the user policies for the specified service.

        References:
            - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5f61dfcff2522e11c4b17ecf?

        Args:
            serviceId: GUID identity of a Veracity service.
            returnUrl: (optional) The url to return the user to after policy issues have been resolved.
            supportCode: (optional) Provide a correlation token for log lookup.

        Returns:
            Tuple of (Is valid: bool, List of violated policies: list[str]).
        """
        endpoint = f"{self.url}/policies/{serviceId}/validate()"
        resp = await self.session.get(endpoint)
        if resp.status == 204:
            return True, []
        data = await resp.json()
        if resp.status == 406:
            return False, data["violatedPolicies"]
        else:
            raise HTTPError(endpoint, resp.status, data, resp.headers, None)

    async def get_profile(self):
        """ Retreives the profile of the current logged in user.

        References:
            - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/My_Info?
        """
        endpoint = f"{self.url}/profile"
        resp = await self.session.get(endpoint)
        data = await resp.json()
        if resp.status != 200:
            raise HTTPError(endpoint, resp.status, data, resp.headers, None)
        return data

    async def get_services(self) -> List[Dict[str, Any]]:
        """ Returns all services for the current user.

        References:
            - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/My_MyServices?
        """
        endpoint = f"{self.url}/services"
        resp = await self.session.get(endpoint)
        data = await resp.json()
        if resp.status != 200:
            raise HTTPError(endpoint, resp.status, data, resp.headers, None)
        return data

    async def get_widgets(self):
        """ Returns all widgets for the user.

        Mainly intended to be used by the Veracity Mobile app.

        References:
            - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5cd946d9acc4d913a429c0c0?
        """
        endpoint = f"{self.url}/widgets"
        resp = await self.session.get(endpoint)
        data = await resp.json()
        if resp.status != 200:
            raise HTTPError(endpoint, resp.status, data, resp.headers, None)
        return data

    async def get_picture(self) -> Dict[str, str]:
        """ Gets the profile picture of the current user

        References:
            - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/607160215d44630fbc66e6f5?
        """
        raise NotImplementedError()


class ClientAPI(ApiBase):
    """ Access to the app client endpoints (/this) in the Veracity REST-API.

    All web calls are async using aiohttp.  Returns web responses exactly as
    received, usually JSON.

    Arguments:
        credential: Oauth access token or the token provider (identity.Credential).
        subscription_key (str): Your application's API subscription key.  Gets
            sent in th Ocp-Apim-Subscription-Key header.
        version (str): Must be "v3" - other API versions not yet supported.
    """

    API_ROOT = "https://api.veracity.com/veracity/services"

    def __init__(self, credential, subscription_key, version="v3", **kwargs):
        super().__init__(
            credential, subscription_key, scope=kwargs.pop("scope", "veracity_service"), **kwargs,
        )
        self._url = f"{ClientAPI.API_ROOT}/{version}/this"

    @property
    def url(self):
        return self._url

    async def get_services(self, page, pageSize=10):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/This_GetServices?
        """
        raise NotImplementedError()

    async def post_notification(
        self, name, content, id_, timeStamp, recipients, serviceId, channelId=None, type_=0, highPriority=False,
    ):
        raise NotImplementedError()

    async def get_subscribers(self, page, pageSize=10, serviceId=None):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/This_GetUsers?
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/This_GetUsersForService?
        """
        raise NotImplementedError()

    async def get_subscriber(self, userId, serviceId=None):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/This_GetServiceUser?
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/This_GetUserForService?
        """
        raise NotImplementedError()

    async def add_subscriber(self, userId, role, serviceId=None):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/This_AddServiceUser?
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/This_AddUserAsync?
        """
        raise NotImplementedError()

    async def remove_subscriber(self, userId, serviceId=None):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/This_RemoveServiceUser?
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/This_RemoveUser?
        """
        raise NotImplementedError()

    async def create_user(
        self,
        firstName,
        lastName,
        email,
        role,
        contactEmail,
        contactName,
        returnUrl,
        sendMail=True,
        createSubscription=True,
    ):
        """https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/This_CreateUser?"""
        raise NotImplementedError()

    async def create_users(self, users):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/This_CreateUsers?
        """
        raise NotImplementedError()

    async def resolve_user(self, email):
        url = url = f"{self.url}/user/resolve({email})"
        resp = await self.session.get(url)
        if resp.status == 200:
            data = await resp.json()
            return data
        elif resp.status == 404:
            return None
        else:
            raise HTTPError(url, resp.status, await resp.text(), resp.headers, None)

    async def get_user_picture(self, serviceId, userId) -> Dict[str, str]:
        """ Gets a user profile picture.

        References:
            - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/607160215d44630fbc66e6f6?
        """
        raise NotImplementedError()

    async def notify_users(
        self,
        serviceId,
        name: str,
        content: str,
        id: str,
        timestamp: datetime.datetime,
        channelId: str,
        recipients: List[str],
        type: int = 0,
        highPriority: bool = False,
    ):
        """ Send notification to your users through the Veracity notification service.

        References:
            - https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/This_NotifyUsers?
        """
        raise NotImplementedError()

    async def verify_policy(self, serviceId, userId) -> bool:
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/615d675a5d44630cbc3e7e51?
        """
        raise NotImplementedError()


class DirectoryAPI(ApiBase):
    """ Access to the directory endpoints (/directory) in the Veracity REST-API.

    All web calls are async using aiohttp.  Returns web responses exactly as
    received, usually JSON.

    Arguments:
        credential: Oauth access token or the token provider (identity.Credential).
        subscription_key (str): Your application's API subscription key.  Gets
            sent in th Ocp-Apim-Subscription-Key header.
        version (str): Must be "v3" - other API versions not yet supported.
    """

    API_ROOT = "https://api.veracity.com/veracity/services"

    def __init__(self, credential, subscription_key, version="v3", **kwargs):
        super().__init__(
            credential, subscription_key, scope=kwargs.pop("scope", "veracity_service"), **kwargs,
        )
        self._url = f"{ClientAPI.API_ROOT}/{version}/directory"

    @property
    def url(self):
        return self._url

    # COMPANY DIRECTORY.

    async def get_company(self, companyId: str) -> Dict[str, Any]:
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/CompaniesDirectory_CompanyById?
        """
        raise NotImplementedError()

    async def get_company_users(self, companyId: str, page: int = 0, pageSize: int = 10) -> List[Dict[str, Any]]:
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/CompaniesDirectory_GetUsersByCompany?
        """
        raise NotImplementedError()

    # SERVICE DIRECTORY.

    async def get_service(self, serviceId: str) -> Dict[str, Any]:
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/ServicesDirectory_GetServiceById?
        """
        raise NotImplementedError()

    async def get_service_users(self, serviceId: str, page: int = 0, pageSize: int = 10) -> List[Dict[str, Any]]:
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/ServicesDirectory_GetUsers?
        """
        raise NotImplementedError()

    async def is_service_admin(self, serviceId: str, userId: str) -> Dict[str, Any]:
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/ServicesDirectory_IsAdmin?
        """
        raise NotImplementedError()

    async def get_service_status(self) -> Dict[str, Any]:
        """ Get the status of the service container.
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/Options_ServiceStatus?
        """
        raise NotImplementedError()

    async def get_data_containers(self, serviceId: str):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/DataContainerService_GetDataContainers?
        """
        raise NotImplementedError()

    async def create_data_container_reference(self, serviceId: str, containerId: str, name: str):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14424acc4d910fcde88b5?
        """
        raise NotImplementedError()

    async def delete_data_container_reference(self, serviceId: str, containerId: str):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5ca702fbacc4d90814cdd5af?
        """
        raise NotImplementedError()

    # USER DIRECTORY.

    async def accept_terms(self):
        """ Accept the service and platform terms on behalf of the logged in user.
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14425acc4d910fcde88c2?
        """
        raise NotImplementedError()

    async def activate_account(self, authToken: str):
        """ Activates a user by providing the activation token obtained in 'me/exchange/otp'
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14425acc4d910fcde88be?
        """
        raise NotImplementedError()

    async def delete_user(self, userId: str):
        """ Delete user account.
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14425acc4d910fcde88ba?
        """
        raise NotImplementedError()

    async def exchange_otp_code(self, otpAuthCode: str, emailAddress: str):
        """ Exchange the OTP code with an activation token
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14425acc4d910fcde88c0?
        """
        raise NotImplementedError()

    async def get_pending_activation(self):
        """ Get the data currently registered on the new user
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14425acc4d910fcde88bf?
        """
        raise NotImplementedError()

    async def get_user(self, userId: str):
        """ Returns the full profile for the user with the provided id
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14424acc4d910fcde88b9?
        """
        raise NotImplementedError()

    async def get_users(self, userId: List[str]):
        """ Returns the full profile for multiple users
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/UsersDirectory_GetUsersIn?
        """
        raise NotImplementedError()

    async def get_user_companies(self, userId: str):
        """ Returns a list of companies tied to a spescified user.
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/UsersDirectory_GetUserCompanies?
        """
        raise NotImplementedError()

    async def get_user_resync(self, userId: str):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/607160225d44630fbc66e6f7?
        """
        raise NotImplementedError()

    async def get_user_from_email(self, email):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14424acc4d910fcde88b8?
        """
        url = f"{self.url}/users/by/email"
        params = {"email": email}
        resp = await self.session.get(url, params=params)
        if resp.status == 200:
            data = await resp.json()
            return data
        elif resp.status == 404:
            return None
        else:
            raise HTTPError(url, resp.status, await resp.text(), resp.headers, None)

    async def get_user_services(self, userId: str, page: int = 0, pageSize: int = 10) -> List[Dict[str, Any]]:
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/UsersDirectory_GetUserServices?
        """
        raise NotImplementedError()

    async def get_user_subscription(self, userId: str, serviceId: str) -> Dict[str, Any]:
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/UsersDirectory_GetUserSubscriptionDetails?
        """
        raise NotImplementedError()

    async def update_current_user(self, firstName: str, lastName: str, displayName: str, countryCode: str):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/UsersDirectory_GetUserSubscriptionDetails?
        """
        raise NotImplementedError()

    async def change_current_user_email(self, email: str):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14425acc4d910fcde88bd?
        """
        raise NotImplementedError()

    async def change_current_user_phone(self, phone: str):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14425acc4d910fcde88bd?
        """
        raise NotImplementedError()

    async def change_current_user_password(self, password: str):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14425acc4d910fcde88bc?
        """
        raise NotImplementedError()

    async def validate_current_user_email(self, activationCode: str, address: str):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14425acc4d910fcde88c1?
        """
        raise NotImplementedError()

    async def validate_current_user_phone(self, activationCode: str, address: str):
        """
        https://api-portal.veracity.com/docs/services/veracity-myservices%20V3/operations/5db14425acc4d910fcde88c1?
        """
        raise NotImplementedError()


# ALIASES

# TODO: Make "AppAPI" the class name above.
AppAPI = ClientAPI

MeAPI = UserAPI
ThisAPI = ClientAPI
