""" Client for whole Veracity API

This encompasses all sub-endpoint APIs and is designed to behave similarly to
the .NET libary.
"""

from .base import ApiBase
from .service import ClientAPI, UserAPI


class ApiClient:
    def __init__(self, credential, subscription_key):
        self.credential = credential
        self.subscription_key = subscription_key
        self._my = None
        self._this = None

    @property
    def my(self):
        if self._my is None:
            self._my = UserAPI(self.credential, self.subscription_key)
        return self._my

    @property
    def this(self):
        if self._this is None:
            self._this = ClientAPI(self.credential, self.subscription_key)
        return self._this
