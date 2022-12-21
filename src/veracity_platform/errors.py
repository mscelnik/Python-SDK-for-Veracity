""" Custom exceptions for the Veracity SDK.
"""

# Base error for HTTP exceptions.
from urllib.error import HTTPError


class VeracityError(RuntimeError):
    """ Base class for all Veracity SDK exceptions.
    """
    pass


class DirectoryError(VeracityError):
    """ Base class for errors in the directory API.
    """
    pass


class UserNotFoundError(DirectoryError):
    """ User does not exist in Veracity.
    """
    pass


class PermissionError(VeracityError):
    """ User does not have permission to perform action.
    """
    pass
