""" Unit tests for Veracity identity services.
"""

from unittest import mock
import pytest
from veracity_platform import identity


@pytest.fixture(scope="module", autouse=True)
def mock_ConfidentialClientApplication():
    """ Mock out the msal ConfidentialClientApplication"""
    with mock.patch("veracity_platform.identity.msal.ConfidentialClientApplication", autospec=True) as mock_class:
        mock_object = mock_class.return_value
        yield mock_object


class TestClientSecretCredential(object):
    @pytest.fixture(scope="class")
    def credential(self):
        yield identity.ClientSecretCredential("Name", "Secret")

    def test_get_token(self, credential, mock_ConfidentialClientApplication):
        mock_token = {"token_type": "", "access_token": ""}
        with mock.patch.object(
            mock_ConfidentialClientApplication, "acquire_token_for_client", return_value=mock_token
        ) as mock_acquire:
            token = credential.get_token(["veracity"])
            mock_acquire.assert_called_with(
                ["https://dnvglb2cprod.onmicrosoft.com/dfc0f96d-1c85-4334-a600-703a89a32a4c/.default"]
            )
            assert token == mock_token

    def test_get_token_service(self, credential, mock_ConfidentialClientApplication):
        mock_token = {"token_type": "", "access_token": ""}
        with mock.patch.object(
            mock_ConfidentialClientApplication, "acquire_token_for_client", return_value=mock_token
        ) as mock_acquire:
            token = credential.get_token(["veracity_service"])
            mock_acquire.assert_called_with(
                ["https://dnvglb2cprod.onmicrosoft.com/dfc0f96d-1c85-4334-a600-703a89a32a4c/.default"]
            )
            assert token == mock_token

    def test_get_token_datafabric(self, credential, mock_ConfidentialClientApplication):
        mock_token = {"token_type": "", "access_token": ""}
        with mock.patch.object(
            mock_ConfidentialClientApplication, "acquire_token_for_client", return_value=mock_token
        ) as mock_acquire:
            token = credential.get_token(["veracity_datafabric"])
            mock_acquire.assert_called_with(
                ["https://dnvglb2cprod.onmicrosoft.com/dfba9693-546d-4300-bcd7-d8d525bdff38/.default"]
            )
            assert token == mock_token

    @pytest.mark.skip("This functionality does not work yet.")
    def test_get_token_multiscope(self, credential, mock_ConfidentialClientApplication):
        token = credential.get_token(["veracity_service", "veracity_datafabric"])
        assert token is not None
        print(token)
        assert "token_type" in token
        assert "access_token" in token
