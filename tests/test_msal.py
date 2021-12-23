""" Testing MSAL for Veracity client credential applications.
"""

import msal
import pytest
import requests
from veracity_platform import identity


@pytest.mark.requires_secrets
class TestClientCredential(object):

    @pytest.fixture(scope='class')
    def app(self, CLIENT_ID, CLIENT_SECRET):
        yield msal.ConfidentialClientApplication(
            client_id=CLIENT_ID,
            client_credential=CLIENT_SECRET,
            authority="https://login.microsoftonline.com/dnvglb2cprod.onmicrosoft.com",
        )

    @pytest.fixture(scope='class')
    def token(self, app):
        yield app.acquire_token_for_client([f'{identity.VERACITY_SERVICE_SCOPE}/.default'])

    def test_acquire_token(self, token):
        print(token)
        assert token is not None
        assert 'error' not in token
        assert 'token_type' in token
        assert token['token_type'] == 'Bearer'
        assert 'access_token' in token

    def test_use_token(self, token, SUBSCRIPTION_KEY):
        url = 'https://api.veracity.com/veracity/services/v3/services'
        params = {'page': 0, 'pageSize': 1}
        headers = {
            'Authorization': f'Bearer {token["access_token"]}',
            'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY,
        }
        response = requests.get(url, headers=headers, params=params)
        print(response.status_code)
        print(response.text)
        assert response.status_code == 200
