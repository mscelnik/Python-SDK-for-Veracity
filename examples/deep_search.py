"""  Veracity SDK Example: Run Deep Search using a client credential.
"""
import os
import azure.identity
import azure.keyvault.secrets
import requests
from veracity_platform.identity import ClientSecretCredential


# For convenience I have put my app credentials in an Azure Key Vault.  Will
# fall back on environment variables though.
try:
    kvurl = os.environ['TEST_KEYVAULT_URL']
    kvcred = azure.identity.DefaultAzureCredential()
    kvclient = azure.keyvault.secrets.SecretClient(kvurl, kvcred)
    CLIENT_ID = kvclient.get_secret('TestApp-ID').value
    CLIENT_SECRET = kvclient.get_secret('TestApp-Secret').value
    SUBSCRIPTION = kvclient.get_secret('TestApp-Sub').value
except KeyError:
    CLIENT_ID = os.environ['TESTAPP_CLIENT_ID']
    CLIENT_SECRET = os.environ['TESTAPP_CLIENT_SECRET']
    SUBSCRIPTION = os.environ['TESTAPP_SUBSCRIPTION_KEY']


def main():
    cred = ClientSecretCredential(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )
    token = cred.get_token(scopes=['veracity_datafabric'])
    print(token)
    access_token = token['access_token']

    uri = "https://api.veracity.com/veracity/datafabric/search/api/v1/me/Services"
    response = requests.get(
        uri,
        headers={
            'Ocp-Apim-Subscription-Key': SUBSCRIPTION,
            'Authorization': f'Bearer {access_token}',
        },
    )

    print(response.status_code)
    print(response.text)


if __name__ == '__main__':
    main()
