""" Veracity SDK example: Give a client app access to a Data Fabric container.

Warning:
    This is an interactive script.  You must log in to Veracity when prompted.

Pre-requisites:
    - Register an application in the Veracity developer portal (developer.veracity.com).
    - Email Veracity Support to register your application with the Data Fabric
      service (provide them your application client ID).
"""

import asyncio
import os
import azure.identity
import azure.keyvault.secrets
from veracity_platform.identity import InteractiveBrowserCredential
from veracity_platform.data import DataFabricAPI
from veracity_platform.utils import fix_aiohttp

# For convenience I have put my app credentials in an Azure Key Vault, but we
# fall back on environment variables if vault not available.
try:
    kvurl = os.environ['TEST_KEYVAULT_URL']
    kvcred = azure.identity.DefaultAzureCredential()
    kvclient = azure.keyvault.secrets.SecretClient(kvurl, kvcred)
    CLIENT_ID = kvclient.get_secret('TestApp-ID').value
    CLIENT_SECRET = kvclient.get_secret('TestApp-Secret').value
    SUBSCRIPTION = kvclient.get_secret('TestApp-Sub').value
    CONTAINER_ID = kvclient.get_secret('Test-Container-ID').value
except KeyError:
    CLIENT_ID = os.environ['TESTAPP_CLIENT_ID']
    CLIENT_SECRET = os.environ['TESTAPP_CLIENT_SECRET']
    SUBSCRIPTION = os.environ['TESTAPP_SUBSCRIPTION_KEY']
    CONTAINER_ID = os.environ['TEST_CONTAINER_ID']


async def main():
    # Get your credential to access the Data Fabric as a user.  This opens a
    # browser window.
    cred = InteractiveBrowserCredential(CLIENT_ID, client_secret=CLIENT_SECRET)
    token = cred.get_token(scopes=['veracity'])

    async with DataFabricAPI(cred, SUBSCRIPTION) as api:
        # Get all key templates for your container then filter to only one.
        allkeys = await api.get_keytemplates()
        subkeys = filter(lambda x: x['totalHours'] == 1 and x['name'] == 'Read, write and list key', allkeys)
        key = next(subkeys)

        # Share access with the client application.
        accessid = await api.share_access(
            CONTAINER_ID,
            CLIENT_ID,
            accessKeyTemplateId=key['id'],
            autoRefreshed=True,
        )

    print(f'Shared access to container {CONTAINER_ID} with access ID {accessid}.')


if __name__ == '__main__':
    fix_aiohttp()  # Known issue in aiohttp 3.X.
    asyncio.run(main())
