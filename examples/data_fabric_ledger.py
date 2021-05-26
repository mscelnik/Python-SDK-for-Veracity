""" Example demonstrating client app using the data fabric ledger.

Requirements:
    - A service principal (client ID and secret) registered with as a data consumer *;
    - A Veracity data fabric container;
    - Assign the SP access to the data fabric container: Share access using the SP's
      email address like CLIENT_ID@service.veracity.com.

* You cannot use an app registered in the developer portal for this (yet).  You must
ask Veracity to create the service principal for you.  They will email you an ID
and secret.
"""

import asyncio
import os
from veracity_platform.identity import ClientSecretCredential
from veracity_platform.data import DataFabricAPI


CLIENT_ID = os.environ.get("EXAMPLE_VERACITY_SP")
CLIENT_SECRET = os.environ.get("EXAMPLE_VERACITY_SP_SECRET")
SUBSCRIPTION_KEY = os.environ.get("EXAMPLE_VERACITY_SP_SUBSCRIPTION")
CONTAINER_ID = os.environ.get("EXAMPLE_DATAFABRIC_CONTAINER")
RESOURCE_URL = os.environ.get("DATAFABRIC_RESOURCE_URL")


# Get a client/secret credential as this is a backend service accessing the data
# fabric.
cred = ClientSecretCredential(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, resource=RESOURCE_URL)


async def main():
    """ Demonstrate data fabric API usage. Note, the API calls are all async.
    """
    async with DataFabricAPI(credential=cred, subscription_key=SUBSCRIPTION_KEY) as api:
        # Get recent ledger entries and print them.
        ledger = await api.get_ledger(CONTAINER_ID)
        print(ledger)

        # The ledger return is a Pandas dataframe, so we can do stats on it.
        print(ledger.groupby('entityId')['fileName'].count())


if __name__ == '__main__':
    # Note, if we use asyncio.run() we get errors due to some known bug in aiohttp.
    # Getting a new event loop fixes this.
    asyncio.get_event_loop().run_until_complete(main())
    # asyncio.run(main())
