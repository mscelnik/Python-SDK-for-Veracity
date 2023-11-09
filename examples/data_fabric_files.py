""" Example demonstrating how to access files in the Veracity Data Fabric.

Requirements:
    - A Veracity app registration (for client ID and secret);
    - A Veracity data fabric container;
    - Access to the container.
"""

import asyncio
import os
from veracity_platform import identity, data, utils

CLIENT_ID = os.environ.get("EXAMPLE_VERACITY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("EXAMPLE_VERACITY_CLIENT_SECRET")
SUBSCRIPTION_KEY = os.environ.get("EXAMPLE_VERACITY_SUBSCRIPTION_KEY")
CONTAINER_ID = os.environ.get("EXAMPLE_VERACITY_CONTAINER_ID")

assert CLIENT_ID is not None
assert CLIENT_SECRET is not None
assert CONTAINER_ID is not None
assert SUBSCRIPTION_KEY is not None

cred = identity.InteractiveBrowserCredential(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
# token = cred.get_token(["veracity"])


async def main():
    """Demonstrate data fabric API usage. Note, the API calls are all async."""
    async with data.DataFabricAPI(credential=cred, subscription_key=SUBSCRIPTION_KEY) as api:
        me = await api.whoami()
        print(me)

        accesses = await api.get_best_access(CONTAINER_ID)
        print(accesses)

        container = await api.get_container(CONTAINER_ID)
        async for name in container.list_blob_names():
            print(name)
            break

        # # Get recent ledger entries and print them.
        # ledger = await api.get_ledger(CONTAINER_ID)
        # print(ledger)

        # # The ledger return is a Pandas dataframe, so we can do stats on it.
        # print(ledger.groupby("entityId")["fileName"].count())


if __name__ == "__main__":
    utils.fix_aiohttp()
    asyncio.run(main())
