""" Global pytest configuration and fixtures.


We read several test settings from environment variables so we don't have to
store client IDs and secrets in the code.
"""

import os
import pytest


@pytest.fixture(scope='session', autouse=True)
def configure_pandas():
    import pandas as pd
    pd.options.display.max_columns = 10
    pd.options.display.max_rows = 100


@pytest.fixture(scope='session')
def vault():
    import azure.identity
    import azure.keyvault.secrets
    url = os.environ.get('TEST_KEYVAULT_URL')
    if url:
        cred = azure.identity.DefaultAzureCredential()
        yield azure.keyvault.secrets.SecretClient(url, cred)


@pytest.fixture(scope='session')
def CLIENT_ID(vault):
    if vault:
        value = vault.get_secret('TestApp-ID').value
    else:
        value = os.environ.get("TEST_VERACITY_CLIENT_ID")
    yield value


@pytest.fixture(scope='session')
def CLIENT_SECRET(vault):
    if vault:
        value = vault.get_secret('TestApp-Secret').value
    else:
        value = os.environ.get("TEST_VERACITY_CLIENT_SECRET")
    yield value


@pytest.fixture(scope='session')
def SUBSCRIPTION_KEY(vault):
    if vault:
        value = vault.get_secret('TestApp-Sub').value
    else:
        value = os.environ.get("TEST_VERACITY_SUBSCRIPTION_KEY")
    yield value


@pytest.fixture(scope='session')
def RESOURCE_URL():
    yield os.environ.get("TEST_DATAFABRIC_RESOURCE_URL")


@pytest.fixture(scope='session')
def CONTAINER_ID(vault):
    if vault:
        value = vault.get_secret('Test-Container-ID').value
    else:
        value = os.environ.get("TEST_CONTAINER_ID")
    yield value


@pytest.fixture(scope='session')
def requires_secrets(request, CLIENT_ID, CLIENT_SECRET, SUBSCRIPTION_KEY):
    missing_secrets = (CLIENT_ID is None) or (CLIENT_SECRET is None) or (SUBSCRIPTION_KEY is None)
    if missing_secrets:
        pytest.skip('Test environment variable(s) not set.')


@pytest.fixture(scope='session')
def requires_datafabric(request, RESOURCE_URL, CONTAINER_ID):
    missing_vars = (RESOURCE_URL is None) or (CONTAINER_ID is None)
    if missing_vars:
        pytest.skip('Test environment variable(s) for data fabric not set.')
