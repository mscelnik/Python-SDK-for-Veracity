""" Global pytest configuration and fixtures.


We read several test settings from environment variables so we don't have to
store client IDs and secrets in the code.
"""

import os
import pytest
import azure.core.exceptions
import azure.identity
import azure.keyvault.secrets
import pandas as pd
from veracity_platform import utils


@pytest.fixture(scope="session", autouse=True)
def test_setup():
    pd.options.display.max_columns = 10
    pd.options.display.max_rows = 100
    utils.fix_aiohttp()


@pytest.fixture(scope="session")
def vault():
    url = os.environ.get("TEST_KEYVAULT_URL")
    try:
        cred = azure.identity.DefaultAzureCredential()
        yield azure.keyvault.secrets.SecretClient(url, cred)
    except (ValueError, azure.core.exceptions.ClientAuthenticationError):
        yield None


@pytest.fixture(scope="session")
def CLIENT_ID(vault):
    try:
        value = vault.get_secret("TestApp-ID").value
    except (ValueError, AttributeError, azure.core.exceptions.ClientAuthenticationError):
        value = os.environ.get("TEST_VERACITY_CLIENT_ID")
    yield value


@pytest.fixture(scope="session")
def CLIENT_SECRET(vault):
    try:
        value = vault.get_secret("TestApp-Secret").value
    except (ValueError, AttributeError, azure.core.exceptions.ClientAuthenticationError):
        value = os.environ.get("TEST_VERACITY_CLIENT_SECRET")
    yield value


@pytest.fixture(scope="session")
def SUBSCRIPTION_KEY(vault):
    try:
        value = vault.get_secret("TestApp-Sub").value
    except (ValueError, AttributeError, azure.core.exceptions.ClientAuthenticationError):
        value = os.environ.get("TEST_VERACITY_SUBSCRIPTION_KEY")
    yield value


@pytest.fixture(scope="session")
def RESOURCE_URL():
    yield os.environ.get("TEST_DATAFABRIC_RESOURCE_URL")


@pytest.fixture(scope="session")
def CONTAINER_ID(vault):
    try:
        value = vault.get_secret("Test-Container-ID").value
    except (ValueError, AttributeError, azure.core.exceptions.ClientAuthenticationError):
        value = os.environ.get("TEST_CONTAINER_ID")
    yield value


@pytest.fixture(scope="session")
def requires_secrets(request, CLIENT_ID, CLIENT_SECRET, SUBSCRIPTION_KEY):
    missing_secrets = (CLIENT_ID is None) or (CLIENT_SECRET is None) or (SUBSCRIPTION_KEY is None)
    if missing_secrets:
        pytest.skip("Test environment variable(s) not set.")


@pytest.fixture(scope="session")
def requires_datafabric(request, RESOURCE_URL, CONTAINER_ID):
    missing_vars = (RESOURCE_URL is None) or (CONTAINER_ID is None)
    if missing_vars:
        pytest.skip("Test environment variable(s) for data fabric not set.")


# ------------------------------------------------------------------------------
# Pytest configuration
# ------------------------------------------------------------------------------


def pytest_addoption(parser):
    parser.addoption(
        "--interactive", action="store_true", default=False, help="Run tests requiring user interaction",
    )


def modify_interactive(config, items):
    option = "interactive"

    if config.getoption(f"--{option}"):
        return [i for i in items if f"{option}" in i.keywords]

    skipped = pytest.mark.skip(reason=f"need --{option} option to run")
    for item in items:
        if option in item.keywords:
            item.add_marker(skipped)

    return []


def pytest_collection_modifyitems(config, items):
    interative_items = modify_interactive(config, items)

    # Break out if the user has selection a subset of tests, either regression
    # or integration.
    all_items = interative_items
    if len(all_items) > 0:
        # This replaces the full test set with the subset in place.
        items[:] = all_items
        return
