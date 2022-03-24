""" Tests for interactive credentials. Requires a user present.
"""


import pytest
from veracity_platform import identity


@pytest.mark.requires_secrets
@pytest.mark.interactive
class TestInteractiveBrowserCredential(object):
    @pytest.fixture(scope="class")
    def credential(self, CLIENT_ID, CLIENT_SECRET):
        yield identity.InteractiveBrowserCredential(
            CLIENT_ID, client_secret=CLIENT_SECRET
        )

    def test_get_token(self, credential):
        token = credential.get_token(["veracity"])
        assert token is not None
        assert "error" not in token
        assert "token_type" in token
        assert "access_token" in token

    def test_get_token_service(self, credential):
        token = credential.get_token(["veracity_service"])
        assert token is not None
        assert "error" not in token
        assert "token_type" in token
        assert "access_token" in token

    def test_get_token_datafabric(self, credential):
        token = credential.get_token(["veracity_datafabric"])
        assert token is not None
        assert "error" not in token
        assert "token_type" in token
        assert "access_token" in token

    @pytest.mark.skip("This functionality does not work yet.")
    def test_get_token_multiscope(self, credential):
        token = credential.get_token(["veracity_service", "veracity_datafabric"])
        assert token is not None
        print(token)
        assert "token_type" in token
        assert "access_token" in token
