import msal

# Client ID and subscription key for our custom app.
# See https://developer.veracity.com/projects/0a40f481-0879-48ec-bd32-a78a275dd604/rg/2d0cb9d7-c0cf-42d7-bd92-9037645c97f1/resources/da2ba7e4-b6a1-4a9e-a014-b7fe967e436e
CLIENT_ID = "da2ba7e4-b6a1-4a9e-a014-b7fe967e436e"
SUBSCRIPTION_KEY = "87377ef2f2174bd69fd5f7c0771d83c0"

# One scope required: the Veracity service API.
SCOPES = ["https://dnvglb2cprod.onmicrosoft.com/83054ebf-1d7b-43f5-82ad-b2bde84d7b75/user_impersonation"]

# Change the authority from Microsoft to Veracity B2C.
client = msal.PublicClientApplication(
    client_id=CLIENT_ID, authority="https://login.veracity.com/dnvglb2cprod.onmicrosoft.com/b2c_1a_signinwithadfsidp",
)

token = client.acquire_token_interactive(SCOPES)

assert "access_token" in token

print("Your Bearer token:")
print(token["access_token"])
