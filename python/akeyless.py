import requests
import base64


# Step 1: Get the token from IMDS
url = "http://169.254.169.254/metadata/identity/oauth2/token"
headers = { "Metadata": "true" }
params = {
    "api-version": "2018-02-01",
    "resource": "https://akeyless.io"
}

response = requests.get(url, headers=headers, params=params)
response.raise_for_status()

access_token = response.json()["access_token"]

# Step 2: Convert to Base64
token_bytes = access_token.encode("utf-8")
base64_token = base64.b64encode(token_bytes).decode("utf-8")

print("Base64 Access Token:", base64_token)
