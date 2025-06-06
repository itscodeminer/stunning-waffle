import requests

url = "http://169.254.169.254/metadata/identity/oauth2/token"
headers = {
    "Metadata": "true"
}
params = {
    "api-version": "2018-02-01",
    "resource": "https://akeyless.io"
}

response = requests.get(url, headers=headers, params=params)
response.raise_for_status()

access_token = response.json()["access_token"]
print("Access Token:", access_token)
