import akeyless
import requests
import base64
import os

os.environ['NO_PROXY'] = '169.254.169.343'

akeyless_base_url = "http://169.254.169.254/metadata/identity/oauth2/token"
config_url = "https://api.secmgmt-uat.cvshealth.com"
access_id = "p-dfgdfgfdf"
secret_path = '/cvs/sssharedservices/fgdf-fgdgdf/secrets/azure/sp-sssharedservices-ftslinq-dev'

headers = { "Metadata": "true" }
params = { "api-version": "2018-02-01", "resource": "https://management.azure.com/" }

# Get cloud_id
response = requests.get(akeyless_base_url, headers=headers, params=params)
response.raise_for_status()
cloud_id = response.json()["access_token"]
print("Access Token:", cloud_id)

# Convert cloud_id to Base64
token_bytes = cloud_id.encode("utf-8")
base64_cloud_id = base64.b64encode(token_bytes).decode("utf-8")
print("Base64 Access Token:", base64_cloud_id)

# Get token using access_id and base64_cloud_id
auth_body = akeyless.Auth(access_type="azure_ad", access_id=access_id, cloud_id=base64_cloud_id)
configuration = akeyless.Configuration(host=config_url)
api_client = akeyless.ApiClient(configuration)
api = akeyless.V2Api(api_client)
auth_response = api.auth(auth_body)
token = auth_response.token
print(token)
# t-154f163b11c98241916ff6ea94ca42ea

# Get rotated secret using secret path and token from previous step
body = akeyless.RotatedSecretGetValue(name=secret_path, token=token)
secret_data_response = api.rotated_secret_get_value(body)
secret = secret_data_response["value"]
print(secret)
#{'username': '7545e3aa-ee5a-4e8e-a417-8b3bf2261094', 'password': 'O.V8Q~dngU~Np454eX4u.QWbnFXkSWCEcL_k8bOP', 'application_id': '7495ef46-df7e-40f3-963e-95cc5011bf9e', 'expiration_date': '2025-07-29 07:00:00 UTC'}
