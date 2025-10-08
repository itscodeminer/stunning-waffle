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






# Disable proxy if needed
$env:NO_PROXY = "169.254.169.343"

# Akeyless and Azure config
$akeyless_base_url = "http://169.254.169.254/metadata/identity/oauth2/token"
$config_url = "https://api.secmgmt-uat.cvshealth.com"
$access_id = "p-dfgdfgfdf"
$secret_path = "/cvs/sssharedservices/fgdf-fgdgdf/secrets/azure/sp-sssharedservices-ftslinq-dev"

# Step 1: Get Azure AD token from IMDS
$headers = @{ "Metadata" = "true" }
$params = @{
    "api-version" = "2018-02-01"
    "resource"    = "https://management.azure.com/"
}

$response = Invoke-RestMethod -Uri $akeyless_base_url -Headers $headers -Method GET -Body $null -UseBasicParsing -ContentType "application/x-www-form-urlencoded" -DisableKeepAlive -TimeoutSec 10 -ErrorAction Stop -Verbose:$false -MaximumRedirection 5 -AllowUnencryptedAuthentication:$true -SkipCertificateCheck:$true -Proxy:$null -ProxyUseDefaultCredentials:$false -ProxyCredential:$null -ProxyBypassList:$null -ProxyBypassOnLocal:$false -Force:$true -OutFile:$null -ResponseHeadersVariable "respHeaders" -BodyParameters $params

$cloud_id = $response.access_token
Write-Host "Access Token: $cloud_id"

# Step 2: Convert token to Base64
$base64_cloud_id = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($cloud_id))
Write-Host "Base64 Access Token: $base64_cloud_id"

# Step 3: Authenticate to Akeyless
$auth_body = @{
    "access_id"  = $access_id
    "access_type" = "azure_ad"
    "cloud_id"    = $base64_cloud_id
}

$auth_response = Invoke-RestMethod -Uri "$config_url/v2/auth" -Method POST -Body ($auth_body | ConvertTo-Json -Depth 3) -ContentType "application/json"
$token = $auth_response.token
Write-Host "Akeyless Session Token: $token"

# Step 4: Fetch the rotated secret
$secret_body = @{
    "name"  = $secret_path
    "token" = $token
}

$secret_response = Invoke-RestMethod -Uri "$config_url/v2/rotated-secret-get-value" -Method POST -Body ($secret_body | ConvertTo-Json -Depth 3) -ContentType "application/json"
$secret = $secret_response.value

Write-Host "Secret:"
$secret | Format-List

