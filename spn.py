import msal
import requests

# Azure and Databricks details
databricks_instance = "https://<your-databricks-instance>.databricks.com"
client_id = "<your-client-id>"  # Application (client) ID
tenant_id = "<your-tenant-id>"  # Directory (tenant) ID
certificate_path = "path/to/your-cert.pem"  # Path to the public certificate
private_key_path = "path/to/your-private.key"  # Path to the private key

# Read the private key
with open(private_key_path, "r") as key_file:
    private_key = key_file.read()

# Initialize MSAL Confidential Client with certificate authentication
app = msal.ConfidentialClientApplication(
    client_id,
    authority=f"https://login.microsoftonline.com/{tenant_id}",
    client_credential={
        "private_key": private_key,
        "certificate": certificate_path,
    }
)

# Get an access token for Databricks
result = app.acquire_token_for_client(scopes=["https://<your-databricks-instance>.databricks.com/.default"])

if "access_token" in result:
    access_token = result["access_token"]

    # Use the token to call the Databricks API
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    workspace_url = f"{databricks_instance}/api/2.0/workspace/list"
    response = requests.get(workspace_url, headers=headers)
    print(response.json())
else:
    print("Failed to acquire token:", result.get("error"), result.get("error_description"))