import os
import msal
from databricks.connect import DatabricksSession

# Azure AD and Databricks configuration
tenant_id = "YOUR_TENANT_ID"
client_id = "YOUR_CLIENT_ID"
client_secret = "YOUR_CLIENT_SECRET"
databricks_instance = "YOUR_DATABRICKS_INSTANCE"  # e.g., https://<databricks-instance>.cloud.databricks.com
cluster_id = "YOUR_CLUSTER_ID"  # Databricks cluster ID

# Set up MSAL with fixed OAuth2 parameters
authority = f"https://login.microsoftonline.com/{tenant_id}"
scope = ["https://databricks.azure.net/.default"]

# MSAL Confidential Client for OAuth2 token acquisition
app = msal.ConfidentialClientApplication(
    client_id=client_id,
    client_credential=client_secret,
    authority=authority
)

# Acquire an OAuth2 token
result = app.acquire_token_for_client(scopes=scope)

if "access_token" in result:
    # Set environment variables for Databricks Connect with OAuth2 token
    os.environ["DATABRICKS_HOST"] = databricks_instance
    os.environ["DATABRICKS_TOKEN"] = result["access_token"]
    os.environ["DATABRICKS_CLUSTER_ID"] = cluster_id

    # Initialize DatabricksSession
    session = DatabricksSession.builder.getOrCreate()

    print("DatabricksSession established.")
    
    # Example action: Print the session configuration
    print(session.conf.getAll())

    # Close session if no further use
    session.stop()
else:
    # Handle token acquisition failure
    print("Error acquiring token:", result.get("error"), result.get("error_description"))
