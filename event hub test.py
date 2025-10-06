import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import ClientSecretCredential

# ==== Configuration ====
TENANT_ID = "<your-tenant-id>"
CLIENT_ID = "<your-app-client-id>"
CLIENT_SECRET = "<your-app-client-secret>"

FULLY_QUALIFIED_NAMESPACE = "your-namespace.servicebus.windows.net"
EVENTHUB_NAME = "your-eventhub-name"

async def test_connection():
    # Create Azure AD credential (Service Principal)
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    # Create Event Hub producer client (used just to test connection)
    client = EventHubProducerClient(
        fully_qualified_namespace=FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENTHUB_NAME,
        credential=credential
    )

    try:
        props = await client.get_eventhub_properties()
        print("✅ Connection successful!")
        print(f"Event Hub: {props['name']}")
        print(f"Partition IDs: {props['partition_ids']}")
    except Exception as e:
        print("❌ Connection failed!")
        print(f"Error: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(test_connection())