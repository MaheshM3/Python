import asyncio
from azure.identity.aio import ClientSecretCredential
from azure.eventhub.aio import EventHubConsumerClient

# ==== Configuration ====
TENANT_ID = "<your-tenant-id>"
CLIENT_ID = "<your-app-client-id>"
CLIENT_SECRET = "<your-app-client-secret>"

FULLY_QUALIFIED_NAMESPACE = "your-namespace.servicebus.windows.net"
EVENTHUB_NAME = "your-eventhub-name"
CONSUMER_GROUP = "$Default"  # or your specific consumer group

# ==== Create Credential ====
credential = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)

# ==== Event Processing Callback ====
async def on_event(partition_context, event):
    print(f"📩 Received: {event.body_as_str()} from partition {partition_context.partition_id}")
    # Optional checkpointing
    await partition_context.update_checkpoint(event)

# ==== Main Event Loop ====
async def main():
    client = EventHubConsumerClient(
        fully_qualified_namespace=FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENTHUB_NAME,
        consumer_group=CONSUMER_GROUP,
        credential=credential
    )

    print("🚀 Listening for new events... (press Ctrl+C to stop)")

    async with client:
        await client.receive(
            on_event=on_event,
            starting_position="@latest"  # only new events
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")