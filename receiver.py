import asyncio
from azure.eventhub.aio import EventHubConsumerClient

connection_str = "Endpoint=sb://<your-namespace>.servicebus.windows.net/;SharedAccessKeyName=ListenPolicy;SharedAccessKey=<key>;EntityPath=<eventhub-name>"
eventhub_name = "<eventhub-name>"

async def on_event(partition_context, event):
    print(f"📩 Received: {event.body_as_str()} from partition {partition_context.partition_id}")
    await partition_context.update_checkpoint(event)

async def main():
    client = EventHubConsumerClient.from_connection_string(
        connection_str,
        consumer_group="$Default",
        eventhub_name=eventhub_name
    )
    async with client:
        await client.receive(
            on_event=on_event,
            starting_position="-1"  # "-1" means from the beginning
        )

if __name__ == "__main__":
    asyncio.run(main())