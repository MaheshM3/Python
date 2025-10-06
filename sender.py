from azure.eventhub import EventHubProducerClient, EventData

connection_str = "Endpoint=sb://<your-namespace>.servicebus.windows.net/;SharedAccessKeyName=SendPolicy;SharedAccessKey=<key>;EntityPath=<eventhub-name>"
eventhub_name = "<eventhub-name>"

producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)

with producer:
    event_data_batch = producer.create_batch()
    for i in range(5):
        event_data_batch.add(EventData(f"Test message {i}"))
    producer.send_batch(event_data_batch)
    print("✅ Sent 5 test messages")