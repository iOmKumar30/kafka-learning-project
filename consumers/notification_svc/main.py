import asyncio
import json
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "user-events"

async def consume():
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="notification-group",
        auto_offset_reset="earliest", # if consumer starts mid-strea, start from the beginning
    )
    await consumer.start()
    print('Notification service started and waiting for events...')
    
    try:
        async for msg in consumer:
            event = msg.value
            print(f"[{msg.partition}:{msg.offset}] Received: {event['action']} for User {event['user_id']}")
            await asyncio.sleep(1)  # Simulate processing time
    finally:
        await consumer.stop()
        print('Notification service stopped.')
        
if __name__ == "__main__":
    asyncio.run(consume())