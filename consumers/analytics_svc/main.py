import asyncio
import json
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "user-events"

async def consume():
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id="analytics-group",  # unique group id separates this consumer from others
        auto_offset_reset="earliest"
    )
    
    await consumer.start()
    print("Analytics Service started. Updating dashboards...")
    
    try:
        async for msg in consumer:
            event = msg.value
            # Simulating database aggregation
            print(f"[Analytics] Counting {event['action']} for User {event['user_id']}")
            await asyncio.sleep(0.1) # Fast processing
    finally:
        await consumer.stop()
        print("Analytics Service stopped.")

if __name__ == "__main__":
    asyncio.run(consume())