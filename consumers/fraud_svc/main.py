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
        group_id="fraud-group", 
        auto_offset_reset="earliest"
    )
    
    await consumer.start()
    print("Fraud Service started. Monitoring for suspicious activity...")
    
    try:
        async for msg in consumer:
            event = msg.value
            # Simulating heavy ML model inference
            print(f"[Fraud] Evaluating risk score for User {event['user_id']} action: {event['action']}")
            await asyncio.sleep(1.5) # Slow processing
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())