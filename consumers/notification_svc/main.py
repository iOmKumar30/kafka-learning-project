import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "user-events"
DLQ_TOPIC_NAME = "user-events-dlq"

MAX_RETRIES = 3

async def process_message(event):
    """Simulates business logic that might fail."""
    if event.get("action") == "CRASH":
        raise ValueError("Simulated API failure or Poison Pill data!")
    
    # Simulate normal processing time
    await asyncio.sleep(0.5) 
    print(f"[Notification] Successfully sent to User {event['user_id']}")

async def consume():
    # Setup Consumer
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id="notification-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False # We take manual control of commits now!
    )
    
    # Setup Producer (for the DLQ)
    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    await consumer.start()
    await dlq_producer.start()
    print("Notification Service started (with DLQ enabled)...")
    
    try:
        async for msg in consumer:
            event = msg.value
            success = False
            
            # 1. The Retry Loop
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    await process_message(event)
                    success = True
                    break # Break out of retry loop if successful
                except Exception as e:
                    print(f"[Notification] Error on attempt {attempt}: {e}")
                    await asyncio.sleep(1) # Backoff before retrying
            
            # 2. The Dead-Letter Queue (DLQ) Fallback
            if not success:
                print(f"[Notification] Max retries reached. Routing to DLQ...")
                # Add metadata to help debugging later
                dlq_event = {
                    "original_event": event,
                    "error": "Max retries exceeded",
                    "failed_service": "notification_svc"
                }
                await dlq_producer.send_and_wait(DLQ_TOPIC_NAME, dlq_event)
            
            # 3. Manually commit the offset ONLY AFTER we have either processed it or secured it in the DLQ
            await consumer.commit()
            
    finally:
        await consumer.stop()
        await dlq_producer.stop()

if __name__ == "__main__":
    asyncio.run(consume())