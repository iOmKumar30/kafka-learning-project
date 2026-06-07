import json
from contextlib import asynccontextmanager
from tkinter import Y
from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaProducer
from shared.schemas import UserEvent

producer = None
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "user-events"

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    print("Kafka producer started")

    yield
    
    await producer.stop()
    print("Kafka producer stopped")

app = FastAPI(lifespan=lifespan)

@app.post("/events")
async def create_event(event: UserEvent):
    try:
        if producer is None:
            raise HTTPException(status_code=500, detail="Kafka producer not initialized")
        await producer.send_and_wait(TOPIC_NAME, value = event.model_dump(mode = "json"), key = str(event.user_id).encode("utf-8"))
        return {"status": "success", "message": "Evenet Queued"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))