Phase 1: Kafka Setup & The Simple Producer-Consumer

In this phase, we will set up the broker, spin up a FastAPI application that ingests user actions, and write a standalone Python consumer to read them.

1. FastAPI starts
        ↓
2. Kafka producer created
        ↓
3. Kafka producer connects
        ↓
4. User calls POST /events
        ↓
5. FastAPI validates request
        ↓
6. event.model_dump()
        ↓
7. Producer sends message
        ↓
8. Kafka stores in topic user-events
        ↓
9. API returns success
        ↓
10. App shuts down
        ↓
11. Producer connection closed

To test this:

    Run the FastAPI app: uvicorn api_gateway.main:app --reload --port 8000

    Open a new terminal and run the consumer: python -m consumers.notification_svc.main

    Send a request:
    curl -X 'POST' \
  'http://localhost:8000/events' \
  -H 'Content-Type: application/json' \
  -d '{"user_id": "101", "action": "SIGNUP"}'