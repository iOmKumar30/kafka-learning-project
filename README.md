# 🚀 Phase 1: Kafka Setup & The Simple Producer–Consumer

In this phase, we will:

* Set up a Kafka broker using Docker.
* Create a FastAPI application that ingests user events.
* Configure a Kafka producer inside the API.
* Build a standalone Python consumer that reads and processes events.

---

## 🏗️ Architecture Overview

```text
Client
   │
   ▼
FastAPI API Gateway
   │
   ▼
Kafka Producer
   │
   ▼
Kafka Topic (user-events)
   │
   ▼
Notification Consumer
```

---

## 🔄 Event Lifecycle

When a user sends an event to the API, the following sequence occurs:

```text
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
8. Kafka stores message in topic: user-events
        ↓
9. API returns success response
        ↓
10. Application shuts down
        ↓
11. Producer connection closed
```

---

## ▶️ Running the Demo

### Step 1: Start the FastAPI Application

```bash
uvicorn api_gateway.main:app --reload --port 8000
```

---

### Step 2: Start the Consumer

Open a new terminal and run:

```bash
python -m consumers.notification_svc.main
```

---

### Step 3: Publish an Event

Send a test request:

```bash
curl -X POST \
  'http://localhost:8000/events' \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"101","action":"SIGNUP"}'
```

---

### Expected Flow

```text
Client
   │
   ▼
POST /events
   │
   ▼
FastAPI validates payload
   │
   ▼
Kafka Producer publishes event
   │
   ▼
Kafka Topic: user-events
   │
   ▼
Notification Consumer receives event
```

---

# 🚀 Phase 2: Multi-Service Architecture

## The Architectural Concept: Fan-Out Pattern

So far, we have:

* One Producer
* One Consumer

While this demonstrates the basics, Kafka's true power comes from its **Publish/Subscribe (Pub/Sub)** architecture.

---

## 🍔 Real-World Example: Food Delivery Apps

Imagine placing an order on Swiggy or Zomato.

When you tap **"Place Order"**, the backend does **not** execute a massive synchronous workflow such as:

1. Charge the customer's card
2. Notify the restaurant
3. Find a delivery partner
4. Update order tracking
5. Send notifications

If any one of these steps fails, the entire request becomes slow and fragile.

---

### Instead...

The API publishes a single event:

```text
ORDER_PLACED
```

to Kafka.

Then multiple independent services subscribe to that event and perform their own responsibilities.

```text
                ORDER_PLACED
                       │
                       ▼
                 Kafka Topic
                       │
      ┌────────────────┼────────────────┐
      ▼                ▼                ▼
Payment Svc    Restaurant Svc   Delivery Svc
      │                │                │
      ▼                ▼                ▼
Charge Card    Notify Kitchen   Assign Rider
```

Each service works independently, making the system:

* Scalable
* Fault tolerant
* Easier to maintain
* Faster to evolve

---

## 👥 Consumer Groups

Kafka uses **Consumer Groups** to determine how messages are delivered.

### Case 1: Same `group_id`

If multiple consumers share the same group ID:

```text
Consumer A (group: notifications)
Consumer B (group: notifications)
```

Kafka distributes messages among them.

```text
Message 1 → Consumer A
Message 2 → Consumer B
Message 3 → Consumer A
Message 4 → Consumer B
```

This is used for **horizontal scaling**.

---

### Case 2: Different `group_id`

If consumers belong to different groups:

```text
Consumer A (group: notifications)
Consumer B (group: analytics)
Consumer C (group: audit)
```

Kafka delivers a complete copy of every message to each group.

```text
Message 1
   ├──► Notification Service
   ├──► Analytics Service
   └──► Audit Service
```

This pattern is called **Fan-Out**.

---

## 🎯 Goal of Phase 2

We will extend the architecture from:

```text
Producer → Kafka → Consumer
```

to:

```text
                        Kafka Topic
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
 Notification Svc     Analytics Svc       Audit Svc
```

Each service will independently consume the same event stream and perform its own business logic.
