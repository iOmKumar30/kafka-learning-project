# 🚀 Phase 1: Kafka Setup & The Simple Producer–Consumer

In this phase, we will:

- Set up a Kafka broker using Docker.
- Create a FastAPI application that ingests user events.
- Configure a Kafka producer inside the API.
- Build a standalone Python consumer that reads and processes events.

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

- One Producer
- One Consumer

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

- Scalable
- Fault tolerant
- Easier to maintain
- Faster to evolve

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

# 🚀 Phase 3: Consumer Groups, Partitions & Horizontal Scaling

## The Concept: Horizontal Scaling

So far, our Fraud Detection Service processes events one at a time.

Imagine the service performs a slow operation:

```python
await asyncio.sleep(1.5)
```

This may seem fine during development, but what happens if your application suddenly receives:

```text
100 signups per second
```

A single consumer instance would quickly become overwhelmed and fall behind.

---

## Vertical vs Horizontal Scaling

### ❌ Vertical Scaling

Upgrade the machine:

```text
More CPU
More RAM
Faster Hardware
```

While this works temporarily, it eventually becomes expensive and has limits.

---

### ✅ Horizontal Scaling

Instead of buying a bigger machine, simply run more instances of the same service:

```text
Fraud Service #1
Fraud Service #2
Fraud Service #3
Fraud Service #4
```

Now multiple workers can process messages simultaneously.

This is one of Kafka's greatest strengths.

---

# ⚠️ The Kafka Rule

To distribute work among multiple consumers in the **same Consumer Group**, the topic must have multiple partitions.

---

## Scenario 1: One Partition

```text
Topic: user-events

Partition 0
```

Consumers:

```text
Fraud #1
Fraud #2
Fraud #3
```

Result:

```text
Partition 0 → Fraud #1

Fraud #2 → Idle
Fraud #3 → Idle
```

Only one consumer can own a partition at a time.

---

## Scenario 2: Three Partitions

```text
Topic: user-events

Partition 0
Partition 1
Partition 2
```

Consumers:

```text
Fraud #1
Fraud #2
Fraud #3
```

Result:

```text
Partition 0 → Fraud #1
Partition 1 → Fraud #2
Partition 2 → Fraud #3
```

Perfect parallelism.

---

# 🛣️ Highway Analogy

Think of a Kafka Topic as a highway.

Think of Partitions as lanes.

### One-Lane Highway

```text
Cars
 ↓
═══════════════
 Lane 1
═══════════════
```

Adding more toll booths doesn't help.

Traffic is still forced through one lane.

---

### Three-Lane Highway

```text
═══════════════
 Lane 1
═══════════════

═══════════════
 Lane 2
═══════════════

═══════════════
 Lane 3
═══════════════
```

Now traffic can flow in parallel.

More consumers can actively participate.

---

# Step 1: Widen the Highway

## Recreate the Topic with 3 Partitions

In Phase 1, Kafka automatically created the topic:

```text
user-events
```

with:

```text
1 partition
```

We now need:

```text
3 partitions
```

---

### Open Kafka UI

Navigate to:

```text
http://localhost:8080
```

---

### Delete Existing Topic

1. Click **Topics**
2. Locate **user-events**
3. Click the three-dot menu
4. Select **Delete**

---

### Create a New Topic

1. Click **Add Topic**
2. Name it:

```text
user-events
```

3. Set:

```text
Partitions = 3
```

4. Click **Create Topic**

---

### Result

```text
user-events

├── Partition 0
├── Partition 1
└── Partition 2
```

---

# Step 2: No Code Changes Required

One of Kafka's biggest advantages is that scaling usually requires **zero changes to your business logic**.

Your Fraud Service can remain exactly the same.

Every instance uses:

```python
group_id="fraud-group"
```

Kafka automatically understands:

```text
These consumers belong to the same team.
```

and distributes partitions among them.

---

# Step 3: Launch Multiple Instances

Close the Notification and Analytics consumers for now to keep the output easy to follow.

Open **three separate terminals**.

In each terminal run:

```bash
python -m consumers.fraud_svc.main
```

You should see:

```text
🛡️ Fraud Service started
```

in all three windows.

---

## What Kafka Does Internally

Kafka detects:

```text
Consumer Group: fraud-group

Member 1
Member 2
Member 3
```

and performs a rebalance.

Example assignment:

```text
Partition 0 → Fraud #1
Partition 1 → Fraud #2
Partition 2 → Fraud #3
```

---

# Step 4: Load Testing

Send multiple events rapidly.

Example:

```bash
curl -X POST \
  'http://localhost:8000/events' \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"101","action":"LOGIN"}'
```

Repeat several times using different user IDs:

```json
{"user_id":"101","action":"LOGIN"}
{"user_id":"102","action":"LOGIN"}
{"user_id":"103","action":"LOGIN"}
{"user_id":"104","action":"LOGIN"}
{"user_id":"105","action":"LOGIN"}
```

---

# 🔑 Why Different User IDs Matter

Kafka uses the message key to determine which partition receives a message.

Example:

```python
key = user_id
```

Messages with the same key always go to the same partition.

This guarantees ordering.

---

### User 101

```text
User 101
    ↓
Partition 0
```

Every future event for User 101 will continue to go to Partition 0.

---

### User 102

```text
User 102
    ↓
Partition 2
```

Every future event for User 102 will continue to go to Partition 2.

---

## Ordering Guarantee

Kafka guarantees order **within a partition**.

Example:

```text
User 101

LOGIN
UPDATE_PROFILE
PURCHASE
LOGOUT
```

These events will always be consumed in the same order.

---

# Expected Output

Instead of one consumer doing all the work:

```text
Fraud #1
  ├── User 101
  ├── User 102
  ├── User 103
  └── User 104
```

Kafka distributes the load:

```text
Fraud #1 → User 101, User 104
Fraud #2 → User 102, User 105
Fraud #3 → User 103, User 106
```

Multiple events are processed simultaneously.

---

# 🎉 Success Criteria

If you see all three Fraud Service instances actively processing events, then you have successfully implemented:

- Consumer Groups
- Topic Partitions
- Kafka Rebalancing
- Horizontal Scaling
- Parallel Event Processing

At this point, you've moved from a simple producer-consumer setup to a scalable event-driven architecture capable of handling significantly higher throughput.

# 🚀 Phase 4: Retry Queues & Dead-Letter Queues (DLQ)

## The Concept: Handling Poison Pills

In a real-world event-driven system, not every message can be processed successfully.

Imagine your Notification Service receives:

* A malformed event
* Missing user information
* A temporary database outage
* An email provider outage (SendGrid, SES, etc.)

If the consumer crashes while processing the message, Kafka will deliver the same message again after restart.

This creates a dangerous situation known as **Head-of-Line Blocking**.

---

## ☠️ The Poison Pill Problem

```text
Topic: user-events

Message A ✅
Message B ❌ (Poison Pill)
Message C ✅
Message D ✅
```

If Message B always crashes the consumer:

```text
Consumer starts
      ↓
Reads Message B
      ↓
Crashes
      ↓
Restarts
      ↓
Reads Message B again
      ↓
Crashes
```

The consumer never reaches:

```text
Message C
Message D
```

Even though they are perfectly valid.

---

## Why Not Just Ignore the Error?

You could write:

```python
try:
    process_message()
except:
    pass
```

But now the message is silently discarded.

```text
❌ Data Lost
```

Not acceptable in production systems.

---

# 🏢 Enterprise Solution

Instead of crashing or dropping the message:

### Step 1

Catch the exception.

```text
Processing Failed
```

↓

### Step 2

Retry a few times.

```text
Attempt 1
Attempt 2
Attempt 3
```

↓

### Step 3

If it still fails, move it to a dedicated topic:

```text
user-events-dlq
```

↓

### Step 4

Commit the original offset and continue processing.

```text
Healthy messages keep flowing.
```

---

# Architecture

```text
                 user-events
                      │
                      ▼
           Notification Service
                      │
          ┌───────────┴───────────┐
          │                       │
          ▼                       ▼
      Success                 Failure
          │                       │
          ▼                       ▼
    Process Event          Retry 3 Times
                                  │
                                  ▼
                           user-events-dlq
```

---

# Step 1: Create the DLQ Topic

Open Kafka UI:

```text
http://localhost:8080
```

Create a new topic:

```text
user-events-dlq
```

Recommended settings:

```text
Topic Name     : user-events-dlq
Partitions     : 1
Replication    : 1
```

---

## Why Only 1 Partition?

DLQs typically receive much lower traffic than production topics.

```text
Normal Traffic
      ↓
user-events
```

```text
Failed Messages
      ↓
user-events-dlq
```

For this project, a single partition is sufficient.

---

# Step 2: Upgrade the Notification Service

Previously, the service only acted as a:

```text
Consumer
```

Now it must become both a:

```text
Consumer
      +
Producer
```

Why?

Because when processing fails, it needs to publish the failed event into:

```text
user-events-dlq
```

---

# Testing the Happy Path

Start the updated service:

```bash
python -m consumers.notification_svc.main
```

---

Send a healthy event:

```bash
curl -X POST \
  'http://localhost:8000/events' \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"101","action":"SIGNUP"}'
```

Expected output:

```text
📩 Processing Event
✅ Successfully sent notification
```

---

# Testing the Poison Pill

Send a deliberately broken event:

```bash
curl -X POST \
  'http://localhost:8000/events' \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"999","action":"CRASH"}'
```

---

## Expected Behavior

```text
Attempt 1 ❌
```

Wait 1 second

```text
Attempt 2 ❌
```

Wait 1 second

```text
Attempt 3 ❌
```

Still failing?

```text
➡️ Routing message to DLQ
```

---

### Example Flow

```text
user-events
      │
      ▼
Notification Service
      │
      ▼
CRASH Event
      │
      ▼
Retry #1
      │
      ▼
Retry #2
      │
      ▼
Retry #3
      │
      ▼
user-events-dlq
```

The consumer remains alive and continues processing new events.

---

# Inspecting the DLQ

Open Kafka UI and navigate to:

```text
Topics
  └── user-events-dlq
```

You should see the failed event stored safely.

Example:

```json
{
  "user_id": "999",
  "action": "CRASH",
  "error": "Simulated processing failure"
}
```

Nothing is lost.

The event can be:

* Investigated
* Reprocessed
* Fixed manually
* Replayed later

---

# The Most Important Change

## Manual Offset Management

Instead of:

```python
enable_auto_commit=True
```

we switch to:

```python
enable_auto_commit=False
```

---

## What Auto Commit Does

Kafka assumes:

```text
You fetched the message
        ↓
Therefore you processed it
```

which is not always true.

---

### Dangerous Scenario

```text
Consumer receives message
        ↓
Starts processing
        ↓
Server crashes
```

But Kafka already committed the offset.

```text
❌ Message Lost Forever
```

---

# Manual Commit Strategy

With:

```python
enable_auto_commit=False
```

Kafka waits for us to explicitly say:

```python
await consumer.commit()
```

Only after:

```text
Processing succeeded
OR
Message sent to DLQ
```

do we commit the offset.

---

# Delivery Guarantee

This gives us:

```text
At-Least-Once Delivery
```

Meaning:

> Every message will be processed at least once, even if the service crashes during processing.

---

# Why This Matters

By introducing:

* Retries
* Dead-Letter Queues
* Manual Offset Commits

we make the system:

✅ Fault tolerant
✅ Self-healing
✅ Resilient to transient failures
✅ Safe from poison pills
✅ Ready for production-style workloads

At this point, your Kafka architecture has evolved from a simple producer-consumer demo into a robust event-driven system capable of handling real-world failures gracefully.
