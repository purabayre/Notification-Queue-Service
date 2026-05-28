# BullMQ Notification Queue Service

A learning demo backend service that demonstrates queue mechanics using **BullMQ**, **Redis**, and **MongoDB**.

## Features

- **HTTP API** for enqueuing notifications
- **Separate worker process** for job processing
- **Automatic retries** with exponential backoff (3 attempts by default)
- **Delayed job support** for scheduling notifications
- **Persistent attempt history** in MongoDB
- **Graceful shutdown** that drains in-flight jobs
- **Status tracking** with full attempt history

## Stack

- **Node.js** 18+
- **TypeScript** (strict mode)
- **Express** (API server)
- **BullMQ** (job queue)
- **Redis** (queue backend)
- **MongoDB** (persistence)
- **Mongoose** (ODM)

## Prerequisites

You must have the following installed and running locally:

### 1. Node.js 18+

```bash
node --version
npm --version
```

### 2. Redis

**macOS (Homebrew):**

```bash
brew install redis
brew services start redis
redis-cli ping  # Should return PONG
```

**Linux (apt):**

```bash
sudo apt install redis-server
redis-server
redis-cli ping  # Should return PONG
```

**Windows:**
Download and install from [redis.io](https://redis.io/download) or use Windows Subsystem for Linux (WSL).

### 3. MongoDB

**macOS (Homebrew):**

```bash
brew install mongodb-community
brew services start mongodb-community
mongosh  # Should connect successfully
```

**Linux (apt):**

```bash
sudo apt install mongodb
mongosh  # Should connect successfully
```

**Windows:**
Download and install from [mongodb.com](https://www.mongodb.com/try/download/community)

## Installation

1. **Clone and navigate to the project:**

   ```bash
   cd bullMQ
   ```

2. **Install dependencies:**

   ```bash
   npm install
   ```

3. **Configure environment variables:**
   ```bash
   cp .env.example .env
   ```
   Edit `.env` if your Redis or MongoDB are on different hosts/ports.

## Running the Service

You need **two terminal windows** — one for the API and one for the worker.

### Terminal 1: Start the API Server

```bash
npm run dev:api
```

Expected output:

```
[INFO] ... ts-node-dev ver. 2.0.0 ...
MongoDB connected
server running on 3000, check health on http://localhost:3000/health
```

### Terminal 2: Start the Worker

```bash
npm run dev:worker
```

Expected output:

```
[INFO] ... ts-node-dev ver. 2.0.0 ...
MongoDB connected
It is highly recommended to use a minimum Redis version of 6.2.0
[Worker ready to process jobs]
```

## API Endpoints

### 1. Health Check

```bash
GET /health
```

Returns `"server is healthy"`

### 2. Enqueue a Notification

**POST** `/notifications`

**Request body:**

```json
{
  "to": "alice@example.com",
  "channel": "email",
  "body": "Welcome to BullMQ!",
  "delayMs": 0
}
```

**Parameters:**

- `to` (required): Recipient identifier (email, phone, user ID, etc.)
- `channel` (required): One of `email`, `sms`, `push`
- `body` (required): Message content
- `delayMs` (optional): Milliseconds to delay before processing (default: 0)

**Response:**

```json
{
  "message": "Notification queued",
  "jobId": "24"
}
```

### 3. Check Notification Status

**GET** `/notifications/:jobId`

Returns the full notification record with attempt history:

```json
{
  "_id": "...",
  "jobId": "24",
  "to": "alice@example.com",
  "channel": "email",
  "body": "Welcome to BullMQ!",
  "status": "sent",
  "attempts": [
    {
      "timestamp": "2025-05-25T15:40:30.000Z",
      "success": false,
      "errorMessage": "Fake provider failure"
    },
    {
      "timestamp": "2025-05-25T15:40:32.000Z",
      "success": true
    }
  ],
  "createdAt": "2025-05-25T15:40:25.000Z",
  "updatedAt": "2025-05-25T15:40:32.000Z"
}
```

**Status values:**

- `queued` — Job created, waiting to be processed
- `processing` — Worker is currently processing
- `sent` — Successfully sent
- `failed` — All retries exhausted

## Usage Example

### Step 1: Enqueue a Notification

```bash
curl -X POST http://localhost:3000/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "to": "alice@example.com",
    "channel": "email",
    "body": "Hello from BullMQ!",
    "delayMs": 0
  }'
```

Response:

```json
{
  "message": "Notification queued",
  "jobId": "42"
}
```

### Step 2: Check Status (while processing)

```bash
curl http://localhost:3000/notifications/42
```

Initial response (status is `processing`):

```json
{
  "status": "processing",
  "attempts": [],
  ...
}
```

### Step 3: Wait and Check Again

After ~2–4 seconds (including 2s fake delay + retries):

```bash
curl http://localhost:3000/notifications/42
```

Final response (success or failure):

```json
{
  "status": "sent",
  "attempts": [
    {
      "timestamp": "...",
      "success": true
    }
  ]
}
```

## How It Works

### Architecture

```
┌─────────────┐         ┌────────────┐         ┌─────────────┐
│  Client     │         │   Redis    │         │  MongoDB    │
│  (curl)     │         │ (queue)    │         │ (history)   │
└──────┬──────┘         └────────────┘         └─────────────┘
       │                       ▲
       │ POST /notifications   │
       └──────┬─────────────────┘
              │
       ┌──────▼──────┐
       │   API       │
       │  (Express)  │
       └──────┬──────┘
              │
              │ add job
              ▼
          ┌────┴────┐
          │  Redis  │
          │  Queue  │
          └────┬────┘
              │
              │ consumes
              ▼
         ┌─────────┐
         │ Worker  │
         │ Process │
         └────┬────┘
              │ updates
              ▼
          ┌─────────┐
          │ MongoDB │
          └─────────┘
```

### Job Lifecycle

1. **Enqueue** — Client calls `POST /notifications`, API creates a MongoDB record with status `queued` and adds a job to Redis.
2. **Processing** — Worker picks up the job, marks the record `processing`.
3. **Attempt** — Worker calls `fakeSender()`, which may succeed or fail (~40% failure rate).
4. **Retry** — On failure, the worker re-throws the error and BullMQ automatically schedules a retry (exponential backoff: 2s, 4s, 8s).
5. **Final Status** — After success, status becomes `sent`. After all 3 attempts fail, status becomes `failed`.
6. **History** — Each attempt is recorded with timestamp and success/failure.

### Retry Configuration

Located in [src/queue/notificationQueue.ts](src/queue/notificationQueue.ts):

```typescript

defaultJobOptions: {
  attempts: 3,           // Total number of attempts (1 initial + 2 retries)
  backoff: {
    type: "exponential",
    delay: 2000,         // Base delay: 2s, 4s, 8s
  },
  removeOnComplete: false,  // Keep job history
  removeOnFail: false,      // Keep failed job history
}
```

## Concepts Explained

### 1. Queue vs. Worker vs. Job

- **Queue** — A Redis-backed data structure that holds jobs. Decouples producers (API) from consumers (worker).
- **Worker** — A separate Node.js process that subscribes to the queue and executes jobs.
- **Job** — A unit of work containing data (to, channel, body) and metadata (attempts, delay, etc.).

**Why separate processes?** Allows scaling: multiple API instances can enqueue jobs while independent workers process them in parallel.

### 2. Why Redis?

BullMQ stores:

- **Job queue** — The list of pending jobs, waiting to be picked up.
- **Job data** — Input parameters and retry metadata.
- **Job status** — The current state (active, failed, delayed, etc.).
- **Locks** — Which worker is currently processing a job.

**What breaks if Redis goes down?**

- New jobs cannot be enqueued (POST /notifications fails).
- Jobs in flight lose their locks, and another worker picks them up (important for recovery).
- Jobs queued but not yet consumed are lost (unless persisted elsewhere — not done in this demo).
- Recovery: Restart Redis, then restart workers; jobs from last checkpoints will retry.

### 3. Retries & Backoff

- **Attempts** — Total tries allowed (3 by default).
- **Backoff** — Delay between retries. Exponential backoff prevents hammering a failing service:
  - Attempt 1 (immediate): fails
  - Attempt 2 (after 2s): fails
  - Attempt 3 (after 4s): succeeds → status `sent`

### 4. Delayed Jobs

Pass `delayMs` when enqueuing:

```json
{
  "to": "alice@example.com",
  "channel": "email",
  "body": "See you tomorrow!",
  "delayMs": 86400000
}
```

Worker will not process this job for 24 hours.

### 5. Idempotency

If a worker crashes mid-job, another worker picks it up. The check in [src/worker/worker.ts](src/worker/worker.ts):

```typescript
if (notification.status === "sent") {
  console.log(`Job ${job.id} already processed`);
  return;
}
```

Ensures that re-processing a completed job is safe (skipped).

### 6. Graceful Shutdown

Pressing `Ctrl-C` on the worker triggers:

```typescript
const shutdown = async () => {
  console.log("Shutting down worker...");
  await worker.close();
  console.log("Worker closed gracefully");
  process.exit(0);
};
process.on("SIGINT", shutdown);
```

Worker finishes in-flight jobs before exiting, preventing partial updates.

## Build & Production

### Build for Production

```bash
npm run build
```

Outputs compiled JavaScript to `dist/`.

### Run Compiled Code

```bash
# Terminal 1
npm run start:api

# Terminal 2
npm run start:worker
```

## Troubleshooting

### "Cannot find module" errors

```bash
npm install
```

### "Connection refused" for Redis

```bash
redis-cli ping
```

If it fails, start Redis:

```bash
redis-server  # or brew services start redis on macOS
```

### "Connection refused" for MongoDB

```bash
mongosh
```

If it fails, start MongoDB:

```bash
mongod  # or brew services start mongodb-community on macOS
```

### Worker not processing jobs

- Ensure both API and worker are running.
- Check MongoDB and Redis are reachable.
- Look at worker terminal for errors.
- Verify job data is being created in MongoDB: `db.notifications.find()` in mongosh.

### Jobs stuck in "processing"

This happens if a worker crashes while processing. Redis will detect the stale lock (after a timeout) and allow another worker to pick it up.

## File Structure

```
src/
├── api/
│   └── server.ts          # Express API (enqueue, status endpoints)
├── config/
│   ├── mongo.ts           # MongoDB connection
│   └── redis.ts           # Redis client
├── models/
│   └── Notification.ts    # MongoDB schema and model
├── queue/
│   └── notifcationQueue.ts # BullMQ queue configuration
├── services/
│   └── fakeSender.ts      # Simulated send logic (40% failure)
├── types/
│   └── notification.ts    # TypeScript types
└── worker/
    └── worker.ts          # Worker process (job consumer)
```

## Stretch Goals

- [ ] Add [Bull Board](https://github.com/felixmosh/bull-board) dashboard for live job monitoring
- [ ] Make processor idempotent: skip if record is already `sent`
- [ ] Add repeatable jobs: e.g., daily digest emails
- [ ] Add job flows (parent/child relationships)
- [ ] Write integration tests

## Daily Digest

- **What:** A repeatable job `send-digest` is scheduled at server startup to emit a daily digest of notifications.
- **Defaults:** `DIGEST_CRON` defaults to `0 0 * * *` (every day at midnight). `DIGEST_TO` defaults to `admin@admin.com`.
- **How it's scheduled:** The API calls a scheduler on startup which adds a repeatable job to the `notifications` queue.
- **Manual control:** You can create or remove repeatable jobs via the API endpoints:
  - `POST /notifications/repeat` — create a repeatable notification (accepts a `pattern` cron string).
  - `GET /admin/repeatable` — list repeatable jobs.
  - `DELETE /admin/repeatable/:jobKey` — remove a repeatable job by key.

## Notes for Learning

1. **Don't read tutorials first.** Read the official docs:
   - [BullMQ Docs](https://docs.bullmq.io/)
   - [ioredis Docs](https://github.com/luin/ioredis)
   - [Mongoose Docs](https://mongoosejs.com/)

2. **Key files to understand:**
   - [src/queue/notifcationQueue.ts](src/queue/notifcationQueue.ts) — Queue and retry config
   - [src/worker/worker.ts](src/worker/worker.ts) — Job processing and error handling
   - [src/api/server.ts](src/api/server.ts) — API endpoints and MongoDB integration

3. **Experiment:**
   - Try increasing the failure rate in [src/services/fakeSender.ts](src/services/fakeSender.ts) to see retries in action.
   - Try adding a delay in the enqueue call to test delayed jobs.
   - Kill the worker mid-job (Ctrl-C) and restart it to see recovery.

## License

ISC
