# Retsu

A Redis-backed background job queue in Go.

The included processor simulates a payment gateway with a 20% failure rate, to exercise the retry logic.[^1]

---

## How it works (v2)

v1 used a List for pending and a Sorted Set for inflight, moved between with two separate Redis calls (`BRPOP` then `ZADD`). If the worker crashed between them, the job was lost — not pending, not inflight, not anywhere.[^2]

v2 replaces that pair with a single Redis Stream and a consumer group.

```
POST /queue
    ↓
retsu:stream         (Stream)
    ↓  XReadGroup — atomically delivered + tracked in the group's PEL
    ↓  success → XAck
    ↓  fail     →
retsu:retry          (Sorted Set, score = run-at timestamp)
    ↓  scheduler re-adds to the stream when due
retsu:stream         (again, up to MaxAttempts times)
    ↓  attempts exhausted →
retsu:dlq            (List)
```

**Worker pool** — N goroutines, each a named consumer (`worker-0`, `worker-1`, ...) in the `retsu:workers` group, blocking on `XReadGroup`. The read and the inflight bookkeeping happen in the same call, so there's no gap for a job to fall through.

**Scheduler** — ticks every second, scans `retsu:retry` for jobs whose run-at timestamp has passed, re-adds them to the stream.

**Watchdog** — ticks every second, finds entries idle past a timeout (`XPending`, filtered by idle time) — jobs some worker claimed but never acked — claims them and requeues them. This is how a crashed worker's jobs get reassigned instead of disappearing.

**Retry backoff** — exponential, unchanged from v1. Attempt 1 waits 20s, attempt 2 waits 40s, attempt 3 waits 80s. After `MaxAttempts` the job goes to the DLQ.

**Idempotency** — a duplicate `idempotency_key` gets the original `job_id` back instead of creating a second job. Backed by `SETNX`.

**Job status** — every transition (`pending` → `in_flight` → `succeeded`/`failed`/`dead`) is snapshotted and queryable by ID.

**Webhooks** — fire-and-forget POST of the final job state to a callback URL, once, no retry.

**Pluggable processors** — jobs aren't hardcoded to payments. Each job type registers its own handler; the worker looks it up by `type`.

**Graceful shutdown** — `SIGINT`/`SIGTERM` drains in-flight HTTP requests before exiting. Workers block forever on `XReadGroup` (`Block: 0`); plain context cancellation can't interrupt that read, so shutdown closes the Redis connection itself to unblock them.[^4]

**Configurable via env vars** — no more hardcoded address/port/pool size. See [Configuration](#configuration).

---

## Quickstart

You need Redis running locally.

```bash
redis-server
```

Start the queue server and worker pool:

```bash
go run main.go
```

Open the dashboard:

```
http://localhost:8080
```

Send 20 test jobs:

```bash
go run cmd/client/main.go
```

Watch the stats:

```bash
curl http://localhost:8080/stats
```

---

## Configuration

All optional, all fall back to the defaults below if unset.

| Env var | Default | Meaning |
|---|---|---|
| `REDIS_ADDR` | `localhost:6379` | Redis connection address |
| `HTTP_ADDR` | `:8080` | address the API + dashboard listen on |
| `WORKER_POOL_SIZE` | `20` | number of consumer goroutines |
| `SHUTDOWN_TIMEOUT_SECONDS` | `10` | how long to wait for in-flight HTTP requests to finish on shutdown |
| `INFLIGHT_TIMEOUT_SECONDS` | `300` | how long a job can sit unacked before the watchdog reclaims it |
| `RETSU_URL` | `http://localhost:8080` | (test client only) which server to fire test jobs at |

---

## API

### POST /queue

Enqueue a job. `type` selects the processor; `payload` is opaque to Retsu.

```bash
curl -X POST http://localhost:8080/queue \
  -H "Content-Type: application/json" \
  -d '{
    "type": "payment",
    "idempotency_key": "ord_abc",
    "callback_url": "http://localhost:9000/hook",
    "payload": {
      "user_id": "usr_123",
      "amount": 1099,
      "currency": "usd",
      "card_token": "tok_xyz"
    }
  }'
```

```json
{"job_id": "3f2e1d..."}
```

Returns `202 Accepted`. Sending the same `idempotency_key` again returns the same `job_id` with `200` instead of creating a duplicate job.

### GET /jobs/{id}

```bash
curl http://localhost:8080/jobs/3f2e1d...
```

```json
{
  "id": "3f2e1d...",
  "status": "succeeded",
  "attempts": 1,
  "max_attempts": 3,
  "last_error": ""
}
```

### GET /stats

```bash
curl http://localhost:8080/stats
```

```json
{
  "pending": 3,
  "inflight": 2,
  "retry": 1,
  "dlq": 0
}
```

`pending` is the stream's lag (work not yet delivered to the group); `inflight` is the group's pending entries count (work delivered but not acked).[^3]

---

## Code layout

```
main.go              wires everything together, starts the server + worker pool
job/job.go           Job struct, status constants, type-agnostic
queue/queue.go       all Redis operations — streams, idempotency, job status, retry, dlq, cleanup
worker/worker.go     worker loop, pool, scheduler goroutine, watchdog goroutine, webhook firing
server/server.go     HTTP handlers for enqueue, stats, job lookup, and the dashboard
server/static/       the dashboard — one HTML file, no framework, no build step
processor/skripe.go  processor registry + the fake payment gateway
cmd/client/main.go   test client — fires 20 jobs at the server
```

---

## Known issues

No tests. This is the biggest gap before this should be trusted with anything real.

No auth on any endpoint — anyone who can reach the port can enqueue jobs or read any job's status by ID.

The stream never trims — acked entries stay in it, so `XLEN` only grows. Not a correctness issue, just an eventual disk-space one. `XTRIM` is the fix and isn't in yet(need something like pg runner which cleans in chunks).

The DLQ has no inspection or replay endpoint. Jobs go in, nothing comes out.

No Docker/Compose setup — "you need Redis running locally" is friction for anyone but the person who wrote this(me).

---

## What's next

- Tests
- Auth
- Trim the stream
- DLQ inspection and requeue endpoints
- Docker Compose
- Webhook retries
- Priority queues

This is a work in progress — being picked back up later(i hope).

---

[^1]: it lies about 1 in 5 times, which is honestly a better failure rate than most real payment gateways.
[^2]: this is the main thing v2 fixes — gone from pending, never made it to inflight, invisible to everything.
[^3]: conflating the two is how v1's stats quietly lied to you.
