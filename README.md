# Retsu

A Redis-backed background job queue in Go.

Currently wired up for payment jobs — the processor simulates a payment gateway with a 50% failure rate to exercise the retry logic.

---

## How it works

Jobs move through four Redis keys:

```
POST /queue
    ↓
retsu:pending  (List)
    ↓  worker picks up
retsu:inflight (Sorted Set, score = timeout timestamp)
    ↓  success → removed
    ↓  fail    →
retsu:retry    (Sorted Set, score = run-at timestamp)
    ↓  scheduler moves back to pending when due
retsu:pending  (again, up to MaxAttempts times)
    ↓  attempts exhausted →
retsu:dlq      (List)
```

**Worker pool** — 5 goroutines, each blocking on `BRPOP`. When a job arrives one worker wakes up, moves the job to inflight, processes it, then acks or fails it.

**Scheduler** — ticks every second, scans `retsu:retry` for jobs whose run-at timestamp has passed, moves them back to pending.

**Watchdog** — ticks every second, scans `retsu:inflight` for jobs whose timeout has passed (stuck jobs), moves them back to pending.

**Retry backoff** — exponential. Attempt 1 waits 20s, attempt 2 waits 40s, attempt 3 waits 80s. After `MaxAttempts` the job goes to the DLQ.

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

Send 20 test jobs:

```bash
go run cmd/client/main.go
```

Watch the stats:

```bash
curl http://localhost:8080/stats
```

---

## API

### POST /queue

Enqueue a payment job.

```bash
curl -X POST http://localhost:8080/queue \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "usr_123",
    "amount": 1099,
    "currency": "usd",
    "idempotency_key": "ord_abc",
    "card_token": "tok_xyz",
    "callback_url": "http://localhost:9000/hook"
  }'
```

```json
{"job_id": "3f2e1d..."}
```

Returns `202 Accepted`. The job is queued, not processed yet.

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

---

## Code layout

```
main.go              wires everything together, starts the server + worker pool
job/job.go           Job struct and status constants
queue/queue.go       all Redis operations — push, pop, ack, fail, schedule, cleanup
worker/worker.go     worker loop, pool, scheduler goroutine, watchdog goroutine
server/server.go     HTTP handlers for enqueue and stats
processor/skripe.go  fake payment processor (simulates gateway failures)
cmd/client/main.go   test client — fires 20 jobs at the server
```

---

## Known issues

**The pop-to-inflight transition is not atomic.** The worker does `BRPOP` (remove from pending) and then `ZADD` (add to inflight) as two separate Redis calls. If the worker crashes between them, the job is lost — gone from pending, never made it to inflight, invisible to the watchdog.

This is the main thing v2 fixes. The options are a Lua script to make both operations atomic, `BLMOVE` into per-worker processing lists (how Sidekiq does it), or Redis Streams which handle this natively.

---

## What's next (v2)

- Atomic pop — fix the gap between pending and inflight
- Idempotency — deduplicate retried jobs on the `idempotency_key`
- Job status endpoint — `GET /jobs/{id}` to check what happened to a specific job
- DLQ inspection — endpoints to view and requeue dead jobs
- Pluggable processors — right now the worker is hardcoded to payment jobs
- also something even more fun couldbe just writing a broker in erlang if i feel like...
