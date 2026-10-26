# SentinelMesh — Distributed Task Processing MVP

Teaching project: Python + FastAPI + Redis Streams + Docker Compose.

## Run
Install Docker Desktop / Docker Engine with the Compose plugin, then:

```bash
docker compose up --build
```

Open dashboard http://localhost:8000 and API docs http://localhost:8000/docs.

Submit a job:
```bash
curl -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' -d '{"task_type":"demo","payload":{"duration_seconds":3}}'
```

## Failure demonstration
1. Submit 15–20 jobs in the dashboard.
2. In another terminal run `docker compose stop worker1`.
3. Observe the remaining workers continue processing.
4. Restart with `docker compose start worker1`.
5. To see pending-message recovery, stop a worker while it is processing a long job; after the pending message has been idle for 10 seconds, another worker can claim it.

## Architecture
Browser -> FastAPI -> Redis Streams (`jobs:stream`) -> consumer group -> worker processes.
Workers update job hashes, heartbeats, and an event stream. The dashboard polls API endpoints.

## Distributed systems concepts
- Concurrency: independent worker processes consume from a shared stream.
- Coordination: Redis Streams consumer group.
- Failure suspicion: heartbeat timestamps and timeout threshold.
- Recovery: `XAUTOCLAIM` can transfer sufficiently idle pending messages.
- Delivery semantics: at-least-once processing is possible; a reclaimed job can execute more than once.
- Idempotency: discuss how real-world side effects must be guarded against duplicate execution.

## Known limitations — disclose in the viva
This is a starter MVP, not production-grade. Job status updates are not atomic with stream acknowledgement; retry limits/dead-letter queue and strong idempotency are not implemented. Heartbeat expiry indicates suspicion, not certainty. Redis persistence and replication are not configured. Test failure recovery before claiming it works in every timing scenario.

## One-week plan
Day 1 run and understand architecture; Day 2 job lifecycle and validation; Day 3 failure tests; Day 4 benchmark one/two/three workers; Day 5 automated tests; Day 6 report and slides; Day 7 freeze and rehearse.

## Team split
A: API and job lifecycle. B: workers, coordination and fault tests. C: dashboard, benchmark and report. All members should understand the full architecture.

## Benchmark
Use identical jobs and durations for 1, 2, and 3 workers. Record total wall-clock time, completed count, and throughput = completed jobs / elapsed seconds. Repeat each run three times and report measured averages only.
