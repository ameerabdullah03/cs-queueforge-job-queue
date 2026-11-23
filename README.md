# QueueForge Durable Job Queue

An executable study of persistent background jobs: enqueue JSON payloads, reserve work, retry failed jobs with backoff, and inspect completed or dead-letter jobs. SQLite provides durable local storage and `BEGIN IMMEDIATE` protects job claims from two local workers racing for the same row.

## Course connection

Parallel & Distributed Computing Systems, Operating Systems and Lab, Data Structures & Algorithms, and Database Management System.

## Run

```bash
python queue.py enqueue greet '{"name":"Ameer"}'
python queue.py work --once
python queue.py list
```

Use `python queue.py work` to keep a worker alive. Built-in handlers are `greet` and `sum`; invalid task types fail and are retried up to the configured limit. `queueforge.db` is created beside the program.

## Design notes

Jobs move through `queued`, `running`, `done`, and `dead`. Each claim increments attempts and sets a lease deadline. Retry delay grows exponentially. This is a single-machine educational queue, not a multi-host broker: it has no distributed clock, authentication, dashboard, or exactly-once guarantee. Handlers should be idempotent because a worker may fail after performing an action but before recording success.
